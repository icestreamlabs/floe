use super::*;

pub(super) fn join_for_row_number_rewrite<'a>(
    ctx: &'a PlannerContext<'a>,
    input_id: usize,
) -> Option<&'a DbspJoinNode> {
    let input_node = ctx.node_by_id(input_id)?;
    match &input_node.kind {
        DbspNodeKind::Join(join) => Some(join),
        DbspNodeKind::Select(_) => {
            let join_input_id = input_node.inputs.first().copied()?;
            let join_node = ctx.node_by_id(join_input_id)?;
            match &join_node.kind {
                DbspNodeKind::Join(join) => Some(join),
                _ => None,
            }
        }
        _ => None,
    }
}

#[derive(Debug, Default)]
pub(super) struct SplitJoinFilter {
    pub(super) left_pushdown: Option<Expr>,
    pub(super) right_pushdown: Option<Expr>,
    pub(super) remaining: Option<Expr>,
    pub(super) required_columns: BTreeSet<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum JoinInputSide {
    Left,
    Right,
}

impl JoinInputSide {
    pub(super) fn opposite(self) -> Self {
        match self {
            JoinInputSide::Left => JoinInputSide::Right,
            JoinInputSide::Right => JoinInputSide::Left,
        }
    }
}

pub(super) fn combine_optional_filters(left: Option<Expr>, right: Option<Expr>) -> Option<Expr> {
    let mut filters = Vec::new();
    if let Some(left) = left {
        filters.push(left);
    }
    if let Some(right) = right {
        filters.push(right);
    }
    combine_filters(filters)
}

pub(super) fn required_columns_for_expressions(
    expressions: &[Expr],
    input_schema: &RowSchema,
) -> Result<BTreeSet<usize>, PlannerError> {
    let mut columns = BTreeSet::new();
    for expr in expressions {
        let (expression, _) = extract_alias(expr.clone())?;
        add_required_expression_columns(&expression, input_schema, &mut columns)?;
    }
    Ok(columns)
}

pub(super) fn infer_join_relation_sides(
    projection_exprs: Option<&[Expr]>,
    top_filter: Option<&Expr>,
    join: &DbspJoinNode,
) -> HashMap<String, JoinInputSide> {
    let mut inferred = HashMap::new();
    if let Some(expressions) = projection_exprs {
        for expr in expressions {
            accumulate_join_relation_sides(expr, join, &mut inferred);
        }
    }
    if let Some(filter) = top_filter {
        accumulate_join_relation_sides(filter, join, &mut inferred);
    }
    inferred
}

pub(super) fn accumulate_join_relation_sides(
    expression: &Expr,
    join: &DbspJoinNode,
    inferred: &mut HashMap<String, JoinInputSide>,
) {
    for column in expression.column_refs() {
        let Some(relation) = column.relation.as_ref().map(ToString::to_string) else {
            continue;
        };
        let left_has = join.left_schema.field_index(column.name.as_str()).is_some();
        let right_has = join
            .right_schema
            .field_index(column.name.as_str())
            .is_some();
        let Some(side) = (match (left_has, right_has) {
            (true, false) => Some(JoinInputSide::Left),
            (false, true) => Some(JoinInputSide::Right),
            _ => None,
        }) else {
            continue;
        };
        inferred.entry(relation).or_insert(side);
    }
}

pub(super) fn rewrite_join_output_projection_expr(
    expression: Expr,
    join: &DbspJoinNode,
    relation_sides: &HashMap<String, JoinInputSide>,
) -> Result<Expr, PlannerError> {
    expression
        .transform_up(|expr| match expr {
            Expr::Column(column) => {
                let output_idx = resolve_join_output_column_index(&column, join, relation_sides)?;
                let field = join.output_schema.field(output_idx).ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "join output column index {output_idx} out of bounds",
                    ))
                })?;
                Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                    field.name.clone(),
                ))))
            }
            other => Ok(Transformed::no(other)),
        })
        .map(|result| result.data)
        .map_err(|err| PlannerError::AnalysisError(err.into()))
}

pub(super) fn resolve_join_output_column_index(
    column: &Column,
    join: &DbspJoinNode,
    relation_sides: &HashMap<String, JoinInputSide>,
) -> Result<usize, DataFusionError> {
    if let Some(relation) = column.relation.as_ref().map(ToString::to_string)
        && let Some(side) = relation_sides.get(&relation)
    {
        return match side {
            JoinInputSide::Left => {
                let input_idx = join
                    .left_schema
                    .field_index(column.name.as_str())
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "column '{}.{}' not found in left join input schema",
                            relation, column.name
                        ))
                    })?;
                join_output_index_for_input(join, JoinInputSide::Left, input_idx).ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "left join input column '{}.{}' is not present in {:?} output",
                        relation, column.name, join.join_type
                    ))
                })
            }
            JoinInputSide::Right => {
                let input_idx = join
                    .right_schema
                    .field_index(column.name.as_str())
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "column '{}.{}' not found in right join input schema",
                            relation, column.name
                        ))
                    })?;
                join_output_index_for_input(join, JoinInputSide::Right, input_idx).ok_or_else(
                    || {
                        DataFusionError::Plan(format!(
                            "right join input column '{}.{}' is not present in {:?} output",
                            relation, column.name, join.join_type
                        ))
                    },
                )
            }
        };
    }

    if let Some(output_idx) = join.output_schema.field_index(column.name.as_str()) {
        return Ok(output_idx);
    }

    match (
        join.left_schema.field_index(column.name.as_str()),
        join.right_schema.field_index(column.name.as_str()),
    ) {
        (Some(left_idx), None) => join_output_index_for_input(join, JoinInputSide::Left, left_idx)
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "left join input column '{}' is not present in {:?} output",
                    column.name, join.join_type
                ))
            }),
        (None, Some(right_idx)) => {
            join_output_index_for_input(join, JoinInputSide::Right, right_idx).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "right join input column '{}' is not present in {:?} output",
                    column.name, join.join_type
                ))
            })
        }
        _ => Err(DataFusionError::Plan(format!(
            "column '{}' could not be resolved in join output schema",
            column.flat_name()
        ))),
    }
}

fn join_output_index_for_input(
    join: &DbspJoinNode,
    side: JoinInputSide,
    input_idx: usize,
) -> Option<usize> {
    match (&join.join_type, side) {
        (
            DbspJoinType::Inner
            | DbspJoinType::LeftOuter
            | DbspJoinType::RightOuter
            | DbspJoinType::FullOuter,
            JoinInputSide::Left,
        )
        | (DbspJoinType::LeftSemi | DbspJoinType::LeftAnti, JoinInputSide::Left) => Some(input_idx),
        (
            DbspJoinType::Inner
            | DbspJoinType::LeftOuter
            | DbspJoinType::RightOuter
            | DbspJoinType::FullOuter,
            JoinInputSide::Right,
        ) => Some(join.left_schema.len() + input_idx),
        (DbspJoinType::RightSemi | DbspJoinType::RightAnti, JoinInputSide::Right) => {
            Some(input_idx)
        }
        (DbspJoinType::LeftSemi | DbspJoinType::LeftAnti, JoinInputSide::Right)
        | (DbspJoinType::RightSemi | DbspJoinType::RightAnti, JoinInputSide::Left) => None,
    }
}

pub(super) fn split_join_filter(
    predicate: Option<&Expr>,
    output_schema: &RowSchema,
    left_width: usize,
    join: &DbspJoinNode,
) -> Result<SplitJoinFilter, PlannerError> {
    let Some(predicate) = predicate else {
        return Ok(SplitJoinFilter::default());
    };
    let normalized = normalize_expr(predicate.clone())?;
    let conjuncts = split_conjuncts(&normalized);
    let mut left_pushdown = Vec::new();
    let mut right_pushdown = Vec::new();
    let mut remaining = Vec::new();
    let mut required_columns = BTreeSet::new();
    let left_to_right_keys = join_key_column_mapping(join, JoinInputSide::Left);
    let right_to_left_keys = join_key_column_mapping(join, JoinInputSide::Right);

    for conjunct in conjuncts {
        let columns = expression_output_columns(&conjunct, output_schema)?;
        required_columns.extend(columns.iter().copied());
        let references_left = columns.iter().any(|column_idx| *column_idx < left_width);
        let references_right = columns.iter().any(|column_idx| *column_idx >= left_width);
        match (references_left, references_right) {
            (true, false) => {
                let left_predicate =
                    rewrite_join_output_expr_for_side(conjunct, join, JoinInputSide::Left)?;
                for right_predicate in
                    rewrite_key_predicates_for_opposite_side(&left_predicate, &left_to_right_keys)?
                {
                    right_pushdown.push(right_predicate);
                }
                left_pushdown.push(left_predicate);
            }
            (false, true) => {
                let right_predicate =
                    rewrite_join_output_expr_for_side(conjunct, join, JoinInputSide::Right)?;
                for left_predicate in
                    rewrite_key_predicates_for_opposite_side(&right_predicate, &right_to_left_keys)?
                {
                    left_pushdown.push(left_predicate);
                }
                right_pushdown.push(right_predicate);
            }
            _ => remaining.push(conjunct),
        }
    }

    Ok(SplitJoinFilter {
        left_pushdown: combine_filters(left_pushdown),
        right_pushdown: combine_filters(right_pushdown),
        remaining: combine_filters(remaining),
        required_columns,
    })
}

pub(super) fn split_conjuncts(predicate: &Expr) -> Vec<Expr> {
    match predicate {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            let mut conjuncts = split_conjuncts(binary.left.as_ref());
            conjuncts.extend(split_conjuncts(binary.right.as_ref()));
            conjuncts
        }
        _ => vec![predicate.clone()],
    }
}

pub(super) fn expression_output_columns(
    expression: &Expr,
    output_schema: &RowSchema,
) -> Result<BTreeSet<usize>, PlannerError> {
    let mut columns = BTreeSet::new();
    add_required_expression_columns(expression, output_schema, &mut columns)?;
    Ok(columns)
}

pub(super) fn rewrite_join_output_expr_for_side(
    expression: Expr,
    join: &DbspJoinNode,
    side: JoinInputSide,
) -> Result<Expr, PlannerError> {
    expression
        .transform_up(|expr| match expr {
            Expr::Column(column) => {
                let output_idx = join
                    .output_schema
                    .field_index(column.name.as_str())
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "column '{}' not found in join output schema",
                            column.name
                        ))
                    })?;
                let rewritten = match side {
                    JoinInputSide::Left if output_idx < join.left_schema.len() => {
                        let field = join.left_schema.field(output_idx).ok_or_else(|| {
                            DataFusionError::Plan(format!(
                                "left join output column index {output_idx} out of bounds",
                            ))
                        })?;
                        Expr::Column(Column::new_unqualified(field.name.clone()))
                    }
                    JoinInputSide::Right if output_idx >= join.left_schema.len() => {
                        let right_idx = output_idx - join.left_schema.len();
                        let field = join.right_schema.field(right_idx).ok_or_else(|| {
                            DataFusionError::Plan(format!(
                                "right join output column index {right_idx} out of bounds",
                            ))
                        })?;
                        Expr::Column(Column::new_unqualified(field.name.clone()))
                    }
                    JoinInputSide::Left => {
                        return Err(DataFusionError::Plan(format!(
                            "attempted to rewrite right-side join column '{}' onto left input",
                            column.name
                        )));
                    }
                    JoinInputSide::Right => {
                        return Err(DataFusionError::Plan(format!(
                            "attempted to rewrite left-side join column '{}' onto right input",
                            column.name
                        )));
                    }
                };
                Ok(Transformed::yes(rewritten))
            }
            other => Ok(Transformed::no(other)),
        })
        .map(|result| result.data)
        .map_err(|err| PlannerError::AnalysisError(err.into()))
}

pub(super) fn join_key_column_mapping(
    join: &DbspJoinNode,
    from_side: JoinInputSide,
) -> HashMap<String, Vec<String>> {
    let mut classes: Vec<BTreeSet<JoinKeyRef>> = Vec::new();
    for key in &join.keys {
        let (Expr::Column(left), Expr::Column(right)) =
            (key.left_expression().expr(), key.right_expression().expr())
        else {
            continue;
        };
        merge_join_key_refs(
            &mut classes,
            JoinKeyRef {
                side: JoinInputSide::Left,
                name: left.name.clone(),
            },
            JoinKeyRef {
                side: JoinInputSide::Right,
                name: right.name.clone(),
            },
        );
    }

    let to_side = from_side.opposite();
    let mut mapping: HashMap<String, BTreeSet<String>> = HashMap::new();
    for class in classes {
        let targets = class
            .iter()
            .filter(|column| column.side == to_side)
            .map(|column| column.name.clone())
            .collect::<BTreeSet<_>>();
        if targets.is_empty() {
            continue;
        }

        for source in class.iter().filter(|column| column.side == from_side) {
            mapping
                .entry(source.name.clone())
                .or_default()
                .extend(targets.iter().cloned());
        }
    }

    mapping
        .into_iter()
        .map(|(source, targets)| (source, targets.into_iter().collect()))
        .collect()
}

pub(super) fn merge_join_key_refs(
    classes: &mut Vec<BTreeSet<JoinKeyRef>>,
    left: JoinKeyRef,
    right: JoinKeyRef,
) {
    let mut matched = Vec::new();
    for (idx, class) in classes.iter().enumerate() {
        if class.contains(&left) || class.contains(&right) {
            matched.push(idx);
        }
    }

    if matched.is_empty() {
        classes.push(BTreeSet::from([left, right]));
        return;
    }

    let first = matched[0];
    classes[first].insert(left);
    classes[first].insert(right);
    for idx in matched.into_iter().skip(1).rev() {
        let other = classes.remove(idx);
        classes[first].extend(other);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct JoinKeyRef {
    side: JoinInputSide,
    name: String,
}

pub(super) fn rewrite_key_predicates_for_opposite_side(
    predicate: &Expr,
    key_mapping: &HashMap<String, Vec<String>>,
) -> Result<Vec<Expr>, PlannerError> {
    if predicate.is_volatile() || key_mapping.is_empty() {
        return Ok(Vec::new());
    }

    let source_columns = predicate
        .column_refs()
        .iter()
        .map(|column| column.name.clone())
        .collect::<BTreeSet<_>>();
    if source_columns.is_empty() {
        return Ok(Vec::new());
    }

    let mut target_sets = Vec::with_capacity(source_columns.len());
    for source in &source_columns {
        let Some(targets) = key_mapping.get(source) else {
            return Ok(Vec::new());
        };
        if targets.is_empty() {
            return Ok(Vec::new());
        }
        target_sets.push((source.as_str(), targets.as_slice()));
    }

    if target_sets.iter().all(|(_, targets)| targets.len() == 1) {
        let mapping = target_sets
            .iter()
            .map(|(source, targets)| ((*source).to_string(), targets[0].clone()))
            .collect::<HashMap<_, _>>();
        return rewrite_key_predicate_columns(predicate, &mapping).map(|expr| vec![expr]);
    }

    if target_sets.len() == 1 {
        let (source, targets) = target_sets[0];
        return targets
            .iter()
            .map(|target| {
                rewrite_key_predicate_columns(
                    predicate,
                    &HashMap::from([(source.to_string(), target.clone())]),
                )
            })
            .collect();
    }

    Ok(Vec::new())
}

pub(super) fn rewrite_key_predicate_columns(
    predicate: &Expr,
    column_mapping: &HashMap<String, String>,
) -> Result<Expr, PlannerError> {
    predicate
        .clone()
        .transform_up(|expr| match expr {
            Expr::Column(column) => {
                let Some(target_name) = column_mapping.get(&column.name) else {
                    return Ok(Transformed::no(Expr::Column(column)));
                };
                Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                    target_name.clone(),
                ))))
            }
            other => Ok(Transformed::no(other)),
        })
        .map(|result| result.data)
        .map_err(|err| PlannerError::AnalysisError(err.into()))
}

pub(super) fn prune_redundant_join_key_pairs(
    key_pairs: Vec<(Expr, Expr)>,
) -> Result<Vec<(Expr, Expr)>, PlannerError> {
    let mut left_to_right = HashMap::new();
    let mut right_to_left = HashMap::new();
    for (left, right) in &key_pairs {
        if let (Expr::Column(left), Expr::Column(right)) = (left, right) {
            left_to_right
                .entry(left.name.clone())
                .or_insert_with(|| right.name.clone());
            right_to_left
                .entry(right.name.clone())
                .or_insert_with(|| left.name.clone());
        }
    }
    if left_to_right.is_empty() {
        return Ok(key_pairs);
    }

    let mut pruned = Vec::with_capacity(key_pairs.len());
    for (left, right) in key_pairs {
        let direct_column_key = matches!((&left, &right), (Expr::Column(_), Expr::Column(_)));
        let redundant = !direct_column_key
            && (rewrite_join_key_columns(&left, &left_to_right)?
                .is_some_and(|rewritten| rewritten == right)
                || rewrite_join_key_columns(&right, &right_to_left)?
                    .is_some_and(|rewritten| rewritten == left));
        if !redundant {
            pruned.push((left, right));
        }
    }
    Ok(pruned)
}

pub(super) fn rewrite_join_key_columns(
    expression: &Expr,
    column_mapping: &HashMap<String, String>,
) -> Result<Option<Expr>, PlannerError> {
    let mut saw_mapped_column = false;
    let mut saw_unmapped_column = false;
    let rewritten = expression
        .clone()
        .transform_up(|expr| match expr {
            Expr::Column(column) => {
                let Some(target_name) = column_mapping.get(&column.name) else {
                    saw_unmapped_column = true;
                    return Ok(Transformed::no(Expr::Column(column)));
                };
                saw_mapped_column = true;
                Ok(Transformed::yes(Expr::Column(Column::new_unqualified(
                    target_name.clone(),
                ))))
            }
            other => Ok(Transformed::no(other)),
        })
        .map(|result| result.data)
        .map_err(|err| PlannerError::AnalysisError(err.into()))?;
    Ok((saw_mapped_column && !saw_unmapped_column).then_some(rewritten))
}

pub(super) fn add_required_expression_columns(
    expression: &Expr,
    input_schema: &RowSchema,
    columns: &mut BTreeSet<usize>,
) -> Result<(), PlannerError> {
    for column in expression.column_refs() {
        let column_idx = input_schema
            .field_index(column.name.as_str())
            .ok_or_else(|| {
                PlannerError::AnalysisError(anyhow::anyhow!(
                    "column '{}' not found in input schema",
                    column.name
                ))
            })?;
        columns.insert(column_idx);
    }
    Ok(())
}

pub(super) fn split_join_required_columns(
    columns: &BTreeSet<usize>,
    left_width: usize,
    left_columns: &mut BTreeSet<usize>,
    right_columns: &mut BTreeSet<usize>,
) -> Result<(), PlannerError> {
    for column_idx in columns {
        if *column_idx < left_width {
            left_columns.insert(*column_idx);
        } else {
            let right_idx = column_idx.checked_sub(left_width).ok_or_else(|| {
                PlannerError::AnalysisError(anyhow::anyhow!(
                    "join column index underflow for {column_idx}",
                ))
            })?;
            right_columns.insert(right_idx);
        }
    }
    Ok(())
}
