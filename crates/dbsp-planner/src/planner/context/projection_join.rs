use super::join_helpers::*;
use super::*;

impl<'cfg> PlannerContext<'cfg> {
    pub(super) fn build_projection_node(
        &mut self,
        input: PlannedNode,
        projection: &datafusion::logical_expr::logical_plan::Projection,
    ) -> Result<PlannedNode, PlannerError> {
        if let Some(optimized) =
            self.optimize_join_subtree(input.clone(), None, Some(&projection.expr))?
        {
            return Ok(optimized);
        }
        self.build_projection_items(input, &projection.expr)
    }

    pub(super) fn build_projection_items(
        &mut self,
        input: PlannedNode,
        expressions: &[Expr],
    ) -> Result<PlannedNode, PlannerError> {
        let rewritten_expressions =
            self.rewrite_projection_expressions_for_input(&input, expressions)?;
        let items = rewritten_expressions
            .iter()
            .map(|expr| {
                let (expression, alias) = extract_alias(expr.clone())?;
                Ok(ProjectItem {
                    expr: expression,
                    alias,
                })
            })
            .collect::<Result<Vec<ProjectItem>, PlannerError>>()?;
        let project = DbspProjectNode::try_new(input.schema.clone(), items)?;
        let output_schema = project.output_schema().clone();
        let id = self.add_node(
            vec![input.id],
            DbspNodeKind::Project(project),
            output_schema.clone(),
        );
        Ok(PlannedNode {
            id,
            schema: output_schema,
        })
    }

    pub(super) fn rewrite_projection_expressions_for_input(
        &self,
        input: &PlannedNode,
        expressions: &[Expr],
    ) -> Result<Vec<Expr>, PlannerError> {
        let join = self.join_node_for_projection_input(input.id);
        let Some(join) = join else {
            return Ok(expressions.to_vec());
        };
        let relation_sides = infer_join_relation_sides(Some(expressions), None, join);
        expressions
            .iter()
            .map(|expr| rewrite_join_output_projection_expr(expr.clone(), join, &relation_sides))
            .collect()
    }

    pub(super) fn join_node_for_projection_input(&self, input_id: usize) -> Option<&DbspJoinNode> {
        let node = self.node_by_id(input_id)?;
        match &node.kind {
            DbspNodeKind::Join(join) => Some(join),
            DbspNodeKind::Select(_) => {
                let join_input_id = *node.inputs.first()?;
                let join_node = self.node_by_id(join_input_id)?;
                match &join_node.kind {
                    DbspNodeKind::Join(join) => Some(join),
                    _ => None,
                }
            }
            _ => None,
        }
    }

    pub(super) fn optimize_join_subtree(
        &mut self,
        input: PlannedNode,
        top_filter: Option<Expr>,
        projection_exprs: Option<&[Expr]>,
    ) -> Result<Option<PlannedNode>, PlannerError> {
        let Some(input_node) = self.node_by_id(input.id).cloned() else {
            return Ok(None);
        };

        let (join_input_id, current_top_filter) = match &input_node.kind {
            DbspNodeKind::Join(_) => (input.id, top_filter),
            DbspNodeKind::Select(select) => {
                let Some(join_input_id) = input_node.inputs.first().copied() else {
                    return Ok(None);
                };
                (
                    join_input_id,
                    combine_optional_filters(
                        top_filter,
                        Some(select.predicate().expression().expr().clone()),
                    ),
                )
            }
            _ => return Ok(None),
        };

        let Some(join_node) = self.node_by_id(join_input_id).cloned() else {
            return Ok(None);
        };
        let DbspNodeKind::Join(join) = &join_node.kind else {
            return Ok(None);
        };
        if !matches!(join.join_type, DbspJoinType::Inner) || join_node.inputs.len() != 2 {
            return Ok(None);
        }

        let join_relation_sides =
            infer_join_relation_sides(projection_exprs, current_top_filter.as_ref(), join);
        let rewritten_projection_exprs = projection_exprs
            .map(|expressions| {
                expressions
                    .iter()
                    .map(|expr| {
                        rewrite_join_output_projection_expr(
                            expr.clone(),
                            join,
                            &join_relation_sides,
                        )
                    })
                    .collect::<Result<Vec<_>, PlannerError>>()
            })
            .transpose()?;

        let required_output_columns =
            if let Some(expressions) = rewritten_projection_exprs.as_deref() {
                required_columns_for_expressions(expressions, input.schema.as_ref())?
            } else {
                (0..input.schema.len()).collect::<BTreeSet<_>>()
            };
        let SplitJoinFilter {
            left_pushdown,
            right_pushdown,
            remaining,
            required_columns,
        } = split_join_filter(
            current_top_filter.as_ref(),
            join.output_schema.as_ref(),
            join.left_schema.len(),
            join,
        )?;

        let mut left_required = BTreeSet::new();
        let mut right_required = BTreeSet::new();
        split_join_required_columns(
            &required_output_columns,
            join.left_schema.len(),
            &mut left_required,
            &mut right_required,
        )?;
        split_join_required_columns(
            &required_columns,
            join.left_schema.len(),
            &mut left_required,
            &mut right_required,
        )?;
        for key in &join.keys {
            add_required_expression_columns(
                key.left_expression().expr(),
                join.left_schema.as_ref(),
                &mut left_required,
            )?;
            add_required_expression_columns(
                key.right_expression().expr(),
                join.right_schema.as_ref(),
                &mut right_required,
            )?;
        }
        if let Some(range) = &join.range {
            add_required_expression_columns(
                range.left_lower_expression().expr(),
                join.left_schema.as_ref(),
                &mut left_required,
            )?;
            add_required_expression_columns(
                range.left_upper_expression().expr(),
                join.left_schema.as_ref(),
                &mut left_required,
            )?;
            add_required_expression_columns(
                range.right_key_expression().expr(),
                join.right_schema.as_ref(),
                &mut right_required,
            )?;
        }
        if let Some(asof) = &join.asof {
            add_required_expression_columns(
                asof.left_timestamp_expression().expr(),
                join.left_schema.as_ref(),
                &mut left_required,
            )?;
            add_required_expression_columns(
                asof.right_timestamp_expression().expr(),
                join.right_schema.as_ref(),
                &mut right_required,
            )?;
        }
        if let Some(residual) = &join.residual {
            let mut residual_columns = BTreeSet::new();
            add_required_expression_columns(
                residual.expr(),
                join.output_schema.as_ref(),
                &mut residual_columns,
            )?;
            split_join_required_columns(
                &residual_columns,
                join.left_schema.len(),
                &mut left_required,
                &mut right_required,
            )?;
        }

        let left_full = (0..join.left_schema.len()).collect::<BTreeSet<_>>();
        let right_full = (0..join.right_schema.len()).collect::<BTreeSet<_>>();
        let changed = left_pushdown.is_some()
            || right_pushdown.is_some()
            || remaining != current_top_filter
            || left_required != left_full
            || right_required != right_full;
        if !changed {
            return Ok(None);
        }

        let left_input = PlannedNode {
            id: join_node.inputs[0],
            schema: Arc::clone(&join.left_schema),
        };
        let right_input = PlannedNode {
            id: join_node.inputs[1],
            schema: Arc::clone(&join.right_schema),
        };

        let left = self.build_required_projection(
            left_input,
            Arc::clone(&join.left_schema),
            &left_required,
        )?;
        let right = self.build_required_projection(
            right_input,
            Arc::clone(&join.right_schema),
            &right_required,
        )?;
        let left = self.build_optional_select(left, left_pushdown)?;
        let right = self.build_optional_select(right, right_pushdown)?;

        let residual = join
            .residual
            .as_ref()
            .map(|residual| residual.expr().clone());
        let rebuilt_join = if let Some(asof) = &join.asof {
            let key_pairs = join
                .keys
                .iter()
                .map(|key| {
                    (
                        key.left_expression().expr().clone(),
                        key.right_expression().expr().clone(),
                    )
                })
                .collect::<Vec<_>>();
            DbspJoinNode::try_new_asof(
                join.join_type.clone(),
                left.schema.clone(),
                right.schema.clone(),
                key_pairs,
                asof.left_timestamp_expression().expr().clone(),
                asof.right_timestamp_expression().expr().clone(),
                residual,
            )
        } else if let Some(range) = &join.range {
            DbspJoinNode::try_new_range(
                left.schema.clone(),
                right.schema.clone(),
                range.right_key_expression().expr().clone(),
                range.left_lower_expression().expr().clone(),
                range.left_upper_expression().expr().clone(),
                residual,
            )
        } else {
            let key_pairs = join
                .keys
                .iter()
                .map(|key| {
                    (
                        key.left_expression().expr().clone(),
                        key.right_expression().expr().clone(),
                    )
                })
                .collect::<Vec<_>>();
            DbspJoinNode::try_new(
                DbspJoinType::Inner,
                left.schema.clone(),
                right.schema.clone(),
                key_pairs,
                residual,
            )
        }?;
        let rebuilt_join_schema = rebuilt_join.output_schema.clone();
        let rebuilt_join_id = self.add_node(
            vec![left.id, right.id],
            DbspNodeKind::Join(Box::new(rebuilt_join)),
            rebuilt_join_schema.clone(),
        );
        let mut current = PlannedNode {
            id: rebuilt_join_id,
            schema: rebuilt_join_schema,
        };
        if let Some(remaining) = remaining {
            current = self.build_select(current, remaining)?;
        }
        if let Some(expressions) = rewritten_projection_exprs.as_deref() {
            return self.build_projection_items(current, expressions).map(Some);
        }
        Ok(Some(current))
    }

    pub(super) fn build_required_projection(
        &mut self,
        input: PlannedNode,
        input_schema: Arc<RowSchema>,
        required_columns: &BTreeSet<usize>,
    ) -> Result<PlannedNode, PlannerError> {
        if required_columns.len() == input_schema.len() {
            return Ok(input);
        }
        let items = required_columns
            .iter()
            .map(|column_idx| {
                let field = input_schema.field(*column_idx).ok_or_else(|| {
                    PlannerError::UnsupportedPlan(format!(
                        "required projection column {column_idx} out of bounds",
                    ))
                })?;
                Ok(ProjectItem {
                    expr: Expr::Column(Column::new_unqualified(field.name.clone())),
                    alias: Some(field.name.clone()),
                })
            })
            .collect::<Result<Vec<_>, PlannerError>>()?;
        let project = DbspProjectNode::try_new(input_schema, items)?;
        let output_schema = project.output_schema().clone();
        let id = self.add_node(
            vec![input.id],
            DbspNodeKind::Project(project),
            output_schema.clone(),
        );
        Ok(PlannedNode {
            id,
            schema: output_schema,
        })
    }

    pub(super) fn build_select(
        &mut self,
        input: PlannedNode,
        predicate: Expr,
    ) -> Result<PlannedNode, PlannerError> {
        let select = DbspSelectNode::try_new(input.schema.clone(), predicate)?;
        let id = self.add_node(
            vec![input.id],
            DbspNodeKind::Select(select),
            input.schema.clone(),
        );
        Ok(PlannedNode {
            id,
            schema: input.schema,
        })
    }

    pub(super) fn build_optional_select(
        &mut self,
        input: PlannedNode,
        predicate: Option<Expr>,
    ) -> Result<PlannedNode, PlannerError> {
        match predicate {
            Some(predicate) => self.build_select(input, predicate),
            None => Ok(input),
        }
    }
}
