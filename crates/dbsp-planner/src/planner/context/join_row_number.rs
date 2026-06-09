use super::join_helpers::*;
use super::row_number_helpers::*;
use super::*;

impl<'cfg> PlannerContext<'cfg> {
    pub(super) fn plan_row_number_filter(
        &mut self,
        filter: &datafusion::logical_expr::logical_plan::Filter,
    ) -> Result<Option<PlannedNode>, PlannerError> {
        let Some((rank_column, limit, offset, residual_predicate)) =
            extract_row_number_limit_with_residual(&filter.predicate)?
        else {
            return Ok(None);
        };

        let (window_plan, post_projection) =
            match self.extract_window_plan(filter.input.as_ref(), &rank_column) {
                Some(found) => found,
                None => return Ok(None),
            };

        if window_plan.window_expr.len() != 1 {
            return Ok(None);
        }
        let Some((rank_alias, partition_by, order_by)) =
            self.parse_row_number_spec(&window_plan.window_expr[0])?
        else {
            return Ok(None);
        };

        if rank_column != rank_alias && post_projection.is_none() {
            return Ok(None);
        }

        let input = self.plan_node(&window_plan.input)?;
        let (partition_by, order_by, post_projection) = if let Some(join) =
            join_for_row_number_rewrite(self, input.id)
        {
            let mut join_relation_exprs = partition_by.clone();
            join_relation_exprs.extend(order_by.iter().map(|sort| sort.expr.clone()));
            if let Some(expressions) = post_projection.as_ref() {
                join_relation_exprs.extend(expressions.iter().cloned());
            }
            let join_relation_sides =
                infer_join_relation_sides(Some(join_relation_exprs.as_slice()), None, join);
            let rewritten_partition_by = partition_by
                .into_iter()
                .map(|expr| rewrite_join_output_projection_expr(expr, join, &join_relation_sides))
                .collect::<Result<Vec<_>, PlannerError>>()?;
            let rewritten_order_by = order_by
                .into_iter()
                .map(|sort| {
                    Ok(ExprSort {
                        expr: rewrite_join_output_projection_expr(
                            sort.expr,
                            join,
                            &join_relation_sides,
                        )?,
                        asc: sort.asc,
                        nulls_first: sort.nulls_first,
                    })
                })
                .collect::<Result<Vec<_>, PlannerError>>()?;
            let rewritten_post_projection = post_projection
                .map(|expressions| {
                    expressions
                        .into_iter()
                        .map(|expr| {
                            rewrite_join_output_projection_expr(expr, join, &join_relation_sides)
                        })
                        .collect::<Result<Vec<_>, PlannerError>>()
                })
                .transpose()?;
            (
                rewritten_partition_by,
                rewritten_order_by,
                rewritten_post_projection,
            )
        } else {
            (partition_by, order_by, post_projection)
        };
        let order_by = self.map_sort_expressions(&order_by, input.schema.clone())?;
        let topn =
            DbspTopNNode::try_new(input.schema.clone(), partition_by, order_by, limit, offset)?;
        let output_schema = topn.output_schema().clone();
        let id = self.add_node(
            vec![input.id],
            DbspNodeKind::TopN(topn),
            output_schema.clone(),
        );
        let topn_node = PlannedNode {
            id,
            schema: output_schema,
        };

        let mut output = if let Some(exprs) = post_projection {
            self.build_projection_items(topn_node, &exprs)?
        } else {
            topn_node
        };

        if let Some(residual_predicate) = residual_predicate {
            let select = DbspSelectNode::try_new(output.schema.clone(), residual_predicate)?;
            let id = self.add_node(
                vec![output.id],
                DbspNodeKind::Select(select),
                output.schema.clone(),
            );
            output = PlannedNode {
                id,
                schema: output.schema,
            };
        }

        Ok(Some(output))
    }

    pub(super) fn plan_join(
        &mut self,
        join: &datafusion::logical_expr::logical_plan::Join,
    ) -> Result<PlannedNode, PlannerError> {
        let join_type = match join.join_type {
            JoinType::Inner => DbspJoinType::Inner,
            JoinType::Left => DbspJoinType::LeftOuter,
            JoinType::Right => DbspJoinType::RightOuter,
            JoinType::Full => DbspJoinType::FullOuter,
            JoinType::LeftSemi => DbspJoinType::LeftSemi,
            JoinType::RightSemi => DbspJoinType::RightSemi,
            JoinType::LeftAnti => DbspJoinType::LeftAnti,
            JoinType::RightAnti => DbspJoinType::RightAnti,
            other => {
                return Err(PlannerError::UnsupportedJoin(format!(
                    "unsupported join type {other:?}; INNER/LEFT/RIGHT/FULL OUTER/SEMI/ANTI joins are supported"
                )));
            }
        };

        let left = self.plan_node(&join.left)?;
        let right = self.plan_node(&join.right)?;

        let mut key_pairs = join
            .on
            .iter()
            .map(|(left_expr, right_expr)| {
                let left = normalize_expr(left_expr.clone())?;
                let right = normalize_expr(right_expr.clone())?;
                Ok((left, right))
            })
            .collect::<Result<Vec<_>, PlannerError>>()?;

        let mut residuals: Vec<Expr> = Vec::new();
        if let Some(filter_expr) = &join.filter {
            let (filter_keys, filter_residual) = extract_join_keys_and_residual(filter_expr)?;
            key_pairs.extend(filter_keys);
            if let Some(expr) = filter_residual {
                residuals.push(expr);
            }
        }

        if key_pairs.is_empty() {
            if let Some(filter_expr) = &join.filter
                && matches!(join_type, DbspJoinType::Inner)
            {
                let (range, range_residual) = extract_range_join_and_residual(
                    filter_expr,
                    left.schema.as_ref(),
                    right.schema.as_ref(),
                )?;
                if let Some(range) = range {
                    let range_join_node = DbspJoinNode::try_new_range(
                        left.schema.clone(),
                        right.schema.clone(),
                        range.right_key,
                        range.left_lower,
                        range.left_upper,
                        range_residual,
                    )
                    .map_err(|err| PlannerError::UnsupportedJoin(err.to_string()))?;
                    let output_schema = range_join_node.output_schema.clone();
                    let id = self.add_node(
                        vec![left.id, right.id],
                        DbspNodeKind::Join(Box::new(range_join_node)),
                        output_schema.clone(),
                    );
                    return Ok(PlannedNode {
                        id,
                        schema: output_schema,
                    });
                }
                let (asof, asof_residual) = extract_asof_join_and_residual(
                    filter_expr,
                    left.schema.as_ref(),
                    right.schema.as_ref(),
                )?;
                if let Some(asof) = asof {
                    let asof_join_node = DbspJoinNode::try_new_asof(
                        DbspJoinType::Inner,
                        left.schema.clone(),
                        right.schema.clone(),
                        Vec::new(),
                        asof.left_timestamp,
                        asof.right_timestamp,
                        asof_residual,
                    )
                    .map_err(|err| PlannerError::UnsupportedJoin(err.to_string()))?;
                    let output_schema = asof_join_node.output_schema.clone();
                    let id = self.add_node(
                        vec![left.id, right.id],
                        DbspNodeKind::Join(Box::new(asof_join_node)),
                        output_schema.clone(),
                    );
                    return Ok(PlannedNode {
                        id,
                        schema: output_schema,
                    });
                }
            }
            return Err(PlannerError::UnsupportedJoin(
                "joins must have at least one equi-key, a half-open range predicate, or an ASOF predicate".to_string(),
            ));
        }
        let key_pairs = prune_redundant_join_key_pairs(key_pairs)?;

        let residual = combine_filters(residuals);
        if !matches!(join_type, DbspJoinType::Inner) && residual.is_some() {
            return Err(PlannerError::UnsupportedJoin(
                "non-INNER joins currently require pure equi-join predicates".to_string(),
            ));
        }

        let join_node = DbspJoinNode::try_new(
            join_type,
            left.schema.clone(),
            right.schema.clone(),
            key_pairs,
            residual,
        )?;
        let output_schema = join_node.output_schema.clone();
        let id = self.add_node(
            vec![left.id, right.id],
            DbspNodeKind::Join(Box::new(join_node)),
            output_schema.clone(),
        );
        Ok(PlannedNode {
            id,
            schema: output_schema,
        })
    }

    pub(super) fn plan_asof_extension(
        &mut self,
        asof_node: &FloeAsofJoinNode,
    ) -> Result<PlannedNode, PlannerError> {
        let join_type = match asof_node.join_type() {
            JoinType::Inner => DbspJoinType::Inner,
            JoinType::Left => DbspJoinType::LeftOuter,
            other => {
                return Err(PlannerError::UnsupportedJoin(format!(
                    "unsupported ASOF join type {other:?}; INNER and LEFT ASOF joins are supported"
                )));
            }
        };

        let left = self.plan_node(asof_node.left())?;
        let right = self.plan_node(asof_node.right())?;
        let mut key_pairs = asof_node
            .on()
            .iter()
            .map(|(left_expr, right_expr)| {
                let left = normalize_expr(left_expr.clone())?;
                let right = normalize_expr(right_expr.clone())?;
                Ok((left, right))
            })
            .collect::<Result<Vec<_>, PlannerError>>()?;

        let filter = asof_node.filter().ok_or_else(|| {
            PlannerError::UnsupportedJoin(
                "ASOF joins require a MATCH_CONDITION predicate".to_string(),
            )
        })?;
        let (asof, residual) = extract_asof_join_and_residual_with_logical_schemas(
            filter,
            left.schema.as_ref(),
            right.schema.as_ref(),
            asof_node.left().schema().as_ref(),
            asof_node.right().schema().as_ref(),
        )?;
        let Some(asof) = asof else {
            return Err(PlannerError::UnsupportedJoin(
                "ASOF joins require exactly one right_timestamp <= left_timestamp predicate"
                    .to_string(),
            ));
        };
        let residual = if let Some(residual) = residual {
            let (filter_keys, filter_residual) =
                extract_join_keys_and_residual_with_logical_schemas(
                    &residual,
                    left.schema.as_ref(),
                    right.schema.as_ref(),
                    asof_node.left().schema().as_ref(),
                    asof_node.right().schema().as_ref(),
                )?;
            key_pairs.extend(filter_keys);
            filter_residual
        } else {
            None
        };
        let key_pairs = prune_redundant_join_key_pairs(key_pairs)?;

        let asof_join_node = DbspJoinNode::try_new_asof(
            join_type,
            left.schema.clone(),
            right.schema.clone(),
            key_pairs,
            asof.left_timestamp,
            asof.right_timestamp,
            residual,
        )
        .map_err(|err| PlannerError::UnsupportedJoin(err.to_string()))?;
        let output_schema = asof_join_node.output_schema.clone();
        let id = self.add_node(
            vec![left.id, right.id],
            DbspNodeKind::Join(Box::new(asof_join_node)),
            output_schema.clone(),
        );
        Ok(PlannedNode {
            id,
            schema: output_schema,
        })
    }
}
