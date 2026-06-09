use super::core::row_schema_from_dfschema;
use super::row_number_helpers::*;
use super::*;

impl<'cfg> PlannerContext<'cfg> {
    pub(super) fn plan_aggregate(
        &mut self,
        aggregate: &datafusion::logical_expr::logical_plan::Aggregate,
    ) -> Result<PlannedNode, PlannerError> {
        let input = self.plan_node(&aggregate.input)?;

        if aggregate.group_expr.is_empty() && aggregate.aggr_expr.is_empty() {
            return Err(PlannerError::UnsupportedPlan(
                "aggregate nodes must have group or aggregate expressions".to_string(),
            ));
        }

        let mut group_keys = Vec::with_capacity(aggregate.group_expr.len());
        let mut window_spec: Option<DbspWindowSpec> = None;
        for expr in &aggregate.group_expr {
            if let Some(spec) = self.parse_window_spec(expr, input.schema.clone())? {
                if window_spec.is_some() {
                    return Err(PlannerError::UnsupportedPlan(
                        "only one window specification is supported in GROUP BY".to_string(),
                    ));
                }
                window_spec = Some(spec);
                continue;
            }
            let default_alias = match expr {
                Expr::Column(column) => column.name.clone(),
                Expr::Alias(alias) => match alias.expr.as_ref() {
                    Expr::Column(column) => column.name.clone(),
                    _ => expr.schema_name().to_string(),
                },
                _ => expr.schema_name().to_string(),
            };
            let (group_expr, alias) = extract_alias(expr.clone())?;
            group_keys.push((group_expr, alias.or(Some(default_alias))));
        }

        let aggregates = aggregate
            .aggr_expr
            .iter()
            .map(map_aggregate_expr)
            .collect::<Result<Vec<_>, _>>()?;

        let agg_node = DbspAggregateNode::try_new(input.schema.clone(), group_keys, aggregates)?;

        if let Some(window_spec) = window_spec {
            let output_schema = self.window_output_schema(&agg_node, &window_spec)?;
            let window_node = DbspWindowAggregateNode {
                aggregate: agg_node,
                window: window_spec,
            };
            let id = self.add_node(
                vec![input.id],
                DbspNodeKind::WindowAggregate(window_node),
                output_schema.clone(),
            );
            return Ok(PlannedNode {
                id,
                schema: output_schema,
            });
        }

        let output_schema = agg_node.output_schema().clone();
        let id = self.add_node(
            vec![input.id],
            DbspNodeKind::Aggregate(agg_node),
            output_schema.clone(),
        );
        Ok(PlannedNode {
            id,
            schema: output_schema,
        })
    }

    pub(super) fn plan_limit(
        &mut self,
        limit: &datafusion::logical_expr::logical_plan::Limit,
    ) -> Result<PlannedNode, PlannerError> {
        let fetch = match limit
            .get_fetch_type()
            .map_err(|err| PlannerError::UnsupportedPlan(err.to_string()))?
        {
            FetchType::Literal(Some(value)) => value,
            FetchType::Literal(None) => {
                return Err(PlannerError::UnsupportedPlan(
                    "LIMIT without FETCH is not supported".to_string(),
                ));
            }
            FetchType::UnsupportedExpr => {
                return Err(PlannerError::UnsupportedPlan(
                    "LIMIT expressions must be literal integers".to_string(),
                ));
            }
        };
        let offset = match limit
            .get_skip_type()
            .map_err(|err| PlannerError::UnsupportedPlan(err.to_string()))?
        {
            SkipType::Literal(value) => value,
            SkipType::UnsupportedExpr => {
                return Err(PlannerError::UnsupportedPlan(
                    "OFFSET expressions must be literal integers".to_string(),
                ));
            }
        };
        if fetch == 0 {
            let output_schema = row_schema_from_dfschema(limit.input.schema().as_ref())?;
            return Ok(self.build_empty_node(output_schema));
        }

        match limit.input.as_ref() {
            LogicalPlan::Sort(sort) => {
                return self.build_topn_from_sort(sort, fetch, offset);
            }
            LogicalPlan::Projection(projection) => {
                if let LogicalPlan::Sort(sort) = projection.input.as_ref() {
                    let topn = self.build_topn_from_sort(sort, fetch, offset)?;
                    return self.build_projection_node(topn, projection);
                }
            }
            _ => {}
        }

        Err(PlannerError::UnsupportedPlan(
            "LIMIT requires an ORDER BY to form a TopN operator".to_string(),
        ))
    }

    fn build_topn_from_sort(
        &mut self,
        sort: &datafusion::logical_expr::logical_plan::Sort,
        fetch: usize,
        offset: usize,
    ) -> Result<PlannedNode, PlannerError> {
        let input = self.plan_node(&sort.input)?;
        let fetch = if let Some(sort_fetch) = sort.fetch {
            match sort_fetch.checked_sub(offset) {
                Some(remaining) => fetch.min(remaining),
                None => 0,
            }
        } else {
            fetch
        };
        if fetch == 0 {
            return Ok(self.build_empty_node(input.schema));
        }

        let order_by = self.map_sort_expressions(&sort.expr, input.schema.clone())?;
        let topn =
            DbspTopNNode::try_new(input.schema.clone(), Vec::new(), order_by, fetch, offset)?;
        let output_schema = topn.output_schema().clone();
        let id = self.add_node(
            vec![input.id],
            DbspNodeKind::TopN(topn),
            output_schema.clone(),
        );
        Ok(PlannedNode {
            id,
            schema: output_schema,
        })
    }

    pub(super) fn build_empty_node(&mut self, output_schema: Arc<RowSchema>) -> PlannedNode {
        let empty = DbspEmptyNode::new(output_schema.clone());
        let id = self.add_node(vec![], DbspNodeKind::Empty(empty), output_schema.clone());
        PlannedNode {
            id,
            schema: output_schema,
        }
    }

    pub(super) fn parse_row_number_spec(
        &self,
        expr: &Expr,
    ) -> Result<Option<RowNumberSpec>, PlannerError> {
        let (alias, window) = match expr {
            Expr::Alias(alias) => {
                let Expr::WindowFunction(window) = alias.expr.as_ref() else {
                    return Ok(None);
                };
                (alias.name.clone(), window)
            }
            Expr::WindowFunction(window) => (expr.schema_name().to_string(), window),
            _ => return Ok(None),
        };
        let is_row_number = matches!(
            &window.fun,
            WindowFunctionDefinition::WindowUDF(udf)
                if udf.name().eq_ignore_ascii_case("row_number")
        );
        if !is_row_number {
            return Ok(None);
        }
        if window.params.filter.is_some()
            || window.params.null_treatment.is_some()
            || window.params.distinct
        {
            return Ok(None);
        }

        let partition_by = window
            .params
            .partition_by
            .iter()
            .cloned()
            .map(normalize_expr)
            .collect::<Result<Vec<_>, _>>()?;
        let order_by = window.params.order_by.clone();
        Ok(Some((alias, partition_by, order_by)))
    }

    pub(super) fn strip_passthrough_wrappers<'a>(
        &self,
        mut plan: &'a LogicalPlan,
    ) -> &'a LogicalPlan {
        loop {
            match plan {
                LogicalPlan::SubqueryAlias(alias) => {
                    plan = alias.input.as_ref();
                }
                LogicalPlan::Repartition(repartition) => {
                    plan = repartition.input.as_ref();
                }
                _ => return plan,
            }
        }
    }

    pub(super) fn extract_window_plan<'a>(
        &self,
        input: &'a LogicalPlan,
        rank_column: &str,
    ) -> Option<(
        &'a datafusion::logical_expr::logical_plan::Window,
        Option<Vec<Expr>>,
    )> {
        let direct = self.strip_passthrough_wrappers(input);
        if let LogicalPlan::Window(window) = direct {
            return Some((window, None));
        }

        let projection = match direct {
            LogicalPlan::Projection(projection) => projection,
            _ => return None,
        };
        let window = match self.strip_passthrough_wrappers(projection.input.as_ref()) {
            LogicalPlan::Window(window) => window,
            _ => return None,
        };

        let mut saw_rank = false;
        let mut remaining = Vec::with_capacity(projection.expr.len());
        for expr in &projection.expr {
            if projection_expr_matches_rank(expr, rank_column) {
                saw_rank = true;
                continue;
            }
            remaining.push(expr.clone());
        }
        if !saw_rank {
            return None;
        }
        Some((window, Some(remaining)))
    }

    pub(super) fn plan_union(
        &mut self,
        union: &datafusion::logical_expr::logical_plan::Union,
    ) -> Result<PlannedNode, PlannerError> {
        let mut input_nodes = Vec::with_capacity(union.inputs.len());
        let mut schemas = Vec::with_capacity(union.inputs.len());
        for input in &union.inputs {
            let planned = self.plan_node(input)?;
            schemas.push(planned.schema.clone());
            input_nodes.push(planned);
        }

        if schemas.is_empty() {
            return Err(PlannerError::UnsupportedPlan(
                "union requires at least one input".to_string(),
            ));
        }

        let union_node = DbspUnionNode::try_new(schemas)?;
        let output_schema = union_node.output_schema().clone();
        let input_ids = input_nodes.iter().map(|node| node.id).collect::<Vec<_>>();
        let id = self.add_node(
            input_ids,
            DbspNodeKind::Union(union_node),
            output_schema.clone(),
        );
        Ok(PlannedNode {
            id,
            schema: output_schema,
        })
    }

    pub(super) fn plan_distinct(
        &mut self,
        distinct: &datafusion::logical_expr::logical_plan::Distinct,
    ) -> Result<PlannedNode, PlannerError> {
        let input_plan = match distinct {
            datafusion::logical_expr::logical_plan::Distinct::All(input) => input,
            datafusion::logical_expr::logical_plan::Distinct::On(_) => {
                return Err(PlannerError::UnsupportedPlan(
                    "DISTINCT ON is not supported".to_string(),
                ));
            }
        };
        let input = self.plan_node(input_plan)?;
        let distinct_node = DbspDistinctNode::new(input.schema.clone());
        let output_schema = distinct_node.output_schema().clone();
        let id = self.add_node(
            vec![input.id],
            DbspNodeKind::Distinct(distinct_node),
            output_schema.clone(),
        );
        Ok(PlannedNode {
            id,
            schema: output_schema,
        })
    }

    pub(super) fn parse_window_spec(
        &self,
        expr: &Expr,
        input_schema: Arc<RowSchema>,
    ) -> Result<Option<DbspWindowSpec>, PlannerError> {
        let expr = match expr {
            Expr::Alias(alias) => alias.expr.as_ref(),
            _ => expr,
        };
        let Expr::ScalarFunction(func) = expr else {
            return Ok(None);
        };

        let name = func.name().to_ascii_lowercase();
        match name.as_str() {
            "tumble" => {
                if !matches!(func.args.len(), 2 | 3) {
                    return Err(PlannerError::UnsupportedPlan(
                        "TUMBLE requires (time_expr, size_ms[, allowed_lateness_ms]) arguments"
                            .to_string(),
                    ));
                }
                let time_expr = normalize_expr(func.args[0].clone())?;
                let size_ms = self.parse_window_arg(&func.args[1])?;
                let allowed_lateness_ms = if func.args.len() == 3 {
                    self.parse_window_arg(&func.args[2])?
                } else {
                    DEFAULT_WINDOW_ALLOWED_LATENESS_MS
                };
                let spec = DbspWindowSpec::try_new(
                    DbspWindowPolicy::Tumbling { size_ms },
                    time_expr,
                    input_schema,
                    allowed_lateness_ms,
                )?;
                Ok(Some(spec))
            }
            "hop" => {
                if !matches!(func.args.len(), 3 | 4) {
                    return Err(PlannerError::UnsupportedPlan(
                        "HOP requires (time_expr, slide_ms, size_ms[, allowed_lateness_ms]) arguments"
                            .to_string(),
                    ));
                }
                let time_expr = normalize_expr(func.args[0].clone())?;
                let slide_ms = self.parse_window_arg(&func.args[1])?;
                let size_ms = self.parse_window_arg(&func.args[2])?;
                let allowed_lateness_ms = if func.args.len() == 4 {
                    self.parse_window_arg(&func.args[3])?
                } else {
                    DEFAULT_WINDOW_ALLOWED_LATENESS_MS
                };
                let spec = DbspWindowSpec::try_new(
                    DbspWindowPolicy::Hopping { size_ms, slide_ms },
                    time_expr,
                    input_schema,
                    allowed_lateness_ms,
                )?;
                Ok(Some(spec))
            }
            "session" => {
                if !matches!(func.args.len(), 2 | 3) {
                    return Err(PlannerError::UnsupportedPlan(
                        "SESSION requires (time_expr, gap_ms[, allowed_lateness_ms]) arguments"
                            .to_string(),
                    ));
                }
                let time_expr = normalize_expr(func.args[0].clone())?;
                let gap_ms = self.parse_window_arg(&func.args[1])?;
                let allowed_lateness_ms = if func.args.len() == 3 {
                    self.parse_window_arg(&func.args[2])?
                } else {
                    DEFAULT_WINDOW_ALLOWED_LATENESS_MS
                };
                let spec = DbspWindowSpec::try_new(
                    DbspWindowPolicy::Session { gap_ms },
                    time_expr,
                    input_schema,
                    allowed_lateness_ms,
                )?;
                Ok(Some(spec))
            }
            _ => Ok(None),
        }
    }

    pub(super) fn parse_window_arg(&self, expr: &Expr) -> Result<i64, PlannerError> {
        let Expr::Literal(value, _) = expr else {
            return Err(PlannerError::UnsupportedPlan(
                "window sizes must be literal Int64 milliseconds".to_string(),
            ));
        };
        let array = value.to_array().map_err(|_| {
            PlannerError::UnsupportedPlan(
                "window sizes must be literal Int64 milliseconds".to_string(),
            )
        })?;
        let values = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
            PlannerError::UnsupportedPlan(
                "window sizes must be literal Int64 milliseconds".to_string(),
            )
        })?;
        if values.is_null(0) {
            return Err(PlannerError::UnsupportedPlan(
                "window sizes must be literal Int64 milliseconds".to_string(),
            ));
        }
        Ok(values.value(0))
    }

    pub(super) fn window_output_schema(
        &self,
        aggregate: &DbspAggregateNode,
        window_spec: &DbspWindowSpec,
    ) -> Result<Arc<RowSchema>, PlannerError> {
        let nullable = window_spec.time_expression.nullable();
        let mut fields = Vec::with_capacity(2 + aggregate.output_schema().fields().len());
        fields.push(Field::new(
            "window_start",
            DbspScalarType::TimestampMillis,
            nullable,
        ));
        fields.push(Field::new(
            "window_end",
            DbspScalarType::TimestampMillis,
            nullable,
        ));
        fields.extend(aggregate.output_schema().fields().iter().cloned());
        RowSchema::try_new(fields).map_err(PlannerError::from)
    }

    pub(super) fn add_node(
        &mut self,
        inputs: Vec<usize>,
        kind: DbspNodeKind,
        schema: Arc<RowSchema>,
    ) -> usize {
        let id = self.nodes.len();
        self.nodes.push(CircuitNode {
            id,
            kind,
            inputs,
            output_schema: schema.clone(),
        });
        id
    }

    pub(super) fn map_sort_expressions(
        &self,
        expressions: &[ExprSort],
        input_schema: Arc<RowSchema>,
    ) -> Result<Vec<OrderExpr>, PlannerError> {
        expressions
            .iter()
            .map(|sort| {
                let expr = normalize_expr(sort.expr.clone())?;
                OrderExpr::try_new(expr, input_schema.clone(), sort.asc, sort.nulls_first)
                    .map_err(PlannerError::from)
            })
            .collect()
    }
}
