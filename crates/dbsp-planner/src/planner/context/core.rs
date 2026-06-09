use super::*;

impl<'cfg> PlannerContext<'cfg> {
    pub(super) fn new(config: &'cfg PlannerConfig) -> Self {
        Self {
            config,
            nodes: Vec::new(),
        }
    }

    pub(super) fn into_reachable_nodes(
        self,
        root: usize,
    ) -> Result<Vec<CircuitNode>, PlannerError> {
        let mut reachable = BTreeSet::new();
        let mut stack = vec![root];
        while let Some(node_id) = stack.pop() {
            if !reachable.insert(node_id) {
                continue;
            }
            let node = self.node_by_id(node_id).ok_or_else(|| {
                PlannerError::UnsupportedPlan(format!(
                    "planned node {node_id} was not found during reachability pruning",
                ))
            })?;
            stack.extend(node.inputs.iter().copied());
        }

        Ok(self
            .nodes
            .into_iter()
            .filter(|node| reachable.contains(&node.id))
            .collect())
    }

    pub(super) fn node_by_id(&self, id: usize) -> Option<&CircuitNode> {
        self.nodes.iter().find(|node| node.id == id)
    }

    pub(super) fn plan_node(&mut self, plan: &LogicalPlan) -> Result<PlannedNode, PlannerError> {
        match plan {
            LogicalPlan::TableScan(scan) => {
                let table = self
                    .config
                    .table(&scan.table_name)
                    .ok_or_else(|| PlannerError::TableNotFound(scan.table_name.to_string()))?;
                let source_schema = table.schema().clone();
                let source_id = self.add_node(
                    vec![],
                    DbspNodeKind::Source(DbspSourceNode {
                        table: table.clone(),
                    }),
                    source_schema.clone(),
                );
                let mut current = PlannedNode {
                    id: source_id,
                    schema: source_schema.clone(),
                };

                if !scan.filters.is_empty() {
                    let predicates = scan
                        .filters
                        .iter()
                        .cloned()
                        .map(normalize_expr)
                        .collect::<Result<Vec<_>, _>>()?;
                    let predicate = combine_filters(predicates).ok_or_else(|| {
                        PlannerError::UnsupportedPlan(
                            "table scan filter list unexpectedly empty".to_string(),
                        )
                    })?;
                    let select = DbspSelectNode::try_new(source_schema.clone(), predicate)?;
                    let id = self.add_node(
                        vec![current.id],
                        DbspNodeKind::Select(select),
                        current.schema.clone(),
                    );
                    current.id = id;
                }

                if let Some(projection) = &scan.projection
                    && !projection.is_empty()
                {
                    let mut items = Vec::with_capacity(projection.len());
                    for idx in projection {
                        let Some(field) = source_schema.field(*idx) else {
                            return Err(PlannerError::UnsupportedPlan(format!(
                                "table scan projection index {idx} out of bounds",
                            )));
                        };
                        items.push(ProjectItem {
                            expr: Expr::Column(Column::new_unqualified(field.name.clone())),
                            alias: Some(field.name.clone()),
                        });
                    }
                    let project = DbspProjectNode::try_new(current.schema.clone(), items)?;
                    let output_schema = project.output_schema().clone();
                    let id = self.add_node(
                        vec![current.id],
                        DbspNodeKind::Project(project),
                        output_schema.clone(),
                    );
                    current = PlannedNode {
                        id,
                        schema: output_schema,
                    };
                }

                Ok(current)
            }
            LogicalPlan::Projection(projection) => {
                let input = self.plan_node(&projection.input)?;
                self.build_projection_node(input, projection)
            }
            LogicalPlan::Filter(filter) => {
                if let Some(topn) = self.plan_row_number_filter(filter)? {
                    return Ok(topn);
                }
                let normalized_predicate = normalize_expr(filter.predicate.clone())?;
                if let LogicalPlan::Projection(projection) = filter.input.as_ref() {
                    let base = self.plan_node(&projection.input)?;
                    let predicate_references_only_base_columns = normalized_predicate
                        .column_refs()
                        .iter()
                        .all(|column| base.schema.field_index(column.name.as_str()).is_some());
                    if predicate_references_only_base_columns {
                        let select =
                            DbspSelectNode::try_new(base.schema.clone(), normalized_predicate)?;
                        let filter_id = self.add_node(
                            vec![base.id],
                            DbspNodeKind::Select(select),
                            base.schema.clone(),
                        );
                        let filtered = PlannedNode {
                            id: filter_id,
                            schema: base.schema.clone(),
                        };
                        return self.build_projection_node(filtered, projection);
                    }
                }

                let input = self.plan_node(&filter.input)?;
                if let Some(optimized) = self.optimize_join_subtree(
                    input.clone(),
                    Some(normalized_predicate.clone()),
                    None,
                )? {
                    return Ok(optimized);
                }
                let mut predicate_schema = input.schema.clone();
                if matches!(filter.input.as_ref(), LogicalPlan::Projection(_))
                    && let Some(node) = self.node_by_id(input.id)
                    && let DbspNodeKind::Project(project) = &node.kind
                    && normalized_predicate.column_refs().iter().all(|column| {
                        project
                            .input_schema()
                            .field_index(column.name.as_str())
                            .is_some()
                    })
                {
                    predicate_schema = Arc::clone(project.input_schema());
                }
                let select = DbspSelectNode::try_new(predicate_schema, normalized_predicate)?;
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
            LogicalPlan::Join(join) => self.plan_join(join),
            LogicalPlan::Aggregate(aggregate) => self.plan_aggregate(aggregate),
            LogicalPlan::Sort(sort) => {
                let input = self.plan_node(&sort.input)?;
                if let Some(fetch) = sort.fetch {
                    let order_by = self.map_sort_expressions(&sort.expr, input.schema.clone())?;
                    let topn = DbspTopNNode::try_new(
                        input.schema.clone(),
                        Vec::new(),
                        order_by,
                        fetch,
                        0,
                    )?;
                    let output_schema = topn.output_schema().clone();
                    let id = self.add_node(
                        vec![input.id],
                        DbspNodeKind::TopN(topn),
                        output_schema.clone(),
                    );
                    return Ok(PlannedNode {
                        id,
                        schema: output_schema,
                    });
                }
                let id = self.add_node(
                    vec![input.id],
                    DbspNodeKind::Passthrough,
                    input.schema.clone(),
                );
                Ok(PlannedNode {
                    id,
                    schema: input.schema,
                })
            }
            LogicalPlan::Limit(limit) => self.plan_limit(limit),
            LogicalPlan::Union(union) => self.plan_union(union),
            LogicalPlan::Window(_) => Err(PlannerError::UnsupportedPlan(
                "window logical plans must be translated via aggregate nodes".to_string(),
            )),
            LogicalPlan::SubqueryAlias(alias) => self.plan_node(&alias.input),
            LogicalPlan::Subquery(subquery) => self.plan_node(&subquery.subquery),
            LogicalPlan::Repartition(repartition) => self.plan_node(&repartition.input),
            LogicalPlan::Distinct(distinct) => self.plan_distinct(distinct),
            LogicalPlan::EmptyRelation(relation) => {
                let output_schema = row_schema_from_dfschema(relation.schema.as_ref())?;
                if !relation.produce_one_row {
                    return Ok(self.build_empty_node(output_schema));
                }
                let one_row = DbspOneRowNode::new(output_schema.clone());
                let id =
                    self.add_node(vec![], DbspNodeKind::OneRow(one_row), output_schema.clone());
                Ok(PlannedNode {
                    id,
                    schema: output_schema,
                })
            }
            LogicalPlan::Values(values) => {
                let output_schema = row_schema_from_dfschema(values.schema.as_ref())?;
                let values_node =
                    DbspValuesNode::try_new(output_schema.clone(), values.values.clone())?;
                let id = self.add_node(
                    vec![],
                    DbspNodeKind::Values(values_node),
                    output_schema.clone(),
                );
                Ok(PlannedNode {
                    id,
                    schema: output_schema,
                })
            }
            LogicalPlan::Explain(_) => Err(PlannerError::UnsupportedPlan(
                "EXPLAIN plans are not supported".to_string(),
            )),
            LogicalPlan::Analyze(_) => Err(PlannerError::UnsupportedPlan(
                "ANALYZE plans are not supported".to_string(),
            )),
            LogicalPlan::Statement(_) => Err(PlannerError::UnsupportedPlan(
                "statement plans are not supported".to_string(),
            )),
            LogicalPlan::Extension(extension) => {
                if let Some(asof) = extension.node.as_any().downcast_ref::<FloeAsofJoinNode>() {
                    return self.plan_asof_extension(asof);
                }
                Err(PlannerError::UnsupportedPlan(format!(
                    "logical extension node {} is not supported",
                    extension.node.name()
                )))
            }
            LogicalPlan::Dml(_) | LogicalPlan::Ddl(_) | LogicalPlan::DescribeTable(_) => Err(
                PlannerError::UnsupportedPlan("plan type is not supported".to_string()),
            ),
            LogicalPlan::Copy(_) | LogicalPlan::RecursiveQuery(_) | LogicalPlan::Unnest(_) => Err(
                PlannerError::UnsupportedPlan("plan type is not supported".to_string()),
            ),
        }
    }
}

pub(super) fn row_schema_from_dfschema(schema: &DFSchema) -> Result<Arc<RowSchema>, PlannerError> {
    let fields = schema
        .iter()
        .map(|(_, field)| {
            Ok(Field::new(
                field.name().to_string(),
                DbspScalarType::try_from_arrow(field.data_type())?,
                field.is_nullable(),
            ))
        })
        .collect::<Result<Vec<_>, anyhow::Error>>()?;
    RowSchema::try_new(fields).map_err(PlannerError::from)
}
