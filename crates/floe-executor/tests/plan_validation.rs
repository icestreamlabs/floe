use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::Result;
use arrow_schema::SchemaRef;
use datafusion::common::Column;
use datafusion::logical_expr::{JoinType, LogicalPlanBuilder, col, lit, table_scan};

use floe_executor::dbsp_plan::{
    CircuitNode, CircuitPlan, DbspAggregateFunction, DbspAggregateNode, DbspNodeKind,
    DbspPlanBuilder, DbspScalarType, DbspSourceNode, DbspWindowAggregateNode, DbspWindowPolicy,
    DbspWindowSpec, Field, RowSchema, TableDescriptor, nexmark_auction_table, nexmark_bid_table,
    nexmark_config, nexmark_person_table, validate_dbsp_plan,
};

#[test]
fn validates_simple_filter_plan() -> Result<()> {
    let planner = planner();
    let plan = bidder_filter_plan(&planner)?;
    let mut sources = BTreeSet::new();
    sources.insert(nexmark_bid_table().name().to_string());

    let validated = validate_dbsp_plan(&plan, &sources, "mv_bidder")?;

    assert_eq!(validated.required_sources, sources);
    assert_eq!(validated.root_node, plan.root);
    assert!(!validated.root_is_sink);
    assert!(validated.fan_in_nodes.is_empty());
    Ok(())
}

#[test]
fn errors_when_source_missing() -> Result<()> {
    let planner = planner();
    let plan = bidder_filter_plan(&planner)?;
    let sources = BTreeSet::new();

    let err = validate_dbsp_plan(&plan, &sources, "mv_bidder").unwrap_err();
    let message = err.to_string();
    assert!(message.contains("source 'nexmark_bid' not provided"));
    assert!(message.contains("available sources: {}"));
    Ok(())
}

#[test]
fn validates_join_plan_with_available_sources() -> Result<()> {
    let planner = planner();
    let plan = join_plan(&planner)?;
    let mut sources = BTreeSet::new();
    sources.insert(nexmark_person_table().name().to_string());
    sources.insert(nexmark_auction_table().name().to_string());

    let validated = validate_dbsp_plan(&plan, &sources, "mv_join")?;
    assert_eq!(validated.required_sources.len(), 2);
    assert!(
        validated
            .required_sources
            .contains(nexmark_person_table().name())
    );
    assert!(
        validated
            .required_sources
            .contains(nexmark_auction_table().name())
    );
    assert!(!validated.root_is_sink);
    assert!(validated.fan_in_nodes.iter().any(|id| {
        plan.node(*id)
            .map(|n| matches!(n.kind, DbspNodeKind::Join(_)))
            .unwrap_or(false)
    }));
    Ok(())
}

#[test]
fn accepts_topn_operator() -> Result<()> {
    let planner = planner();
    let plan = topn_plan(&planner)?;
    let mut sources = BTreeSet::new();
    sources.insert(nexmark_bid_table().name().to_string());

    let validated = validate_dbsp_plan(&plan, &sources, "mv_topn")?;
    assert_eq!(validated.root_node, plan.root);
    Ok(())
}

#[test]
fn accepts_union_operator() -> Result<()> {
    let planner = planner();
    let plan = union_plan(&planner)?;
    let mut sources = BTreeSet::new();
    sources.insert(nexmark_bid_table().name().to_string());

    let validated = validate_dbsp_plan(&plan, &sources, "mv_union")?;
    assert_eq!(validated.root_node, plan.root);
    assert!(validated.fan_in_nodes.iter().any(|id| {
        plan.node(*id)
            .map(|node| matches!(node.kind, DbspNodeKind::Union(_)))
            .unwrap_or(false)
    }));
    Ok(())
}

#[test]
fn accepts_passthrough_operator() -> Result<()> {
    let planner = planner();
    let plan = passthrough_plan(&planner)?;
    let mut sources = BTreeSet::new();
    sources.insert(nexmark_bid_table().name().to_string());

    let validated = validate_dbsp_plan(&plan, &sources, "mv_passthrough")?;
    assert_eq!(validated.root_node, plan.root);
    Ok(())
}

#[test]
fn accepts_window_aggregate_operator() -> Result<()> {
    let plan = window_aggregate_plan()?;
    let mut sources = BTreeSet::new();
    sources.insert(nexmark_bid_table().name().to_string());

    let validated = validate_dbsp_plan(&plan, &sources, "mv_window")?;
    assert_eq!(validated.root_node, plan.root);
    Ok(())
}

#[test]
fn detects_join_fan_in_mismatch() -> Result<()> {
    let planner = planner();
    let mut plan = join_plan(&planner)?;
    let mut sources = BTreeSet::new();
    sources.insert(nexmark_person_table().name().to_string());
    sources.insert(nexmark_auction_table().name().to_string());

    let join_id = plan
        .nodes
        .iter()
        .find(|node| matches!(node.kind, DbspNodeKind::Join(_)))
        .map(|node| node.id)
        .expect("join node");
    let join_node = plan
        .nodes
        .iter_mut()
        .find(|node| node.id == join_id)
        .expect("join node present");
    let first_input = *join_node.inputs.first().expect("join input");
    join_node.inputs = vec![first_input];

    plan.root = join_id;

    fn collect_reachable(plan: &CircuitPlan, id: usize, seen: &mut BTreeSet<usize>) {
        if !seen.insert(id) {
            return;
        }
        if let Some(node) = plan.node(id) {
            for input in &node.inputs {
                collect_reachable(plan, *input, seen);
            }
        }
    }

    let mut reachable = BTreeSet::new();
    collect_reachable(&plan, join_id, &mut reachable);
    plan.nodes.retain(|node| reachable.contains(&node.id));

    let err = validate_dbsp_plan(&plan, &sources, "mv_join").unwrap_err();
    let expected = format!("node {join_id} → Join expects ≥2 inputs (found 1)");
    assert!(err.to_string().contains(&expected));
    Ok(())
}

#[test]
fn rejects_bad_namespace() -> Result<()> {
    let planner = planner();
    let plan = bidder_filter_plan(&planner)?;
    let mut sources = BTreeSet::new();
    sources.insert(nexmark_bid_table().name().to_string());

    let err = validate_dbsp_plan(&plan, &sources, "mv/bad").unwrap_err();
    assert!(
        err.to_string()
            .contains("materialized view name cannot contain '/'")
    );
    Ok(())
}

#[test]
fn detects_cycles_in_plan() -> Result<()> {
    let planner = planner();
    let mut plan = bidder_filter_plan(&planner)?;

    let root_id = plan.root;
    let root_node = plan
        .nodes
        .iter_mut()
        .find(|node| node.id == root_id)
        .expect("root node");
    root_node.inputs.push(root_id);

    let mut sources = BTreeSet::new();
    sources.insert(nexmark_bid_table().name().to_string());

    let err = validate_dbsp_plan(&plan, &sources, "mv_cycle").unwrap_err();
    assert!(err.to_string().contains("cycle involving node"));
    Ok(())
}

fn bidder_filter_plan(planner: &DbspPlanBuilder) -> Result<CircuitPlan> {
    let bid = nexmark_bid_table();
    let predicate = col("price").gt(lit(10i64));
    let logical_plan = table_scan(Some(bid.name()), &schema_for(bid), None)?
        .filter(predicate)?
        .project(vec![col("bidder")])?
        .build()?;
    Ok(planner.build(&logical_plan)?)
}

fn join_plan(planner: &DbspPlanBuilder) -> Result<CircuitPlan> {
    let auction = nexmark_auction_table();
    let person = nexmark_person_table();

    let right = table_scan(Some(person.name()), &schema_for(person), None)?.build()?;
    let logical_plan = table_scan(Some(auction.name()), &schema_for(auction), None)?
        .join(
            right,
            JoinType::Inner,
            (
                vec![Column::from_name("seller")],
                vec![Column::from_name("id")],
            ),
            None,
        )?
        .build()?;
    Ok(planner.build(&logical_plan)?)
}

fn topn_plan(planner: &DbspPlanBuilder) -> Result<CircuitPlan> {
    let bid = nexmark_bid_table();
    let logical_plan = table_scan(Some(bid.name()), &schema_for(bid), None)?
        .sort(vec![col("price").sort(true, true)])?
        .limit(0, Some(5))?
        .build()?;
    Ok(planner.build(&logical_plan)?)
}

fn union_plan(planner: &DbspPlanBuilder) -> Result<CircuitPlan> {
    let bid = nexmark_bid_table();
    let left = table_scan(Some(bid.name()), &schema_for(bid), None)?.build()?;
    let right = table_scan(Some(bid.name()), &schema_for(bid), None)?.build()?;
    let logical_plan = LogicalPlanBuilder::from(left).union(right)?.build()?;
    Ok(planner.build(&logical_plan)?)
}

fn passthrough_plan(planner: &DbspPlanBuilder) -> Result<CircuitPlan> {
    let bid = nexmark_bid_table();
    let logical_plan = table_scan(Some(bid.name()), &schema_for(bid), None)?
        .sort(vec![col("price").sort(true, true)])?
        .build()?;
    Ok(planner.build(&logical_plan)?)
}

fn window_aggregate_plan() -> Result<CircuitPlan> {
    let bid = nexmark_bid_table();
    let input_schema = bid.schema().clone();
    let aggregate = DbspAggregateNode::try_new(
        input_schema.clone(),
        vec![(col("auction"), None)],
        vec![(
            DbspAggregateFunction::Count,
            None,
            None,
            false,
            Some("bid_count".to_string()),
        )],
    )?;

    let window = DbspWindowSpec::try_new(
        DbspWindowPolicy::Tumbling { size_ms: 1_000 },
        col("date_time"),
        input_schema.clone(),
        0,
    )?;

    let mut fields = Vec::new();
    fields.push(Field::new(
        "window_start",
        DbspScalarType::TimestampMillis,
        false,
    ));
    fields.push(Field::new(
        "window_end",
        DbspScalarType::TimestampMillis,
        false,
    ));
    fields.extend(aggregate.output_schema().fields().iter().cloned());
    let output_schema = RowSchema::try_new(fields)?;

    let source = CircuitNode {
        id: 0,
        kind: DbspNodeKind::Source(DbspSourceNode {
            table: Arc::new(bid.clone()),
        }),
        inputs: vec![],
        output_schema: input_schema,
    };
    let window_node = CircuitNode {
        id: 1,
        kind: DbspNodeKind::WindowAggregate(DbspWindowAggregateNode { aggregate, window }),
        inputs: vec![0],
        output_schema,
    };

    Ok(CircuitPlan {
        root: 1,
        nodes: vec![source, window_node],
    })
}

fn planner() -> DbspPlanBuilder {
    DbspPlanBuilder::new(nexmark_config())
}

fn schema_for(table: &'static TableDescriptor) -> SchemaRef {
    table.schema().to_arrow_schema()
}
