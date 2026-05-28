use super::*;

pub(super) struct TransientSegmentOptimization {
    pub(super) durable_input_idx: usize,
    pub(super) optimized_nodes: Vec<usize>,
    pub(super) score: i32,
    pub(super) steps: Vec<TransientSegmentStep>,
    pub(super) transform: Arc<DeltaTransformFn>,
}

pub(super) fn try_build_transient_segment_optimization(
    plan: &CircuitPlan,
    terminal_input_idx: usize,
    built: &HashMap<usize, DeltaHandleStream>,
    graph_id: &str,
    allow_terminal_without_consumer: bool,
    persistence_policy: &PersistencePolicy,
) -> Result<Option<TransientSegmentOptimization>> {
    let Some(segment) = persistence_policy.build_transient_segment(
        plan,
        terminal_input_idx,
        built,
        allow_terminal_without_consumer,
    )?
    else {
        return Ok(None);
    };
    build_transient_segment_optimization_from_spec(graph_id, segment).map(Some)
}

fn build_transient_segment_optimization_from_spec(
    graph_id: &str,
    segment: TransientSegmentSpec,
) -> Result<TransientSegmentOptimization> {
    let steps = segment.steps.clone();
    let mut evaluators = Vec::new();
    for step in &steps {
        match step {
            TransientSegmentStep::Passthrough => {}
            TransientSegmentStep::Select { predicate, schema } => {
                evaluators.push(VectorizedFilterProjectEvaluator::for_filter(
                    predicate,
                    Arc::clone(schema),
                )?);
            }
            TransientSegmentStep::Project {
                expressions,
                schema,
            } => {
                evaluators.push(VectorizedFilterProjectEvaluator::for_map(
                    expressions.as_ref(),
                    Arc::clone(schema),
                )?);
            }
        }
    }

    let evaluators = Arc::new(evaluators);
    let graph_id = graph_id.to_string();
    let transform: Arc<DeltaTransformFn> = Arc::new(move |deltas| {
        let graph_id = graph_id.clone();
        let evaluators = Arc::clone(&evaluators);
        Box::pin(async move {
            apply_transient_segment_vectorized(&graph_id, evaluators.as_ref(), deltas).await
        })
    });

    Ok(TransientSegmentOptimization {
        durable_input_idx: segment.durable_input_idx,
        optimized_nodes: segment.segment_nodes,
        score: segment.score,
        steps,
        transform,
    })
}

async fn apply_transient_segment_vectorized(
    graph_id: &str,
    evaluators: &[VectorizedFilterProjectEvaluator],
    deltas: Arc<Vec<(Vec<u8>, i64)>>,
) -> Result<Vec<(Vec<u8>, i64)>> {
    if evaluators.is_empty() {
        return Ok(deltas.as_ref().clone());
    }
    let mut deltas = evaluators[0]
        .transform_delta_arrow(graph_id, Arc::clone(&deltas))
        .await?;
    for evaluator in &evaluators[1..] {
        if deltas.is_empty() {
            break;
        }
        deltas = evaluator
            .transform_delta_arrow(graph_id, Arc::new(deltas))
            .await?;
    }
    Ok(deltas)
}
