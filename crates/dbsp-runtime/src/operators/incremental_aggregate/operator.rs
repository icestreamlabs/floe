use super::*;

#[async_trait]
impl<K, V> DeltaOperator for IncrementalAggregateOp<K, V>
where
    K: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    V: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    V::Archived: RkyvDeserialize<V, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    async fn on_step(
        &mut self,
        _ts: i64,
        inputs: &[ZSetHandle],
    ) -> anyhow::Result<Option<ZSetHandle>> {
        let step_start = Instant::now();
        let delta_handle = inputs
            .first()
            .cloned()
            .context("incremental aggregate operator requires one input delta handle")?;

        let load_delta_start = Instant::now();
        let delta_values =
            delta_zset_handle_batch::<V>(self.table.clone(), &mut self.dict_cache, &delta_handle)
                .await
                .context("load delta for incremental aggregate")?;
        metrics::observe_operator_phase_latency_ms(
            "incremental_aggregate",
            "step",
            "load_delta",
            load_delta_start.elapsed().as_millis() as u64,
        );
        let mut work = metrics::LogicalWorkSnapshot::from_input_delta_rows(delta_values.len());

        let apply_values_start = Instant::now();
        let output_deltas = self
            .apply_delta_values_with_work(delta_values.as_ref(), Some(&mut work))
            .await?;
        metrics::observe_operator_phase_latency_ms(
            "incremental_aggregate",
            "step",
            "apply_delta_values",
            apply_values_start.elapsed().as_millis() as u64,
        );
        if output_deltas.is_empty() {
            metrics::observe_operator_phase_latency_ms(
                "incremental_aggregate",
                "step",
                "on_step_total",
                step_start.elapsed().as_millis() as u64,
            );
            self.logical_work.finish_tick(work);
            return Ok(Some(self.output.handle_for_version(0)));
        }
        work.record_output_delta_rows(output_deltas.len());

        let persist_output_start = Instant::now();
        let delta_handle =
            Self::apply_deltas_to_versioned(&mut self.output, &output_deltas, None, "output")
                .await
                .context("persist incremental aggregate output delta")?;
        work.record_persisted_rows(output_deltas.len());
        metrics::observe_operator_phase_latency_ms(
            "incremental_aggregate",
            "step",
            "persist_output",
            persist_output_start.elapsed().as_millis() as u64,
        );
        publish_transient_zset_batch(
            &delta_handle,
            Arc::new(output_deltas.into_iter().collect::<Vec<_>>()),
        );
        metrics::observe_operator_phase_latency_ms(
            "incremental_aggregate",
            "step",
            "on_step_total",
            step_start.elapsed().as_millis() as u64,
        );
        self.logical_work.finish_tick(work);
        Ok(Some(delta_handle))
    }

    fn logical_work(&self) -> Option<metrics::LogicalWorkSnapshot> {
        Some(self.logical_work.last_tick())
    }
}
