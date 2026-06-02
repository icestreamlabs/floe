use super::*;

impl<L, R, O, K> JoinOp<L, R, O, K>
where
    L: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    L::Archived: RkyvDeserialize<L, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    R: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    R::Archived: RkyvDeserialize<R, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    O: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    O::Archived: RkyvDeserialize<O, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
    K: Archive
        + Clone
        + Eq
        + Hash
        + Send
        + Sync
        + 'static
        + for<'a> RkyvSerialize<RkyvSerializer<'a>>,
    K::Archived: RkyvDeserialize<K, RkyvDeserializer> + for<'a> CheckBytes<RkyvValidator<'a>>,
{
    pub(super) async fn step_internal(
        &mut self,
        _ts: i64,
        inputs: &[ZSetHandle],
        transient_inputs: Option<JoinTransientInputs<L, R, K>>,
        persist_output: bool,
    ) -> anyhow::Result<Option<JoinStepResult<O>>> {
        let left_delta_handle = inputs
            .first()
            .cloned()
            .context("join operator requires left delta handle")?;
        let right_delta_handle = inputs
            .get(1)
            .cloned()
            .context("join operator requires right delta handle")?;

        let left_loaded;
        let left_delta_values: &[(L, i64)] = if let Some(batch) = transient_inputs
            .as_ref()
            .and_then(|inputs| inputs.left.as_ref())
        {
            batch.as_ref()
        } else {
            left_loaded = delta_zset_handle_batch::<L>(
                self.table.clone(),
                &mut self.dict_cache_left,
                &left_delta_handle,
            )
            .await
            .context("load left delta for join")?;
            left_loaded.as_ref().as_slice()
        };
        let right_loaded;
        let right_delta_values: &[(R, i64)] = if let Some(batch) = transient_inputs
            .as_ref()
            .and_then(|inputs| inputs.right.as_ref())
        {
            batch.as_ref()
        } else {
            right_loaded = delta_zset_handle_batch::<R>(
                self.table.clone(),
                &mut self.dict_cache_right,
                &right_delta_handle,
            )
            .await
            .context("load right delta for join")?;
            right_loaded.as_ref().as_slice()
        };
        let mut work = metrics::LogicalWorkSnapshot {
            input_delta_rows: left_delta_values
                .len()
                .saturating_add(right_delta_values.len()) as u64,
            input_delta_batches: (!left_delta_values.is_empty()) as u64
                + (!right_delta_values.is_empty()) as u64,
            left_delta_rows: left_delta_values.len() as u64,
            right_delta_rows: right_delta_values.len() as u64,
            ..metrics::LogicalWorkSnapshot::default()
        };
        let left_keyed = self.stage_keyed_deltas(left_delta_values, &self.left_key);
        let right_keyed = self.stage_keyed_deltas(right_delta_values, &self.right_key);
        work.left_changed_keys = left_keyed.len() as u64;
        work.right_changed_keys = right_keyed.len() as u64;
        let left_closed_key_updates = Self::coalesce_closed_key_updates(
            transient_inputs
                .as_ref()
                .and_then(|inputs| inputs.left_closed_keys.as_ref()),
        );
        let right_closed_key_updates = Self::coalesce_closed_key_updates(
            transient_inputs
                .as_ref()
                .and_then(|inputs| inputs.right_closed_keys.as_ref()),
        );

        if self.persist_indexes {
            work.add_assign(
                Self::seed_memory_index_for_keys(
                    &self.right_index,
                    &mut self.right_memory_index,
                    left_keyed.keys(),
                )
                .await
                .context("seed right join memory index")?,
            );
            Self::seed_memory_index_for_keys(
                &self.left_index,
                &mut self.left_memory_index,
                right_keyed.keys(),
            )
            .await
            .context("seed left join memory index")
            .map(|seed_work| work.add_assign(seed_work))?;
            Self::seed_memory_index_for_keys(
                &self.left_index,
                &mut self.left_memory_index,
                right_closed_key_updates.keys(),
            )
            .await
            .context("seed left join memory index for right closed keys")
            .map(|seed_work| work.add_assign(seed_work))?;
            Self::seed_memory_index_for_keys(
                &self.right_index,
                &mut self.right_memory_index,
                left_closed_key_updates.keys(),
            )
            .await
            .context("seed right join memory index for left closed keys")
            .map(|seed_work| work.add_assign(seed_work))?;
            Self::seed_closed_memory_index_for_keys(
                &self.right_closed_index,
                &mut self.right_closed_memory_index,
                left_keyed.keys(),
            )
            .await
            .context("seed right closed join key index")
            .map(|seed_work| work.add_assign(seed_work))?;
            Self::seed_closed_memory_index_for_keys(
                &self.left_closed_index,
                &mut self.left_closed_memory_index,
                right_keyed.keys(),
            )
            .await
            .context("seed left closed join key index")
            .map(|seed_work| work.add_assign(seed_work))?;
        }

        // Build output delta from pre-update state (A, B) and current deltas
        // (ΔA, ΔB). State/index updates happen after this block to keep
        // each tick atomic.
        let mut delta_join: FastHashMap<O, i64> = FastHashMap::new();
        let has_left = !left_keyed.is_empty();
        let has_right = !right_keyed.is_empty();

        // ΔA ⋈ B
        if has_left {
            for (key, left_entries) in &left_keyed {
                let join_metrics = self.join_entries_with_maps(
                    Some(left_entries),
                    self.right_memory_index.get(key),
                    &mut delta_join,
                );
                work.right_state_rows_examined = work
                    .right_state_rows_examined
                    .saturating_add(join_metrics.right_rows_examined);
                work.state_scan_rows = work
                    .state_scan_rows
                    .saturating_add(join_metrics.right_rows_examined);
                work.join_output_rows = work
                    .join_output_rows
                    .saturating_add(join_metrics.output_rows);
            }
        }

        // A ⋈ ΔB
        if has_right {
            for (key, right_entries) in &right_keyed {
                let join_metrics = self.join_entries_with_maps(
                    self.left_memory_index.get(key),
                    Some(right_entries),
                    &mut delta_join,
                );
                work.left_state_rows_examined = work
                    .left_state_rows_examined
                    .saturating_add(join_metrics.left_rows_examined);
                work.state_scan_rows = work
                    .state_scan_rows
                    .saturating_add(join_metrics.left_rows_examined);
                work.join_output_rows = work
                    .join_output_rows
                    .saturating_add(join_metrics.output_rows);
            }
        }

        // ΔA ⋈ ΔB
        if has_left && has_right {
            for (key, left_entries) in &left_keyed {
                if let Some(right_entries) = right_keyed.get(key) {
                    let join_metrics = self.join_entries_with_maps(
                        Some(left_entries),
                        Some(right_entries),
                        &mut delta_join,
                    );
                    work.delta_delta_rows_examined = work
                        .delta_delta_rows_examined
                        .saturating_add(join_metrics.candidate_pairs_examined);
                    work.join_output_rows = work
                        .join_output_rows
                        .saturating_add(join_metrics.output_rows);
                }
            }
        }
        delta_join.retain(|_, w| *w != 0);
        work.record_output_delta_rows(delta_join.len());

        let left_retained_updates =
            self.retained_left_updates(&left_keyed, &right_keyed, &right_closed_key_updates);
        let right_retained_updates =
            self.retained_right_updates(&left_keyed, &right_keyed, &left_closed_key_updates);

        Self::apply_keyed_updates_to_memory_index(
            &mut self.left_memory_index,
            &left_retained_updates,
        );
        Self::apply_keyed_updates_to_memory_index(
            &mut self.right_memory_index,
            &right_retained_updates,
        );
        Self::apply_closed_key_updates_to_memory_index(
            &mut self.left_closed_memory_index,
            &left_closed_key_updates,
        );
        Self::apply_closed_key_updates_to_memory_index(
            &mut self.right_closed_memory_index,
            &right_closed_key_updates,
        );

        if self.persist_indexes {
            let left_updates = Self::flatten_keyed_updates(&left_retained_updates);
            work.record_persisted_rows(left_updates.len());
            let left_index_persist_start = std::time::Instant::now();
            self.left_index
                .apply_deltas(left_updates)
                .await
                .context("update left join index")?;
            metrics::observe_operator_persistence_latency_ms(
                "join",
                "left_index",
                left_index_persist_start.elapsed().as_millis() as u64,
            );
        }

        if self.persist_indexes {
            let right_updates = Self::flatten_keyed_updates(&right_retained_updates);
            work.record_persisted_rows(right_updates.len());
            let right_index_persist_start = std::time::Instant::now();
            self.right_index
                .apply_deltas(right_updates)
                .await
                .context("update right join index")?;
            metrics::observe_operator_persistence_latency_ms(
                "join",
                "right_index",
                right_index_persist_start.elapsed().as_millis() as u64,
            );
        }

        if self.persist_indexes && !left_closed_key_updates.is_empty() {
            let left_closed_updates = left_closed_key_updates
                .iter()
                .map(|(key, weight)| (key.clone(), (), *weight));
            work.record_persisted_rows(left_closed_key_updates.len());
            let left_closed_persist_start = std::time::Instant::now();
            self.left_closed_index
                .apply_deltas(left_closed_updates)
                .await
                .context("update left closed join key index")?;
            metrics::observe_operator_persistence_latency_ms(
                "join",
                "left_closed_index",
                left_closed_persist_start.elapsed().as_millis() as u64,
            );
        }

        if self.persist_indexes && !right_closed_key_updates.is_empty() {
            let right_closed_updates = right_closed_key_updates
                .iter()
                .map(|(key, weight)| (key.clone(), (), *weight));
            work.record_persisted_rows(right_closed_key_updates.len());
            let right_closed_persist_start = std::time::Instant::now();
            self.right_closed_index
                .apply_deltas(right_closed_updates)
                .await
                .context("update right closed join key index")?;
            metrics::observe_operator_persistence_latency_ms(
                "join",
                "right_closed_index",
                right_closed_persist_start.elapsed().as_millis() as u64,
            );
        }

        if delta_join.is_empty() {
            self.logical_work.finish_tick(work);
            return if persist_output {
                let empty_handle = self
                    .output
                    .as_ref()
                    .context("join output persistence requested without configured output zset")?
                    .handle_for_version(0);
                Ok(Some(JoinStepResult {
                    delta_batch: Arc::new(Vec::new()),
                    persisted_handle: Some(empty_handle),
                }))
            } else {
                Ok(None)
            };
        }

        if let Some(integrated) = &mut self.integrated {
            let base = integrated
                .integrated
                .current_handle()
                .map(|handle| handle.version);
            work.record_persisted_rows(delta_join.len());
            let new_integrated_handle = Self::apply_deltas_to_versioned(
                &mut integrated.integrated,
                &delta_join,
                base,
                "integrated_output",
            )
            .await
            .context("update integrated join state")?;
            integrated.update_handle(new_integrated_handle);
        }

        let delta_batch = Arc::new(
            delta_join
                .iter()
                .map(|(row, weight)| (row.clone(), *weight))
                .collect(),
        );

        let persisted_handle = if persist_output {
            let output = self
                .output
                .as_mut()
                .context("join output persistence requested without configured output zset")?;
            work.record_persisted_rows(delta_join.len());
            let persisted_handle =
                Self::apply_deltas_to_versioned(output, &delta_join, None, "output")
                    .await
                    .context("persist join delta output")?;
            publish_transient_zset_batch(&persisted_handle, Arc::clone(&delta_batch));
            Some(persisted_handle)
        } else {
            None
        };

        self.logical_work.finish_tick(work);
        Ok(Some(JoinStepResult {
            delta_batch,
            persisted_handle,
        }))
    }
}
