use super::*;

impl<K, V> IncrementalAggregateOp<K, V>
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
    pub(super) fn extrema_index_key(
        &self,
        group_key: &K,
        slot_idx: usize,
        aggregate_value: &AggregateValue,
        descending: bool,
    ) -> Result<Option<OrderedBytes>> {
        let Some(value_bytes) = aggregate_value_order_bytes(aggregate_value, descending) else {
            return Ok(None);
        };
        let mut key = self.extrema_index_prefix(group_key, slot_idx)?;
        key.extend_from_slice(&value_bytes);
        Ok(Some(OrderedBytes::new(key)))
    }

    fn extrema_index_prefix(&self, group_key: &K, slot_idx: usize) -> Result<Vec<u8>> {
        let slot_idx = u32::try_from(slot_idx)
            .context("incremental aggregate extrema slot index exceeds u32")?;
        let group_bytes = encoding::encode(group_key)
            .context("encode incremental aggregate extrema group key")?;
        let mut prefix = Vec::with_capacity(group_bytes.len() + 8);
        append_memcomparable_bytes(&group_bytes, &mut prefix);
        prefix.extend_from_slice(&slot_idx.to_be_bytes());
        Ok(prefix)
    }

    pub(super) async fn refresh_extrema_slots_from_index(
        &self,
        key: &K,
        state: &mut GroupedIncrementalAggregateState,
        mut logical_work: Option<&mut metrics::LogicalWorkSnapshot>,
    ) -> Result<()> {
        for (slot_idx, slot_kind) in self.slot_kinds.iter().enumerate() {
            let is_extrema = matches!(
                slot_kind,
                IncrementalAggregateSlotKind::Min(_) | IncrementalAggregateSlotKind::Max(_)
            );
            if !is_extrema {
                continue;
            }
            let current = self
                .lookup_extrema_slot_from_index(key, slot_idx, logical_work.as_deref_mut())
                .await?;
            match (&mut state.slots[slot_idx], slot_kind) {
                (
                    IncrementalAggregateSlotState::Min {
                        current: slot_current,
                    },
                    IncrementalAggregateSlotKind::Min(_),
                )
                | (
                    IncrementalAggregateSlotState::Max {
                        current: slot_current,
                    },
                    IncrementalAggregateSlotKind::Max(_),
                ) => {
                    *slot_current = current;
                }
                (state_slot, slot_kind) => {
                    tracing::warn!(
                        slot_idx,
                        ?state_slot,
                        ?slot_kind,
                        "incremental aggregate extrema index refresh saw mismatched slot"
                    );
                }
            }
        }
        Ok(())
    }

    async fn lookup_extrema_slot_from_index(
        &self,
        key: &K,
        slot_idx: usize,
        logical_work: Option<&mut metrics::LogicalWorkSnapshot>,
    ) -> Result<Option<AggregateValue>> {
        let extrema_index = self
            .extrema_index
            .as_ref()
            .context("incremental aggregate extrema index missing")?;
        let lower = self.extrema_index_prefix(key, slot_idx)?;
        let Some(upper) = bytes_prefix_successor(&lower) else {
            return Ok(None);
        };
        let (rows, lookup_metrics) = extrema_index
            .first_values_for_key_range_with_metrics(
                &OrderedBytes::new(lower),
                &OrderedBytes::new(upper),
            )
            .await
            .context("lookup incremental aggregate extrema slot")?;
        if let Some(work) = logical_work {
            work.add_lookup_metrics(lookup_metrics);
            work.extrema_rebuild_rows = work.extrema_rebuild_rows.saturating_add(rows.len() as u64);
        }
        if rows.is_empty() {
            return Ok(None);
        }

        let row_inputs = rows
            .into_iter()
            .filter_map(|(_, value, weight)| (weight > 0).then_some((value, weight)))
            .collect::<Vec<_>>();
        for (_value, row_update, weight) in (self.row_evaluator)(&row_inputs) {
            if weight <= 0 {
                continue;
            }
            if let Some(IncrementalAggregateSlotUpdate::Value(Some(value))) =
                row_update.slots.get(slot_idx)
            {
                return Ok(Some(value.clone()));
            }
        }
        Ok(None)
    }
}
