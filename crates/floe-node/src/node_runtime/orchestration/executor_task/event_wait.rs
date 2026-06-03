use super::*;

pub(super) async fn wait_for_ready_events(
    cancel: &CancellationToken,
    connector_receiver: &mut core_source::RoutedAppendIngestEventReceiver,
    connector_queues: &mut [ConnectorQueue],
    cdc_enabled: bool,
    cdc_transaction_receiver: &mut mpsc::Receiver<QueuedCdcTransaction>,
    cdc_transaction_queue: &mut VecDeque<QueuedCdcTransaction>,
) -> bool {
    if connector_queues
        .iter()
        .all(|queue| queue.pending.is_empty())
        && cdc_transaction_queue.is_empty()
    {
        let has_events = wait_for_next_ready_event(
            cancel,
            connector_receiver,
            connector_queues,
            cdc_enabled,
            cdc_transaction_receiver,
            cdc_transaction_queue,
        )
        .await;
        if !has_events {
            return false;
        }
    }
    drain_ready(connector_receiver, connector_queues);
    if cdc_enabled {
        drain_cdc_ready(cdc_transaction_receiver, cdc_transaction_queue);
    }
    true
}

async fn wait_for_next_ready_event(
    cancel: &CancellationToken,
    connector_receiver: &mut core_source::RoutedAppendIngestEventReceiver,
    connector_queues: &mut [ConnectorQueue],
    cdc_enabled: bool,
    cdc_transaction_receiver: &mut mpsc::Receiver<QueuedCdcTransaction>,
    cdc_transaction_queue: &mut VecDeque<QueuedCdcTransaction>,
) -> bool {
    loop {
        let connector_receiver_active = !connector_receiver.is_closed();
        let cdc_receiver_active = cdc_enabled && !cdc_transaction_receiver.is_closed();
        match (connector_receiver_active, cdc_receiver_active) {
            (false, false) => return false,
            (true, false) => {
                return tokio::select! {
                    _ = cancel.cancelled() => false,
                    has_events = recv_from_ready(connector_receiver, connector_queues) => has_events,
                };
            }
            (false, true) => {
                return tokio::select! {
                    _ = cancel.cancelled() => false,
                    has_events = recv_cdc_from_ready(
                        cdc_transaction_receiver,
                        cdc_transaction_queue,
                    ) => has_events,
                };
            }
            (true, true) => {
                let has_events = tokio::select! {
                    _ = cancel.cancelled() => false,
                    has_events = recv_cdc_from_ready(
                        cdc_transaction_receiver,
                        cdc_transaction_queue,
                    ) => has_events,
                    has_events = recv_from_ready(connector_receiver, connector_queues) => has_events,
                };
                if has_events {
                    return true;
                }
            }
        }
    }
}
