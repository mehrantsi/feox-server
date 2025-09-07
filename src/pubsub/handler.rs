use super::{BroadcastMsg, GlobalRegistry, PubSubMessage, ThreadLocalPubSub};
use crate::network::{Connection, PubSubOp};
use crate::protocol::resp::{write_resp_value, RespValue};
use std::sync::Arc;

/// Handle pub/sub operations and return local deliveries
pub fn handle_pubsub_operation(
    pubsub_manager: &mut ThreadLocalPubSub,
    pubsub_registry: &Arc<GlobalRegistry>,
    conn_id: usize,
    op: PubSubOp,
    connection: &mut Connection,
    thread_id: usize,
) -> Vec<(usize, PubSubMessage)> {
    let mut local_deliveries = Vec::new();

    match op {
        PubSubOp::Subscribe(channels) => {
            let messages = pubsub_manager.subscribe(conn_id, channels);
            for message in messages {
                if let PubSubMessage::Subscribe { count, .. } = &message {
                    connection.set_subscription_count(*count);
                }
                connection.queue_pubsub_message(message);
            }
        }
        PubSubOp::Unsubscribe(channels) => {
            let messages = pubsub_manager.unsubscribe(conn_id, channels);
            for message in messages {
                if let PubSubMessage::Unsubscribe { count, .. } = &message {
                    connection.set_subscription_count(*count);
                }
                connection.queue_pubsub_message(message);
            }
        }
        PubSubOp::PSubscribe(patterns) => {
            let messages = pubsub_manager.psubscribe(conn_id, patterns);
            for message in messages {
                if let PubSubMessage::PSubscribe { count, .. } = &message {
                    connection.set_subscription_count(*count);
                }
                connection.queue_pubsub_message(message);
            }
        }
        PubSubOp::PUnsubscribe(patterns) => {
            let messages = pubsub_manager.punsubscribe(conn_id, patterns);
            for message in messages {
                if let PubSubMessage::PUnsubscribe { count, .. } = &message {
                    connection.set_subscription_count(*count);
                }
                connection.queue_pubsub_message(message);
            }
        }
        PubSubOp::Publish { channel, message } => {
            // First, publish locally and collect deliveries
            local_deliveries =
                pubsub_manager.publish_local(&channel, &bytes::Bytes::from(message.clone()));

            // Get all threads interested in this channel
            let channel_threads = pubsub_registry.get_channel_threads(&channel);
            let pattern_threads = pubsub_registry.get_all_pattern_threads();

            // Broadcast to channel subscribers (excluding our thread)
            let msg = BroadcastMsg::Publish {
                channel: channel.clone(),
                message: bytes::Bytes::from(message.clone()),
                exclude_thread: Some(thread_id),
            };
            pubsub_registry.broadcast_to_threads(msg, &channel_threads);

            // Broadcast to pattern subscribers (excluding our thread)
            let pattern_msg = BroadcastMsg::PatternPublish {
                channel: channel.clone(),
                message: bytes::Bytes::from(message),
                exclude_thread: Some(thread_id),
            };
            let pattern_thread_vec: Vec<_> = pattern_threads.into_iter().collect();
            pubsub_registry.broadcast_to_threads(pattern_msg, &pattern_thread_vec);

            // Get the total count from global registry
            let total_count = pubsub_registry.get_channel_subscriber_count(&channel)
                + pubsub_registry.get_total_pattern_matches(&channel);
            let resp = RespValue::Integer(total_count as i64);
            write_resp_value(&mut connection.write_buffer, &resp);
        }
        PubSubOp::PubSubChannels { pattern } => {
            // Get all channels from the registry
            let all_channels = pubsub_registry.get_all_channels();
            let filtered = if let Some(pat) = pattern {
                all_channels
                    .into_iter()
                    .filter(|ch| GlobalRegistry::glob_match(&pat, ch))
                    .map(|ch| RespValue::BulkString(Some(ch.into())))
                    .collect()
            } else {
                all_channels
                    .into_iter()
                    .map(|ch| RespValue::BulkString(Some(ch.into())))
                    .collect()
            };
            let resp = RespValue::Array(Some(filtered));
            write_resp_value(&mut connection.write_buffer, &resp);
        }
        PubSubOp::PubSubNumSub { channels } => {
            let mut results = Vec::new();
            for channel in channels {
                results.push(RespValue::BulkString(Some(channel.clone().into())));
                let count = pubsub_registry.get_channel_subscriber_count(&channel);
                results.push(RespValue::Integer(count as i64));
            }
            let resp = RespValue::Array(Some(results));
            write_resp_value(&mut connection.write_buffer, &resp);
        }
        PubSubOp::PubSubNumPat => {
            let count = pubsub_registry.get_pattern_count();
            let resp = RespValue::Integer(count as i64);
            write_resp_value(&mut connection.write_buffer, &resp);
        }
    }

    local_deliveries
}
