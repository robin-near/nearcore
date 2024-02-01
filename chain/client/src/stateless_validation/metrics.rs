use near_o11y::metrics::{IntCounter, IntCounterVec};
use once_cell::sync::Lazy;

pub static NUM_CHUNK_ENDORSEMENT_MESSAGES_SENT: Lazy<IntCounter> = Lazy::new(|| {
    near_o11y::metrics::try_create_int_counter(
        "near_num_chunk_endorsement_messages_sent",
        concat!("Number of times we sent a chunk endorsement message to anyone"),
    )
    .unwrap()
});

pub static NUM_CHUNK_ENDORSEMENT_MESSAGES_RECEIVED: Lazy<IntCounter> = Lazy::new(|| {
    near_o11y::metrics::try_create_int_counter(
        "near_num_chunk_endorsement_messages_received",
        concat!("Number of times we received a chunk endorsement message from anyone"),
    )
    .unwrap()
});

pub static NUM_CHUNK_ENDORSEMENT_MESSAGES_RECEIVED_BUT_DROPPED: Lazy<IntCounterVec> =
    Lazy::new(|| {
        near_o11y::metrics::try_create_int_counter_vec(
            "near_num_chunk_endorsement_messages_received_but_dropped",
            concat!(
            "Number of times we received a chunk endorsement message from anyone but dropped it"
        ),
            &["reason"],
        )
        .unwrap()
    });
