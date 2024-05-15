use crate::{DBCol, Store};
use near_async::time::{Duration, Instant};
use std::fmt::Display;
use std::ops::Range;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};

pub type RangeId = usize;

#[derive(Clone)]
pub struct ParallelIterationOptions {
    num_threads: usize,
    min_time_before_split: Duration,
    max_entries_per_range: usize,
    print_progress: bool,
}

impl Default for ParallelIterationOptions {
    fn default() -> Self {
        Self {
            num_threads: num_cpus::get(),
            min_time_before_split: Duration::milliseconds(10),
            max_entries_per_range: 1000000,
            print_progress: true,
        }
    }
}

/// Provides parallel iteration and lookup over a Store. It is intended to be
/// used for heavy data processing. It is NOT intended for optimizing the
/// latency of small-sized iterations or lookups.
///
/// This iterator can either iterate over a range (which may be open-ended on
/// either side), or look up a large list of keys. In either case, it is
/// perfectly allowed for the data to be unevenly distributed (e.g. all keys
/// in the range start with a long common prefix, or some keys being looked up
/// are much larger than others). The implementation of the parallel iterator
/// performs automatic online load-balancing and work stealing, so that all
/// worker threads are kept busy as much as possible.
///
/// The iterator always returns raw bytes (equivalent to using either
/// `iter_range_raw_bytes` or `get_raw_bytes` on the store).
pub struct StoreParallelIterator {
    /// The threads that perform the iteration.
    threads: Vec<std::thread::JoinHandle<()>>,
    /// State that is shared across all iteration threads as well as the
    /// controlling thread.
    shared_state: Arc<ParallelIterationSharedState>,
}

struct ParallelIterationSharedState {
    /// Receiver of tasks.
    work_receiver: flume::Receiver<WorkQueueItem>,
    /// Number of threads that are not currently doing any work. This is used
    /// by threads that do have work to do to check if they should split their
    /// work.
    num_threads_free: AtomicUsize,
    /// A status indicator for each thread, showing which item they are working
    /// on (or if they are free). This is used for debug printing only.
    thread_work: Mutex<Vec<Option<WorkItem>>>,
    /// The range ID to assign to the next work item.
    next_range_id: AtomicUsize,
}

impl ParallelIterationSharedState {
    /// Returns a description of the current work that each worker thread is doing.
    fn describe(&self) -> String {
        let mut result = String::new();
        for (idx, item) in self.thread_work.lock().unwrap().iter().enumerate() {
            if let Some(item) = item {
                result.push_str(&format!("  [{}] {}\n", idx, item));
            } else {
                result.push_str(&format!("  [{}] free\n", idx));
            }
        }
        result
    }

    /// Returns whether a thread that has work should consider splitting the work.
    fn should_split(&self) -> bool {
        // Consider splitting if there are free threads and there is no work to be picked up.
        // The latter condition is more expensive as it involves a lock, but it's OK because
        // it's rare that the former be true but latter be false.
        self.num_threads_free.load(Ordering::Relaxed) > 0 && self.work_receiver.len() == 0
    }

    fn next_range_id(&self) -> RangeId {
        self.next_range_id.fetch_add(1, Ordering::Relaxed)
    }
}

/// The work queue consists of `WorkItem`s, but with each queue element we also
/// increment the refcount of the sender. That way, when all work items are
/// complete, the sender is also dropped and this will automatically cause all
/// receiving ends to error, and that's when the worker threads will exit.
struct WorkQueueItem {
    work: WorkItem,
    range_id: usize,
    work_sender: Arc<flume::Sender<WorkQueueItem>>,
}

impl StoreParallelIterator {
    /// Creates a new parallel iterator that performs the given task.
    fn new(
        options: ParallelIterationOptions,
        store: Store,
        column: DBCol,
        callback: Arc<dyn Fn(usize, &[u8], Option<&[u8]>) + Send + Sync>,
        initial_task: WorkItem,
    ) -> Self {
        let (tx, rx) = flume::unbounded::<WorkQueueItem>();
        let mut threads = Vec::new();
        let shared_state = Arc::new(ParallelIterationSharedState {
            work_receiver: rx,
            num_threads_free: AtomicUsize::new(options.num_threads),
            thread_work: Mutex::new(vec![None; options.num_threads]),
            next_range_id: AtomicUsize::new(0),
        });
        let tx = Arc::new(tx);
        tx.send(WorkQueueItem {
            work: initial_task,
            range_id: shared_state.next_range_id.fetch_add(1, Ordering::Relaxed),
            work_sender: tx.clone(),
        })
        .unwrap();
        drop(tx);
        for idx in 0..options.num_threads {
            let shared_state = shared_state.clone();
            let options = options.clone();
            let store = store.clone();
            let callback = callback.clone();
            let thread = std::thread::spawn(move || loop {
                let Ok(WorkQueueItem { work, range_id, work_sender }) =
                    shared_state.work_receiver.recv()
                else {
                    // If receiving fails, it means all senders are dropped,
                    // i.e. there is no more work to do.
                    break;
                };
                shared_state.num_threads_free.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                shared_state.thread_work.lock().unwrap()[idx] = Some(work.clone());

                // Spend some uninterrupted time to work on the task before checking whether we
                // should split. This is to avoid unnecessary thrashing when the remaining work
                // is small. Also, randomize this uninterrupted time, so that when there are free
                // threads, the other threads don't all try to split at the same time.
                let min_time_to_split = Instant::now()
                    + Duration::seconds_f64(
                        options.min_time_before_split.as_seconds_f64()
                            * (rand::random::<f64>() + 1.0),
                    );
                let mut current_range_entries: usize = 0;

                match work {
                    WorkItem::Range(range) => {
                        let start = if range.start.is_empty() {
                            None
                        } else {
                            Some(range.start.as_slice())
                        };
                        let end =
                            if range.end.is_empty() { None } else { Some(range.end.as_slice()) };
                        let iter = store.iter_range_raw_bytes(column, start, end);

                        for kv in iter {
                            let (key, value) = kv.unwrap();
                            callback(range_id, &key, Some(&value));
                            current_range_entries += 1;
                            if Instant::now() > min_time_to_split && shared_state.should_split() {
                                let next_key = IterationRange::next_key(&key);
                                if &next_key == &range.end {
                                    break;
                                }
                                let remaining_range =
                                    IterationRange::new(next_key, range.end.clone());
                                let work_items = WorkItem::Range(remaining_range).divide();
                                for work_item in work_items {
                                    work_sender
                                        .send(WorkQueueItem {
                                            work: work_item,
                                            range_id: shared_state.next_range_id(),
                                            work_sender: work_sender.clone(),
                                        })
                                        .unwrap();
                                }
                                break;
                            }
                            if current_range_entries >= options.max_entries_per_range {
                                let next_key = IterationRange::next_key(&key);
                                if &next_key == &range.end {
                                    break;
                                }
                                let remaining_range =
                                    IterationRange::new(next_key, range.end.clone());
                                work_sender
                                    .send(WorkQueueItem {
                                        work: WorkItem::Range(remaining_range),
                                        range_id: shared_state.next_range_id(),
                                        work_sender: work_sender.clone(),
                                    })
                                    .unwrap();
                                break;
                            }
                        }
                    }
                    WorkItem::Lookup(keys, range) => {
                        for i in range.start..range.end {
                            let key = &keys[i];
                            let value = store.get_raw_bytes(column, key).unwrap();
                            callback(range_id, key, value.as_ref().map(|slice| slice.as_slice()));
                            current_range_entries += 1;
                            if i + 1 < range.end {
                                if Instant::now() > min_time_to_split && shared_state.should_split()
                                {
                                    let work_items =
                                        WorkItem::Lookup(keys.clone(), i + 1..range.end).divide();
                                    for work_item in work_items {
                                        work_sender
                                            .send(WorkQueueItem {
                                                work: work_item,
                                                range_id: shared_state.next_range_id(),
                                                work_sender: work_sender.clone(),
                                            })
                                            .unwrap();
                                    }
                                    break;
                                }
                                if current_range_entries >= options.max_entries_per_range {
                                    work_sender
                                        .send(WorkQueueItem {
                                            work: WorkItem::Lookup(keys.clone(), i + 1..range.end),
                                            range_id: shared_state.next_range_id(),
                                            work_sender: work_sender.clone(),
                                        })
                                        .unwrap();
                                    break;
                                }
                            }
                        }
                    }
                };

                shared_state.num_threads_free.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                shared_state.thread_work.lock().unwrap()[idx] = None;
            });
            threads.push(thread);
        }
        Self { threads, shared_state }
    }

    /// Waits for all threads to complete.
    fn join(&mut self) {
        for thread in self.threads.drain(..) {
            thread.join().unwrap();
        }
    }

    fn wait_for_completion_and_print_progress(mut self) {
        let shared_state = self.shared_state.clone();
        let (done_tx, done_rx) = mpsc::channel::<()>();
        let progress_printer = std::thread::spawn(move || {
            while matches!(
                done_rx.recv_timeout(std::time::Duration::from_secs(3)),
                Err(RecvTimeoutError::Timeout)
            ) {
                let desc = shared_state.describe();
                println!("Progress:\n{}", desc);
            }
        });
        self.join();
        drop(done_tx);
        progress_printer.join().unwrap();
    }

    /// Calls the callback function in any order for every key from start to end.
    /// Start is inclusive, end is exclusive. If end is the empty array, it is unbounded.
    /// This may be used no matter what the distribution of the keys are; it will
    /// internally automatically balance the work between the specified number of threads.
    ///
    /// The callback is called with the parameters (range_id, key, value). Callback
    /// invocations carrying the same range_id are guaranteed to be in key order, and
    /// furthermore, the range of keys returned for each range_id are disjoint. The
    /// range_id is a dense ID meaning that it can be used to index into an array, with
    /// the maximum value of range_id being one less than the total amount of parallel
    /// tasks ever created during the iteration.
    pub fn for_each_in_range(
        store: Store,
        column: DBCol,
        start: Vec<u8>,
        end: Vec<u8>,
        callback: impl Fn(usize, &[u8], &[u8]) + Send + Sync + 'static,
        options: ParallelIterationOptions,
    ) {
        let print_progress = options.print_progress;
        let iter = StoreParallelIterator::new(
            options,
            store,
            column,
            Arc::new(move |range_id, key, value| callback(range_id, key, value.unwrap())),
            WorkItem::new_range(start, end),
        );
        if print_progress {
            iter.wait_for_completion_and_print_progress();
        }
    }

    /// Calls the callback function in any order for every key being looked up.
    /// This may be used no matter what the key/value size distributions are; if there is
    /// any imbalance, it will internally automatically balance the work between the specified
    /// number of threads.
    ///
    /// The callback is called with the parameters (range_id, key, value). Callback
    /// invocations carrying the same range_id are guaranteed to correspond to a contiguous
    /// range of keys as specified in the `keys` array. The range_id is a dense ID meaning that it
    /// can be used to index into an array, with the maximum value of range_id being one
    /// less than the total amount of parallel tasks ever created during the iteration.
    pub fn lookup_keys(
        store: Store,
        column: DBCol,
        keys: Vec<Vec<u8>>,
        callback: impl Fn(usize, &[u8], Option<&[u8]>) + Send + Sync + 'static,
        options: ParallelIterationOptions,
    ) {
        let num_keys = keys.len();
        let print_progress = options.print_progress;
        let iter = StoreParallelIterator::new(
            options,
            store,
            column,
            Arc::new(callback),
            WorkItem::new_lookup(keys, 0..num_keys),
        );
        if print_progress {
            iter.wait_for_completion_and_print_progress();
        }
    }
}

impl Drop for StoreParallelIterator {
    fn drop(&mut self) {
        self.join();
    }
}

#[derive(Clone)]
enum WorkItem {
    Range(IterationRange),
    Lookup(Arc<Vec<Vec<u8>>>, Range<usize>),
}

impl WorkItem {
    /// Divides the work item to multiple smaller ones.
    fn divide(&self) -> Vec<Self> {
        match self {
            WorkItem::Range(range) => range.divide().into_iter().map(WorkItem::Range).collect(),
            WorkItem::Lookup(keys, range) => {
                let start = range.start;
                let end = range.end;
                let ranges = 16.min(end - start);
                let range_size = (end - start) / ranges;
                let mut result = Vec::new();
                for i in 0..ranges {
                    let start = start + i * range_size;
                    let end = if i == ranges - 1 { end } else { start + range_size };
                    result.push(WorkItem::Lookup(keys.clone(), start..end));
                }
                result
            }
        }
    }

    fn new_range(start: Vec<u8>, end: Vec<u8>) -> Self {
        Self::Range(IterationRange::new(start, end))
    }

    fn new_lookup(keys: Vec<Vec<u8>>, range: Range<usize>) -> Self {
        assert!(range.start < range.end);
        Self::Lookup(Arc::new(keys), range)
    }
}

impl Display for WorkItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkItem::Range(range) => write!(f, "range {}", range),
            WorkItem::Lookup(_, range) => {
                write!(f, "keys {}..{}", range.start, range.end)
            }
        }
    }
}

#[derive(Clone)]
struct IterationRange {
    start: Vec<u8>,
    end: Vec<u8>, // empty means unbounded
}

impl IterationRange {
    fn new(start: Vec<u8>, end: Vec<u8>) -> Self {
        assert!(start < end || end.is_empty());
        Self { start, end }
    }

    /// Returns the smallest possible key that is greater than the given key.
    fn next_key(key: &[u8]) -> Vec<u8> {
        let mut key = key.to_vec();
        key.push(0);
        key
    }

    /// Divides the range into smaller ranges. The implementation can be anything as long as
    /// this will exponentially divide the key space.
    fn divide(&self) -> Vec<Self> {
        // First find the common prefix; the resulting ranges will always start with these,
        // so just ignore these for the remaining processing.
        let mut common_prefix = Vec::new();
        for (a, b) in self.start.iter().zip(self.end.iter()) {
            if a == b {
                common_prefix.push(*a);
            } else {
                break;
            }
        }

        // Remove the common prefix.
        let start = self.start[common_prefix.len()..].to_vec();
        let end = self.end[common_prefix.len()..].to_vec();

        // We'll find some split points and use these to split the ranges.
        let mut split_points = Vec::new();

        // If the start is the beginning, divide based on the first character.
        if start.is_empty() {
            if end.len() == 1 {
                // If the end is just one byte, then that byte is exclusive. There exists a corner
                // case where the range is "..00" (i.e. the end is a single zero byte); this range
                // is unsplittable as it only contains one element. But it's OK because we'll end
                // up with no splitting point and we'll return the single range.
                for byte in 0..end[0] {
                    let point = vec![byte];
                    split_points.push(point);
                }
            } else {
                // Otherwise the end byte is inclusive.
                let first_character_max = end.get(0).copied().unwrap_or(0xff);
                for byte in 0..=first_character_max {
                    let point = vec![byte];
                    split_points.push(point);
                }
            }
        }
        // If there's no end, then divide based on the first byte of start that is not 0xff.
        else if end.is_empty() {
            let mut byte_not_ff = 0;
            for (i, byte) in start.iter().enumerate() {
                if *byte != 0xff {
                    byte_not_ff = i;
                    break;
                }
            }
            for byte in start[byte_not_ff] + 1..=0xff {
                let point = start
                    .iter()
                    .copied()
                    .take(byte_not_ff)
                    .chain(std::iter::once(byte))
                    .collect::<Vec<u8>>();
                split_points.push(point);
            }
        } else {
            // If there's both start and end, then the first byte must be different. Divide on that.
            let start_byte = start[0];
            let end_byte = end[0];
            if end.len() == 1 {
                // If the end is just one byte, then that byte is exclusive. In this case, the first
                // byte of the start may be just one less than the end byte, in which case we will
                // need to keep going to find the first start byte that is not 0xff.
                let mut start_prefix_len = 0;
                while start_prefix_len < start.len()
                    && ((start_prefix_len == 0 && start[0] + 1 == end[0])
                        || start[start_prefix_len] == 0xff)
                {
                    start_prefix_len += 1;
                }
                let next_byte_min =
                    start.get(start_prefix_len).copied().map(|v| v + 1).unwrap_or(0);
                let end_byte = if start_prefix_len == 0 { end_byte - 1 } else { 0xff };
                for byte in next_byte_min..=end_byte {
                    let point = start
                        .iter()
                        .copied()
                        .take(start_prefix_len)
                        .chain(std::iter::once(byte))
                        .collect::<Vec<u8>>();
                    split_points.push(point);
                }
            } else {
                // If the end is more than one byte, then just divide on the first byte.
                for byte in start_byte + 1..=end_byte {
                    let point = vec![byte];
                    split_points.push(point);
                }
            }
        }

        // Now we have the split points, we can create the ranges.
        let mut ranges = Vec::new();
        let mut prev = start;
        for point in split_points {
            assert!(!point.is_empty());
            assert!(prev < point, "{} < {}", hex::encode(&prev), hex::encode(&point));
            assert!(
                point < end || end.is_empty(),
                "{} < {} while splitting {} - {}",
                hex::encode(&point),
                hex::encode(&end),
                hex::encode(&self.start),
                hex::encode(&self.end)
            );
            ranges.push(Self::new(prev.clone(), point.clone()));
            prev = point;
        }
        ranges.push(Self::new(prev, end));

        // Finally put the prefix back.
        ranges.iter_mut().for_each(|range| {
            range.start =
                common_prefix.iter().copied().chain(range.start.iter().copied()).collect();
            range.end = common_prefix.iter().copied().chain(range.end.iter().copied()).collect();
        });
        ranges
    }
}

impl FromStr for IterationRange {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split("..").collect();
        if parts.len() != 2 {
            return Err(anyhow::anyhow!("Invalid range format"));
        }
        let start = hex::decode(parts[0])?;
        let end = hex::decode(parts[1])?;
        Ok(Self::new(start, end))
    }
}

impl Display for IterationRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}..{}", hex::encode(&self.start), hex::encode(&self.end))
    }
}

#[cfg(test)]
mod tests {
    use super::StoreParallelIterator;
    use crate::parallel_iter::{IterationRange, ParallelIterationOptions, WorkItem};
    use crate::test_utils::create_test_store;
    use crate::DBCol;
    use std::collections::{BTreeMap, HashMap, HashSet};
    use std::str::FromStr;
    use std::sync::{Arc, Mutex};

    #[test]
    fn test_iteration_range_divide() {
        let ranges = IterationRange::from_str("..").unwrap().divide();
        assert_eq!(ranges.len(), 257);
        assert_eq!(ranges[0].to_string(), "..00");
        assert_eq!(ranges[5].to_string(), "04..05");
        assert_eq!(ranges[256].to_string(), "ff..");

        let ranges = IterationRange::from_str("..04").unwrap().divide();
        assert_eq!(ranges.len(), 5);
        assert_eq!(ranges[0].to_string(), "..00");
        assert_eq!(ranges[1].to_string(), "00..01");
        assert_eq!(ranges[4].to_string(), "03..04");

        let ranges = IterationRange::from_str("..0400").unwrap().divide();
        assert_eq!(ranges.len(), 6);
        assert_eq!(ranges[0].to_string(), "..00");
        assert_eq!(ranges[1].to_string(), "00..01");
        assert_eq!(ranges[4].to_string(), "03..04");
        assert_eq!(ranges[5].to_string(), "04..0400");

        let ranges = IterationRange::from_str("00..").unwrap().divide();
        assert_eq!(ranges.len(), 256);
        assert_eq!(ranges[0].to_string(), "00..01");
        assert_eq!(ranges[5].to_string(), "05..06");
        assert_eq!(ranges[255].to_string(), "ff..");

        let ranges = IterationRange::from_str("00..01").unwrap().divide();
        assert_eq!(ranges.len(), 257);
        assert_eq!(ranges[0].to_string(), "00..0000");
        assert_eq!(ranges[5].to_string(), "0004..0005");
        assert_eq!(ranges[256].to_string(), "00ff..01");

        let ranges = IterationRange::from_str("00..02").unwrap().divide();
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].to_string(), "00..01");
        assert_eq!(ranges[1].to_string(), "01..02");

        let ranges = IterationRange::from_str("00fd..01").unwrap().divide();
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].to_string(), "00fd..00fe");
        assert_eq!(ranges[1].to_string(), "00fe..00ff");
        assert_eq!(ranges[2].to_string(), "00ff..01");

        let ranges = IterationRange::from_str("00fd..02").unwrap().divide();
        assert_eq!(ranges.len(), 2);
        assert_eq!(ranges[0].to_string(), "00fd..01");
        assert_eq!(ranges[1].to_string(), "01..02");

        let ranges = IterationRange::from_str("000000000000000030..01").unwrap().divide();
        assert_eq!(ranges.len(), 256);
        assert_eq!(ranges[0].to_string(), "000000000000000030..0001");
        assert_eq!(ranges[1].to_string(), "0001..0002");
        assert_eq!(ranges[255].to_string(), "00ff..01");

        let ranges = IterationRange::from_str("00fffffd..01").unwrap().divide();
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].to_string(), "00fffffd..00fffffe");
        assert_eq!(ranges[1].to_string(), "00fffffe..00ffffff");
        assert_eq!(ranges[2].to_string(), "00ffffff..01");

        let ranges = IterationRange::from_str("00010203..02030405").unwrap().divide();
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].to_string(), "00010203..01");
        assert_eq!(ranges[1].to_string(), "01..02");
        assert_eq!(ranges[2].to_string(), "02..02030405");

        let ranges = IterationRange::from_str("0100010203..0102030405").unwrap().divide();
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].to_string(), "0100010203..0101");
        assert_eq!(ranges[1].to_string(), "0101..0102");
        assert_eq!(ranges[2].to_string(), "0102..0102030405");

        let ranges = IterationRange::from_str("01000000000000000030..0101").unwrap().divide();
        assert_eq!(ranges.len(), 256);
        assert_eq!(ranges[0].to_string(), "01000000000000000030..010001");
        assert_eq!(ranges[1].to_string(), "010001..010002");
        assert_eq!(ranges[255].to_string(), "0100ff..0101");

        let ranges = IterationRange::from_str("..00").unwrap().divide();
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].to_string(), "..00");
    }

    #[test]
    fn test_work_item_divide() {
        let range = WorkItem::Range(IterationRange::from_str("00..01").unwrap());
        let items = range.divide();
        assert_eq!(items.len(), 257);
        assert_eq!(items[0].to_string(), "range 00..0000");
        assert_eq!(items[5].to_string(), "range 0004..0005");
        assert_eq!(items[256].to_string(), "range 00ff..01");

        let item = WorkItem::Lookup(Arc::new(vec![vec![0], vec![1], vec![2]]), 0..3).divide();
        assert_eq!(item.len(), 3);
        assert_eq!(item[0].to_string(), "keys 0..1");
        assert_eq!(item[1].to_string(), "keys 1..2");
        assert_eq!(item[2].to_string(), "keys 2..3");

        let item = WorkItem::Lookup(Arc::new(vec![]), 0..16).divide();
        assert_eq!(item.len(), 16);
        assert_eq!(item[0].to_string(), "keys 0..1");
        assert_eq!(item[1].to_string(), "keys 1..2");
        assert_eq!(item[2].to_string(), "keys 2..3");
        assert_eq!(item[15].to_string(), "keys 15..16");

        let item = WorkItem::Lookup(Arc::new(vec![]), 0..1).divide();
        assert_eq!(item.len(), 1);
        assert_eq!(item[0].to_string(), "keys 0..1");

        let item = WorkItem::Lookup(Arc::new(vec![]), 0..17).divide();
        assert_eq!(item.len(), 16);
        assert_eq!(item[0].to_string(), "keys 0..1");
        assert_eq!(item[15].to_string(), "keys 15..17");

        let item = WorkItem::Lookup(Arc::new(vec![]), 0..256).divide();
        assert_eq!(item.len(), 16);
        assert_eq!(item[0].to_string(), "keys 0..16");
        assert_eq!(item[15].to_string(), "keys 240..256");
    }

    #[test]
    fn test_parallel_iteration() {
        let store = create_test_store();
        let mut update = store.store_update();
        let mut data = BTreeMap::new();
        for _ in 0..1000000 {
            let key_len = rand::random::<usize>() % 10;
            let key = (0..key_len).map(|_| rand::random::<u8>()).collect::<Vec<_>>();
            let value_len = rand::random::<usize>() % 10;
            data.entry(key.clone()).or_insert_with(|| {
                let value = (0..value_len).map(|_| rand::random::<u8>()).collect::<Vec<_>>();
                update.insert(DBCol::BlockHeader, key, value.clone());
                value
            });
        }
        update.commit().unwrap();

        let read_data = Arc::new(Mutex::new(BTreeMap::new()));
        // For each range, (min key, max key, num keys).
        let ranges = Arc::new(Mutex::new(HashMap::<usize, (Vec<u8>, Vec<u8>, usize)>::new()));
        {
            let read_data = read_data.clone();
            let ranges = ranges.clone();
            StoreParallelIterator::for_each_in_range(
                store,
                DBCol::BlockHeader,
                Vec::new(),
                Vec::new(),
                move |range_id, key: &[u8], value: &[u8]| {
                    assert!(read_data
                        .lock()
                        .unwrap()
                        .insert(key.to_vec(), value.to_vec())
                        .is_none());
                    // Within the same range, the keys must be in order.
                    let mut ranges = ranges.lock().unwrap();
                    ranges
                        .entry(range_id)
                        .and_modify(|(_, last, num_keys)| {
                            assert!(last.as_slice() < key);
                            *last = key.to_vec();
                            *num_keys += 1;
                        })
                        .or_insert_with(|| (key.to_vec(), key.to_vec(), 1));
                },
                ParallelIterationOptions { max_entries_per_range: 1000, ..Default::default() },
            );
        }

        let read_data = read_data.lock().unwrap();
        assert_eq!(*read_data, data);

        // Check that the key ranges returned for the ranges are disjoint.
        let ranges = ranges.lock().unwrap();
        for (_, range) in ranges.iter() {
            assert!(range.2 <= 1000);
        }
        let mut sorted_key_ranges = ranges.values().collect::<Vec<_>>();
        sorted_key_ranges.sort();
        for i in 1..sorted_key_ranges.len() {
            assert!(&sorted_key_ranges[i - 1].1 < &sorted_key_ranges[i].0);
        }
    }

    #[test]
    fn test_parallel_lookup() {
        let store = create_test_store();
        let mut update = store.store_update();
        let mut data = BTreeMap::new();
        for _ in 0..1000000 {
            let key_len = rand::random::<usize>() % 10;
            let key = (0..key_len).map(|_| rand::random::<u8>()).collect::<Vec<_>>();
            let value_len = rand::random::<usize>() % 10;
            data.entry(key.clone()).or_insert_with(|| {
                let value = (0..value_len).map(|_| rand::random::<u8>()).collect::<Vec<_>>();
                update.insert(DBCol::BlockHeader, key, value.clone());
                value
            });
        }
        update.commit().unwrap();

        let shuffled_keys =
            data.keys().cloned().collect::<HashSet<_>>().into_iter().collect::<Vec<_>>();
        let key_to_index = shuffled_keys
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, key)| (key, index))
            .collect::<HashMap<_, _>>();

        let read_data = Arc::new(Mutex::new(BTreeMap::new()));
        let range_last_key_index = Arc::new(Mutex::new(HashMap::<usize, usize>::new()));
        let range_num_keys = Arc::new(Mutex::new(HashMap::<usize, usize>::new()));
        {
            let read_data = read_data.clone();
            let range_num_keys = range_num_keys.clone();
            StoreParallelIterator::lookup_keys(
                store,
                DBCol::BlockHeader,
                shuffled_keys,
                move |range_id, key: &[u8], value: Option<&[u8]>| {
                    assert!(read_data
                        .lock()
                        .unwrap()
                        .insert(key.to_vec(), value.unwrap().to_vec())
                        .is_none());
                    // The range must return keys in the order of contiguous indexes in the original list.
                    let mut range_last_key_index = range_last_key_index.lock().unwrap();
                    let key_index = *key_to_index.get(key).unwrap();
                    if let Some(last_index) = range_last_key_index.insert(range_id, key_index) {
                        assert_eq!(last_index + 1, key_index);
                    }

                    let mut range_num_keys = range_num_keys.lock().unwrap();
                    *range_num_keys.entry(range_id).or_insert(0) += 1;
                },
                ParallelIterationOptions { max_entries_per_range: 1000, ..Default::default() },
            );
        }

        let read_data = read_data.lock().unwrap();
        assert_eq!(*read_data, data);

        let range_num_keys = range_num_keys.lock().unwrap();
        for (_, num_keys) in range_num_keys.iter() {
            assert!(*num_keys <= 1000);
        }
    }
}
