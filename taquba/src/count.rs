//! Counts: a named number of active items, and at most one job waiting for the
//! number to reach zero. [`SettlementEffects::count_up`] and
//! [`SettlementEffects::count_down`] change a count in the transaction of their
//! effects, and [`EnqueueOptions::count_wait`] makes a scheduled job wait for
//! zero. Every change writes the record of the count, so when two transactions
//! change the same count concurrently, the second to commit conflicts and
//! retries. A single change observes each return of the count to zero.
//!
//! [`SettlementEffects::count_up`]: crate::SettlementEffects::count_up
//! [`SettlementEffects::count_down`]: crate::SettlementEffects::count_down
//! [`EnqueueOptions::count_wait`]: crate::EnqueueOptions::count_wait

use serde::{Deserialize, Serialize};

use crate::codec::encode;
use crate::error::{Error, Result};
use crate::job::{JobRecord, JobStatus};
use crate::keys::count_key;
use crate::queue_core::control::{Wake, stage_wake};
use crate::txn::{Txn, get_indexed_job};

/// The stored record of a count, deleted when the count reaches zero.
#[derive(Debug, Default, Serialize, Deserialize)]
struct CountRecord {
    /// The number of active items.
    active: i64,
    /// The id of the job waiting for zero.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    waiter: Option<String>,
}

/// The record of the count at `key`, or the record of zero.
async fn read_record(txn: &Txn, key: &[u8]) -> Result<CountRecord> {
    Ok(match txn.get(key).await? {
        Some(bytes) => rmp_serde::from_slice(&bytes)?,
        None => CountRecord::default(),
    })
}

/// Stage the net change `delta` of the count `name` in `txn`. The change that
/// leaves the count at zero deletes the record and wakes the waiting job.
pub(crate) async fn stage_count_change(txn: &mut Txn, name: &str, delta: i64) -> Result<()> {
    if delta == 0 {
        return Ok(());
    }
    let key = count_key(name);
    let mut record = read_record(txn, &key).await?;
    record.active += delta;
    if record.active != 0 {
        txn.put(&key, encode(&record))?;
        return Ok(());
    }
    txn.delete(&key)?;
    if let Some(waiter) = record.waiter {
        // A recorded job that is no longer waiting, after its `run_at` or a
        // cancel, stays as it is.
        let _ = stage_wake(txn, &waiter, Wake::Count(name.to_string())).await?;
    }
    Ok(())
}

/// Stage the wait of the new scheduled `job` for the count of its
/// [`JobRecord::count_wait`] in `txn`. When the count is zero, the job is
/// pending without a wait. Otherwise the record identifies the job as its
/// waiting job and replaces a recorded job that is no longer waiting.
pub(crate) async fn stage_count_wait(txn: &mut Txn, job: &mut JobRecord) -> Result<()> {
    let Some(name) = &job.count_wait else {
        return Ok(());
    };
    let key = count_key(name);
    let mut record = read_record(txn, &key).await?;
    if record.active == 0 {
        job.status = JobStatus::Pending;
        job.run_at = None;
        job.signal_wait = false;
        job.count_wait = None;
        return Ok(());
    }
    if let Some(waiter) = &record.waiter
        && let Some((_, _, current)) = get_indexed_job(txn, waiter).await?
        && current.status == JobStatus::Scheduled
        && current.count_wait.as_deref() == Some(name.as_str())
    {
        return Err(Error::CountWaiterExists {
            count: name.clone(),
            waiter: waiter.clone(),
        });
    }
    record.waiter = Some(job.id.clone());
    txn.put(&key, encode(&record))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::error::Error;
    use crate::test_util::*;

    const INITIAL: u64 = 1_700_000_000_000;

    async fn open(clock: &MockClock) -> Queue {
        let opts = OpenOptions {
            clock: Arc::new(clock.clone()),
            ..no_backoff_opts()
        };
        Queue::open_with_options(make_store(), "test", opts)
            .await
            .unwrap()
    }

    /// The options of a job that waits for the count `name` for an hour.
    fn waiting(name: &str) -> EnqueueOptions {
        EnqueueOptions::default()
            .run_at(std::time::UNIX_EPOCH + Duration::from_millis(INITIAL + 3_600_000))
            .count_wait(name.to_string())
    }

    async fn job(q: &Queue, id: &str) -> JobRecord {
        q.view().get_job(id).await.unwrap().unwrap()
    }

    async fn change(q: &Queue, effects: SettlementEffects) {
        q.commit_effects(effects).await.unwrap();
    }

    #[tokio::test]
    async fn the_commit_that_leaves_the_count_at_zero_wakes_the_waiting_job() {
        let q = open(&MockClock::new(INITIAL)).await;
        change(
            &q,
            SettlementEffects::default()
                .count_up("batch")
                .count_up("batch"),
        )
        .await;
        let id = q
            .enqueue_with("jobs", b"next".to_vec(), waiting("batch"))
            .await
            .unwrap();

        change(&q, SettlementEffects::default().count_down("batch")).await;
        let waiting = job(&q, &id).await;
        assert_eq!(waiting.status, JobStatus::Scheduled);
        assert_eq!(waiting.count_wait.as_deref(), Some("batch"));

        // The count passes zero within the commit and ends at one.
        change(
            &q,
            SettlementEffects::default()
                .count_down("batch")
                .count_up("batch"),
        )
        .await;
        assert_eq!(job(&q, &id).await.status, JobStatus::Scheduled);

        change(&q, SettlementEffects::default().count_down("batch")).await;
        let woken = job(&q, &id).await;
        assert_eq!(woken.status, JobStatus::Pending);
        assert_eq!(woken.count_wait, None);
        assert_eq!(woken.signal, None);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn concurrent_changes_bring_the_count_to_zero_once() {
        let q = open(&MockClock::new(INITIAL)).await;
        change(
            &q,
            SettlementEffects::default()
                .count_up("batch")
                .count_up("batch"),
        )
        .await;
        let id = q
            .enqueue_with("jobs", b"next".to_vec(), waiting("batch"))
            .await
            .unwrap();

        let (a, b) = tokio::join!(
            q.commit_effects(SettlementEffects::default().count_down("batch")),
            q.commit_effects(SettlementEffects::default().count_down("batch")),
        );
        a.unwrap();
        b.unwrap();
        assert_eq!(job(&q, &id).await.status, JobStatus::Pending);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_wait_for_a_count_at_zero_is_pending_at_once() {
        let q = open(&MockClock::new(INITIAL)).await;
        change(&q, SettlementEffects::default().count_up("done")).await;
        change(&q, SettlementEffects::default().count_down("done")).await;
        for name in ["done", "never-counted"] {
            let id = q
                .enqueue_with("jobs", b"next".to_vec(), waiting(name))
                .await
                .unwrap();
            let pending = job(&q, &id).await;
            assert_eq!(pending.status, JobStatus::Pending, "{name}");
            assert_eq!(pending.count_wait, None, "{name}");
            assert_eq!(pending.run_at, None, "{name}");
        }
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_count_below_zero_wakes_its_waiting_job_at_zero() {
        let q = open(&MockClock::new(INITIAL)).await;
        change(&q, SettlementEffects::default().count_down("batch")).await;
        let id = q
            .enqueue_with("jobs", b"next".to_vec(), waiting("batch"))
            .await
            .unwrap();
        assert_eq!(job(&q, &id).await.status, JobStatus::Scheduled);

        change(&q, SettlementEffects::default().count_up("batch")).await;
        assert_eq!(job(&q, &id).await.status, JobStatus::Pending);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn the_count_changes_of_a_settlement_apply_before_its_enqueues() {
        let q = open(&MockClock::new(INITIAL)).await;
        let ids = q
            .commit_effects(
                SettlementEffects::default()
                    .enqueue(crate::effects::EnqueueRequest {
                        queue: "jobs".to_string(),
                        payload: b"next".to_vec(),
                        options: waiting("batch"),
                    })
                    .count_up("batch"),
            )
            .await
            .unwrap();
        assert_eq!(job(&q, ids[0].id()).await.status, JobStatus::Scheduled);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_count_wait_requires_run_at() {
        let q = open(&MockClock::new(INITIAL)).await;
        let err = q
            .enqueue_with(
                "jobs",
                b"x".to_vec(),
                EnqueueOptions::default().count_wait("batch".to_string()),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, Error::CountWaitWithoutRunAt));
        assert_eq!(q.view().stats("jobs").await.unwrap().pending, 0);
        q.close().await.unwrap();
    }

    #[tokio::test]
    async fn a_wait_replaces_only_a_waiting_job_that_left_its_wait() {
        let clock = MockClock::new(INITIAL);
        let q = open(&clock).await;
        change(&q, SettlementEffects::default().count_up("batch")).await;
        let timed_out = q
            .enqueue_with(
                "jobs",
                b"timed-out".to_vec(),
                waiting("batch")
                    .run_at(std::time::UNIX_EPOCH + Duration::from_millis(INITIAL + 1_000)),
            )
            .await
            .unwrap();

        let err = q
            .enqueue_with("jobs", b"rejected".to_vec(), waiting("batch"))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, Error::CountWaiterExists { count, waiter }
                if count == "batch" && *waiter == timed_out),
            "{err:?}"
        );

        clock.advance(Duration::from_secs(2));
        q.promote_scheduled_now().await.unwrap();
        let promoted = job(&q, &timed_out).await;
        assert_eq!(promoted.status, JobStatus::Pending);
        assert_eq!(promoted.count_wait, None);

        let replacement = q
            .enqueue_with("jobs", b"replacement".to_vec(), waiting("batch"))
            .await
            .unwrap();
        change(&q, SettlementEffects::default().count_down("batch")).await;
        assert_eq!(job(&q, &replacement).await.status, JobStatus::Pending);
        q.close().await.unwrap();
    }
}
