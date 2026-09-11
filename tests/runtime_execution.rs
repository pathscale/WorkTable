//! Runtime selection must execute work, not merely retain metadata.
use std::sync::{Arc, Mutex};
use std::thread::ThreadId;
use worktable::{prelude::*, runtimes, worktable};

runtimes! { scheduled: nagoya(shared_slot), }
worktable! {
    name: Scheduled,
    runtime: nagoya(shared_slot),
    columns: { id: u64 primary_key, value: u64, group: u64 },
    indexes: { group_idx: group },
    queries: {
        update runtime scheduled: { ValueById(value) by id },
        delete runtime scheduled: { ByGroup() by group },
        in_place runtime scheduled: { ValueById(value) by id },
    }
}

static DISPATCH_THREAD: Mutex<Option<ThreadId>> = Mutex::new(None);
struct Observed;
impl Profile for Observed {
    type Backend = NagoyaRt<SharedSlot>;
    fn tuning() -> Tuning {
        scheduled::tuning()
    }
    fn dispatcher() -> worktable::runtime::Dispatch {
        |work| {
            Box::pin(worktable::runtime::run_on::<NagoyaRt<SharedSlot>, _>(async move {
                *DISPATCH_THREAD.lock().unwrap() = Some(std::thread::current().id());
                work();
            }))
        }
    }
}

#[test]
fn generated_profiles_dispatch_mutations_and_owned_selects() {
    nagoya::block_on(async {
        let table = Arc::new(ScheduledWorkTable::default());
        for id in 0..10 {
            table
                .insert(ScheduledRow {
                    id,
                    value: id,
                    group: id % 2,
                })
                .await
                .unwrap();
        }
        table
            .update_value_by_id(ValueByIdQuery { value: 100 }, 9u64)
            .await
            .unwrap();
        let caller = std::thread::current().id();
        table
            .update_value_by_id_in_place(
                move |value| {
                    assert_ne!(std::thread::current().id(), caller);
                    *value = 101.into();
                },
                9u64,
            )
            .await
            .unwrap();
        assert_eq!(table.select(9u64).unwrap().value, 101);
        assert!(matches!(
            table.select_all().runtime(scheduled).execute(),
            Err(WorkTableError::RuntimeRequiresAsync)
        ));
        // The borrowed predicate is evaluated before scheduling. It need not be Send or 'static.
        let minimum = std::rc::Rc::new(2u64);
        let future = table
            .select_all()
            .where_by(|row| row.id >= *minimum)
            .range_on(ScheduledRowFields::Value, 0u64..50)
            .order_on(ScheduledRowFields::Id, Order::Desc)
            .offset(1)
            .limit(3)
            .runtime(Observed)
            .execute_async();
        drop(minimum); // The returned future no longer borrows the predicate state.
        let selected = future.await.unwrap();
        assert_eq!(selected.iter().map(|r| r.id).collect::<Vec<_>>(), vec![7, 6, 5]);
        assert_ne!(DISPATCH_THREAD.lock().unwrap().unwrap(), caller);
        assert_eq!(table.select_all().execute_async().await.unwrap().len(), 10);
        table.delete_by_group(1u64).await.unwrap();
        assert_eq!(table.select_all().execute().unwrap().len(), 5);
        assert!(table.select(9u64).is_none());
    });
}

#[test]
fn nested_dispatch_progresses_on_one_worker() {
    nagoya::block_on(async {
        let result = worktable::runtime::run_on::<NagoyaRt<SharedSlot>, _>(async {
            let table = Arc::new(ScheduledWorkTable::default());
            table
                .insert(ScheduledRow {
                    id: 1,
                    value: 2,
                    group: 3,
                })
                .await
                .unwrap();
            table
                .update_value_by_id(ValueByIdQuery { value: 4 }, 1u64)
                .await
                .unwrap();
            table.select_all().runtime(scheduled).execute_async().await.unwrap()[0].value
        })
        .await
        .unwrap();
        assert_eq!(result, 4);
    });
}

#[test]
fn dropping_a_pending_dispatch_cancels_its_owned_future() {
    use std::sync::atomic::{AtomicBool, Ordering};
    struct Dropped(Arc<AtomicBool>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    let dropped = Arc::new(AtomicBool::new(false));
    let started = Arc::new(AtomicBool::new(false));
    let signal = started.clone();
    let guard = Dropped(dropped.clone());
    let mut task = Box::pin(worktable::runtime::run_on::<NagoyaRt<SharedSlot>, _>(async move {
        let _guard = guard;
        signal.store(true, Ordering::Release);
        std::future::pending::<()>().await;
    }));
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    assert!(std::future::Future::poll(task.as_mut(), &mut cx).is_pending());
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !started.load(Ordering::Acquire) {
        assert!(std::time::Instant::now() < deadline);
        std::thread::yield_now();
    }
    drop(task);
    while !dropped.load(Ordering::Acquire) {
        assert!(std::time::Instant::now() < deadline);
        std::thread::yield_now();
    }
}

#[test]
fn a_panicking_owned_task_does_not_hang_the_caller() {
    let result = std::panic::catch_unwind(|| {
        nagoya::block_on(worktable::runtime::run_on::<NagoyaRt<SharedSlot>, _>(async {
            panic!("owned task panic")
        }))
    });
    assert!(result.is_err());
}

worktable! {
    name: ScheduledDisk,
    persist: true,
    runtime: nagoya(shared_slot),
    columns: { id: u64 primary_key, value: u64 },
    queries: { update runtime scheduled: { DiskValueById(value) by id } }
}

#[test]
fn scheduled_mutation_is_persisted_and_reopened() {
    nagoya::block_on(async {
        let dir = std::path::PathBuf::from(format!("tests/data/runtime-execution-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let config = DiskConfig::new_with_table_name(
            dir.to_str().unwrap(),
            ScheduledDiskWorkTable::name_snake_case(),
            ScheduledDiskWorkTable::version(),
        );
        {
            let engine = ScheduledDiskPersistenceEngine::new(config.clone()).await.unwrap();
            let table = Arc::new(ScheduledDiskWorkTable::load(engine).await.unwrap());
            table.insert(ScheduledDiskRow { id: 1, value: 2 }).await.unwrap();
            table
                .update_disk_value_by_id(DiskValueByIdQuery { value: 99 }, 1u64)
                .await
                .unwrap();
            assert_eq!(
                table.select_all().runtime(scheduled).execute_async().await.unwrap()[0].value,
                99
            );
            table.wait_for_ops().await.unwrap();
        }
        let engine = ScheduledDiskPersistenceEngine::new(config).await.unwrap();
        let table = ScheduledDiskWorkTable::load(engine).await.unwrap();
        assert_eq!(table.select(1u64).unwrap().value, 99);
        drop(table);
        std::fs::remove_dir_all(dir).unwrap();
    });
}

#[cfg(feature = "tokio-runtime")]
mod tokio_execution {
    use super::*;
    runtimes! { on_tokio: tokio, }
    worktable! {
        name: TokioScheduled,
        runtime: tokio,
        columns: { id: u64 primary_key, value: u64 },
        queries: { in_place runtime on_tokio: { TokioValueById(value) by id } }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn tokio_profiles_dispatch_on_the_entered_runtime() {
        let caller = std::thread::current().id();
        let table = Arc::new(TokioScheduledWorkTable::default());
        table.insert(TokioScheduledRow { id: 1, value: 0 }).await.unwrap();
        table
            .update_tokio_value_by_id_in_place(
                move |value| {
                    assert_ne!(std::thread::current().id(), caller);
                    *value = 8.into();
                },
                1u64,
            )
            .await
            .unwrap();
        assert_eq!(
            table.select_all().runtime(on_tokio).execute_async().await.unwrap()[0].value,
            8
        );
    }
}

runtimes! { default_profile: nagoya, }
worktable! { name: DefaultScheduled, columns: { id: u64 primary_key } }
#[test]
fn an_omitted_table_runtime_matches_a_bare_nagoya_profile() {
    nagoya::block_on(async {
        let table = DefaultScheduledWorkTable::default();
        table.insert(DefaultScheduledRow { id: 1 }).await.unwrap();
        assert_eq!(
            table
                .select_all()
                .runtime(default_profile)
                .execute_async()
                .await
                .unwrap()
                .len(),
            1
        );
    });
}

#[test]
fn an_owned_select_future_outlives_the_table() {
    let future = {
        let table = ScheduledWorkTable::default();
        nagoya::block_on(table.insert(ScheduledRow {
            id: 1,
            value: 2,
            group: 3,
        }))
        .unwrap();
        let future = table.select_all().runtime(scheduled).execute_async();
        drop(table);
        future
    };
    assert_eq!(nagoya::block_on(future).unwrap()[0].id, 1);
}
