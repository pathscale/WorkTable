use crate::remove_dir_if_exists;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use worktable::prelude::*;
use worktable::s3_sync_persistence;
use worktable::worktable;

worktable!(
    name: TestS3,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        value: u64,
    },
);

s3_sync_persistence!(TestS3WorkTable);

async fn fake_s3() -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut chunk = [0_u8; 8192];
                let (header_end, content_length) = loop {
                    let read = socket.read(&mut chunk).await.unwrap();
                    if read == 0 {
                        return;
                    }
                    request.extend_from_slice(&chunk[..read]);
                    let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n") else {
                        continue;
                    };
                    let headers = std::str::from_utf8(&request[..header_end]).unwrap();
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    break (header_end + 4, content_length);
                };

                while request.len() < header_end + content_length {
                    let read = socket.read(&mut chunk).await.unwrap();
                    if read == 0 {
                        return;
                    }
                    request.extend_from_slice(&chunk[..read]);
                }

                let is_list = request.starts_with(b"GET ");
                let body = if is_list {
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult><Name>test</Name><Prefix></Prefix><KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated></ListBucketResult>"
                } else {
                    ""
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            });
        }
    });
    (format!("http://{address}"), task)
}

#[test]
fn s3_engine_reuses_logical_persistence_for_a_loaded_default_arctic_table() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        remove_dir_if_exists("tests/data/s3/compile_test".to_string()).await;

        let (endpoint, server) = fake_s3().await;

        let config = S3DiskConfig {
            disk: DiskConfig::new_with_table_name(
                "tests/data/s3/compile_test",
                TestS3WorkTable::name_snake_case(),
                TestS3WorkTable::version(),
            ),
            s3: S3Config {
                bucket_name: "test".to_string(),
                endpoint,
                access_key: "test".to_string(),
                secret_key: "test".to_string(),
                region: None,
                prefix: Some("wt-test".to_string()),
            },
        };

        // Build the existing WTI-compatible on-disk format through the normal
        // generated engine. Default in-memory indexes emit logical Arctic
        // events, so every wrapper around this engine must select the same
        // logical-to-structural persistence adapter after loading it.
        {
            let engine = TestS3PersistenceEngine::new(config.disk.clone()).await.unwrap();
            let table = TestS3WorkTable::load(engine).await.unwrap();
            for value in 0..512 {
                table
                    .insert(TestS3Row {
                        id: table.get_next_pk().into(),
                        value,
                    })
                    .await
                    .unwrap();
            }
            assert_eq!(table.select_all().execute().unwrap().len(), 512);
            table.wait_for_ops().await.unwrap();
        }

        // Before beta19 the S3 macro hardcoded raw SpaceIndex here. The
        // update's logical primary-index event was then interpreted as a WTI
        // structural event and failed with "index event references a missing
        // page" during shutdown.
        {
            let engine = TestS3S3SyncPersistenceEngine::new(config.clone()).await.unwrap();
            let table = TestS3WorkTable::load(engine).await.unwrap();
            let mut row = table.select(257).expect("persisted row");
            row.value = 10_000;
            table.update(row).await.unwrap();
            table.insert(TestS3Row { id: 512, value: 512 }).await.unwrap();
            table.delete(100).await.unwrap();
            table.wait_for_ops().await.unwrap();
        }

        {
            let engine = TestS3PersistenceEngine::new(config.disk.clone()).await.unwrap();
            let table = TestS3WorkTable::load(engine).await.unwrap();
            let rows = table.select_all().execute().unwrap();
            assert_eq!(rows.len(), 512);
            assert!(table.select(100).is_none(), "deleted primary key returned");
            for id in (0..=512).filter(|id| *id != 100) {
                let row = table.select(id).expect("every primary key survives");
                let expected = if id == 257 { 10_000 } else { id };
                assert_eq!(row.value, expected, "wrong value for primary key {id}");
            }
        }

        server.abort();
    });
}
