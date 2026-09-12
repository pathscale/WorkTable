use crate::remove_dir_if_exists;
// A tokio `TcpStream`, so tokio's extension traits: this is the mock S3
// server the test talks to, not the storage path the crate took off tokio.
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use worktable::database_s3_persistence;
use worktable::prelude::*;
use worktable::s3_sync_persistence;
use worktable::worktable;

worktable!(
    name: TestS3,
    persist: true,
    columns: {
        id: u64 primary_key autoincrement,
        value: u64,
        payload: String,
    },
);

s3_sync_persistence!(TestS3WorkTable);
database_s3_persistence!(TestS3WorkTable);

fn data_page_image(address: worktable::data_bucket::storage::PageAddress, rows: u32, fill: u8) -> Vec<u8> {
    use worktable::data_bucket::{DataPage, GeneralHeader, INNER_PAGE_SIZE, Link, PAGE_SIZE, PageType};

    let mut page = DataPage::<INNER_PAGE_SIZE>::new();
    for index in 0..rows {
        let offset = index * 64;
        page.update_at(
            Link {
                page_id: address.page_id,
                offset,
                length: 64,
            },
            &[fill; 64],
        )
        .unwrap();
    }
    let mut header = GeneralHeader::new(address.page_id, PageType::Data, address.space_id);
    header.data_length = page.length;
    let mut image = worktable::prelude::rkyv::to_bytes::<worktable::prelude::rkyv::rancor::Error>(&header)
        .unwrap()
        .to_vec();
    image.extend_from_slice(&page.encode(INNER_PAGE_SIZE).unwrap());
    assert_eq!(image.len(), PAGE_SIZE);
    image
}

#[derive(Clone, Default)]
struct FakeS3State {
    objects: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    puts: Arc<Mutex<Vec<(String, usize)>>>,
    gets: Arc<Mutex<Vec<String>>>,
    reject_manifest_puts: Arc<AtomicBool>,
}

async fn fake_s3() -> (String, FakeS3State, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let state = FakeS3State::default();
    let server_state = state.clone();
    let task = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let state = server_state.clone();
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

                let request_line = std::str::from_utf8(&request[..header_end])
                    .unwrap()
                    .lines()
                    .next()
                    .unwrap();
                let mut request_parts = request_line.split_whitespace();
                let method = request_parts.next().unwrap();
                let target = request_parts.next().unwrap();
                let path = target.split('?').next().unwrap();
                let key = path.strip_prefix("/test/").unwrap_or(path.trim_start_matches('/'));

                let headers = std::str::from_utf8(&request[..header_end])
                    .unwrap()
                    .lines()
                    .skip(1)
                    .filter_map(|line| line.split_once(':'))
                    .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_string()))
                    .collect::<HashMap<_, _>>();

                let (status, content_type, body, etag) = if method == "PUT" {
                    let body = request[header_end..header_end + content_length].to_vec();
                    if key.ends_with("/manifest.v1") && state.reject_manifest_puts.load(Ordering::Acquire) {
                        (
                            "500 Internal Server Error",
                            "text/plain",
                            b"injected failure".to_vec(),
                            None,
                        )
                    } else {
                        let mut objects = state.objects.lock().unwrap();
                        let current_etag = objects
                            .get(key)
                            .map(|body| format!("\"{}\"", blake3::hash(body).to_hex()));
                        let conflict = headers.get("if-none-match").is_some_and(|value| value == "*")
                            && current_etag.is_some()
                            || headers
                                .get("if-match")
                                .is_some_and(|value| Some(value) != current_etag.as_ref());
                        if conflict {
                            (
                                "412 Precondition Failed",
                                "text/plain",
                                b"conflict".to_vec(),
                                current_etag,
                            )
                        } else {
                            let etag = format!("\"{}\"", blake3::hash(&body).to_hex());
                            objects.insert(key.to_string(), body);
                            drop(objects);
                            state.puts.lock().unwrap().push((key.to_string(), content_length));
                            ("200 OK", "application/octet-stream", Vec::new(), Some(etag))
                        }
                    }
                } else if target.contains("list-type=2") {
                    (
                        "200 OK",
                        "application/xml",
                        b"<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>test</Name><Prefix></Prefix><KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated></ListBucketResult>".to_vec(),
                        None,
                    )
                } else if let Some(stored) = state.objects.lock().unwrap().get(key).cloned() {
                    state.gets.lock().unwrap().push(key.to_string());
                    let etag = format!("\"{}\"", blake3::hash(&stored).to_hex());
                    if method == "HEAD" {
                        ("200 OK", "application/octet-stream", Vec::new(), Some(etag))
                    } else if let Some(range) = headers.get("range") {
                        let range = range.strip_prefix("bytes=").unwrap();
                        let (start, end) = range.split_once('-').unwrap();
                        let start = start.parse::<usize>().unwrap();
                        let end = end.parse::<usize>().unwrap();
                        (
                            "206 Partial Content",
                            "application/octet-stream",
                            stored[start..=end].to_vec(),
                            Some(etag),
                        )
                    } else {
                        ("200 OK", "application/octet-stream", stored, Some(etag))
                    }
                } else {
                    ("404 Not Found", "text/plain", b"not found".to_vec(), None)
                };
                let etag = etag.map_or_else(String::new, |value| format!("ETag: {value}\r\n"));
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\n{etag}Content-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
                socket.write_all(&body).await.unwrap();
            });
        }
    });
    (format!("http://{address}"), state, task)
}

#[test]
fn data_bucket_domain_uses_one_generated_catalog_and_page_range_reads() {
    use worktable::S3Database;
    use worktable::data_bucket::storage::s3::S3Config as DataBucketS3Config;
    use worktable::data_bucket::storage::{PageAddress, PageKind, StorageDomainId};
    use worktable::data_bucket::{PAGE_SIZE, PageId, SpaceId};

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        let (endpoint, state, server) = fake_s3().await;
        let domain = StorageDomainId([9; 16]);
        let config = DataBucketS3Config {
            bucket_name: "test".to_string(),
            endpoint,
            access_key: "test".to_string(),
            secret_key: "test".to_string(),
            session_token: None,
            region: "auto".to_string(),
            prefix: Some("db-domain".to_string()),
            virtual_host_style: false,
        };
        let database = S3Database::open_s3(domain, 1, config.clone()).unwrap();
        let table = database.register_table("orders", 3).unwrap();
        let address = PageAddress {
            domain,
            table_id: table,
            space_id: SpaceId(2),
            page_id: PageId::from(4),
            page_kind: PageKind::Data,
        };

        let before = state.puts.lock().unwrap().iter().map(|(_, bytes)| bytes).sum::<usize>();
        let image = data_page_image(address, 9, 0x5a);
        let mut generation = database.begin_generation().unwrap();
        generation.put_page(address, image.clone(), 9, 777);
        database.commit_generation(generation.finish()).unwrap();
        let after = state.puts.lock().unwrap().iter().map(|(_, bytes)| bytes).sum::<usize>();
        println!("DB_S3_INCREMENTAL_BYTES={}", after - before);
        assert!(
            after - before < PAGE_SIZE * 3,
            "one page and its catalog update uploaded {} bytes",
            after - before
        );
        assert_eq!(database.read_page(address).unwrap(), Some(image.clone()));
        assert_eq!(database.catalog().system_tables()[0].row_count, 9);

        let mut generation = database.begin_generation().unwrap();
        for page in 1_u32..=200 {
            if page == 4 {
                continue;
            }
            generation.put_page(
                PageAddress {
                    page_id: PageId::from(page),
                    ..address
                },
                data_page_image(
                    PageAddress {
                        page_id: PageId::from(page),
                        ..address
                    },
                    1,
                    page as u8,
                ),
                1,
                64,
            );
        }
        database.commit_generation(generation.finish()).unwrap();

        drop(database);
        let reopened = S3Database::open_s3(domain, 2, config).unwrap();
        assert_eq!(reopened.read_page(address).unwrap(), Some(image));
        assert_eq!(reopened.catalog().system_pages().len(), 200);

        let address = PageAddress {
            page_id: PageId::from(150),
            ..address
        };
        let before = state.puts.lock().unwrap().iter().map(|(_, bytes)| bytes).sum::<usize>();
        let image = data_page_image(address, 2, 0xa5);
        let mut generation = reopened.begin_generation().unwrap();
        generation.put_page(address, image.clone(), 2, 128);
        reopened.commit_generation(generation.finish()).unwrap();
        let after = state.puts.lock().unwrap().iter().map(|(_, bytes)| bytes).sum::<usize>();
        println!("DB_S3_LARGE_CATALOG_INCREMENTAL_BYTES={}", after - before);
        assert!(
            after - before < PAGE_SIZE * 4,
            "one page update with a multi-page catalog uploaded {} bytes",
            after - before
        );
        assert_eq!(reopened.read_page(address).unwrap(), Some(image));
        server.abort();
    });
}

#[test]
fn generated_table_uses_the_shared_database_domain_and_restores() {
    use worktable::data_bucket::storage::StorageDomainId;
    use worktable::data_bucket::storage::s3::S3Config as DataBucketS3Config;
    use worktable::{DatabaseS3DiskConfig, S3Database};

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        let path = "tests/data/s3/database_domain";
        remove_dir_if_exists(path.to_string()).await;
        let (endpoint, _, server) = fake_s3().await;
        let domain = StorageDomainId([0x44; 16]);
        let s3 = DataBucketS3Config {
            bucket_name: "test".to_string(),
            endpoint,
            access_key: "test".to_string(),
            secret_key: "test".to_string(),
            session_token: None,
            region: "auto".to_string(),
            prefix: Some("generated-database".to_string()),
            virtual_host_style: false,
        };
        let disk = DiskConfig::new_with_table_name(path, "orders", TestS3WorkTable::version());
        let database = S3Database::open_s3(domain, 10, s3.clone()).unwrap();
        {
            let engine = TestS3DatabaseS3PersistenceEngine::new(DatabaseS3DiskConfig {
                disk: disk.clone(),
                database: database.clone(),
            })
            .await
            .unwrap();
            let table = TestS3WorkTable::load(engine).await.unwrap();
            table
                .insert(TestS3Row {
                    id: table.get_next_pk().into(),
                    value: 7,
                    payload: "remote".repeat(200),
                })
                .await
                .unwrap();
            table.wait_for_ops().await.unwrap();
            assert!(!database.catalog().system_pages().is_empty());
        }

        remove_dir_if_exists(disk.table_path().to_string()).await;
        let reopened_database = S3Database::open_s3(domain, 11, s3).unwrap();
        let engine = TestS3DatabaseS3PersistenceEngine::new(DatabaseS3DiskConfig {
            disk: disk.clone(),
            database: reopened_database.clone(),
        })
        .await
        .unwrap();
        let reopened = TestS3WorkTable::load(engine).await.unwrap();
        let row = reopened.select(0).expect("row restored through the database catalog");
        assert_eq!(row.value, 7);
        assert_eq!(reopened_database.catalog().system_tables().len(), 1);
        remove_dir_if_exists(path.to_string()).await;
        server.abort();
    });
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

        let (endpoint, s3, server) = fake_s3().await;

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
            for value in 0..2600 {
                table
                    .insert(TestS3Row {
                        id: table.get_next_pk().into(),
                        value,
                        payload: format!("{value:0>4096}"),
                    })
                    .await
                    .unwrap();
            }
            assert_eq!(table.select_all().execute().unwrap().len(), 2600);
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
            table.wait_for_ops().await.unwrap();

            let uploaded_before = s3.puts.lock().unwrap().iter().map(|(_, length)| length).sum::<usize>();
            let table_bytes = std::fs::read_dir(config.disk.table_path())
                .unwrap()
                .map(|entry| entry.unwrap().metadata().unwrap().len() as usize)
                .sum::<usize>();
            let mut row = table.select(1300).expect("persisted row");
            row.value = 20_000;
            table.update(row).await.unwrap();
            table.wait_for_ops().await.unwrap();
            let uploaded_after = s3.puts.lock().unwrap().iter().map(|(_, length)| length).sum::<usize>();
            let incremental_bytes = uploaded_after - uploaded_before;
            println!(
                "S3_TRANSFER table_bytes={table_bytes} incremental_bytes={incremental_bytes} ratio={:.3}",
                incremental_bytes as f64 / table_bytes as f64
            );
            assert!(
                incremental_bytes < data_bucket::PAGE_SIZE * 2,
                "one row update uploaded {incremental_bytes} bytes for a {table_bytes}-byte table"
            );

            table
                .insert(TestS3Row {
                    id: 2600,
                    value: 2600,
                    payload: "x".repeat(4096),
                })
                .await
                .unwrap();
            table.wait_for_ops().await.unwrap();
            table.delete(100).await.unwrap();
            table.wait_for_ops().await.unwrap();

            // New immutable segments may arrive before the commit point. If the
            // manifest PUT fails, a fresh reader must still see the preceding
            // complete table generation.
            s3.reject_manifest_puts.store(true, Ordering::Release);
            table
                .insert(TestS3Row {
                    id: 2601,
                    value: 2601,
                    payload: "y".repeat(4096),
                })
                .await
                .unwrap();
            assert!(table.wait_for_ops().await.is_err());
        }
        s3.reject_manifest_puts.store(false, Ordering::Release);

        let puts = s3.puts.lock().unwrap().clone();
        assert!(puts.iter().any(|(key, _)| key.ends_with("/manifest.v1")));
        assert!(puts.iter().any(|(key, _)| key.contains("/chunks/")));
        assert!(
            puts.iter()
                .all(|(key, _)| key.ends_with("/manifest.v1") || key.contains("/chunks/")),
            "new S3 writes must use immutable segments and the table manifest: {puts:?}"
        );

        // Removing the complete local table forces a strict remote restore.
        // The one manifest must reconstruct data and every index before the
        // directory is atomically installed for DiskPersistenceEngine.
        remove_dir_if_exists(config.disk.table_path().to_string()).await;
        {
            let engine = TestS3S3SyncPersistenceEngine::new(config.clone()).await.unwrap();
            let table = TestS3WorkTable::load(engine).await.unwrap();
            let rows = table.select_all().execute().unwrap();
            assert_eq!(rows.len(), 2600);
            assert!(table.select(100).is_none(), "deleted primary key returned");
            assert!(table.select(2601).is_none(), "uncommitted S3 generation became visible");
            for id in (0..=2600).filter(|id| *id != 100) {
                let row = table.select(id).expect("every primary key survives");
                let expected = if id == 257 {
                    10_000
                } else if id == 1300 {
                    20_000
                } else {
                    id
                };
                assert_eq!(row.value, expected, "wrong value for primary key {id}");
            }
        }

        // A committed manifest is authoritative, but a failed restore must
        // leave a usable local table untouched until the remote damage is
        // repaired.
        let missing_segment = s3
            .gets
            .lock()
            .unwrap()
            .iter()
            .find(|key| key.contains("/chunks/"))
            .cloned()
            .unwrap();
        s3.objects.lock().unwrap().remove(&missing_segment);
        assert!(TestS3S3SyncPersistenceEngine::new(config.clone()).await.is_err());
        {
            let engine = TestS3PersistenceEngine::new(config.disk.clone()).await.unwrap();
            let table = TestS3WorkTable::load(engine).await.unwrap();
            assert_eq!(table.select_all().execute().unwrap().len(), 2600);
            assert_eq!(table.select(257).unwrap().value, 10_000);
        }

        server.abort();
    });
}
