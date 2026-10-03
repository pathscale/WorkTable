#![cfg(feature = "s3-support")]
use std::sync::Arc;
use worktable::prelude::*;
use worktable::{s3_sync_persistence, worktable};
worktable!(name: TestS3, persist: true, columns: { id: u64 primary_key autoincrement, value: u64, payload: String, },);
s3_sync_persistence!(TestS3WorkTable);

#[test]
fn supplied_s3_agent_enforces_tls_roots_and_hostname() {
    use std::io::{Read, Write};
    let identity = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert = identity.cert.der().clone();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let server = Arc::new(
        rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.clone()],
                rustls::pki_types::PrivatePkcs8KeyDer::from(identity.signing_key.serialize_der()).into(),
            )
            .unwrap(),
    );
    for (host, approved) in [("localhost", true), ("127.0.0.1", true), ("localhost", false)] {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = server.clone();
        let worker = std::thread::spawn(move || {
            for request_index in 0..if host == "localhost" && approved { 2 } else { 1 } {
                let (socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .unwrap();
                let mut tls = rustls::StreamOwned::new(rustls::ServerConnection::new(server.clone()).unwrap(), socket);
                if tls.read(&mut [0u8; 8192]).is_ok() {
                    let response = if request_index == 0 {
                        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_owned()
                    } else {
                        let body = "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>test</Name><Prefix></Prefix><KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated></ListBucketResult>";
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        )
                    };
                    let _ = tls.write_all(response.as_bytes());
                    let _ = tls.flush();
                }
            }
        });
        let mut roots = rustls::RootCertStore::empty();
        if approved {
            roots.add(cert.clone()).unwrap();
        }
        let tls = Arc::new(
            rustls::ClientConfig::builder_with_provider(provider.clone())
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_root_certificates(roots)
                .with_no_client_auth(),
        );
        let agent = ureq::AgentBuilder::new()
            .tls_config(tls)
            .timeout(std::time::Duration::from_secs(5))
            .redirects(0)
            .build();
        let directory = format!("target/tls-s3-{}", address.port());
        let config = S3DiskConfig {
            disk: DiskConfig::new_with_table_name(
                directory.clone(),
                TestS3WorkTable::name_snake_case(),
                TestS3WorkTable::version(),
            ),
            s3: S3Config {
                bucket_name: "test".into(),
                endpoint: format!("https://{host}:{}/", address.port()),
                access_key: "test".into(),
                secret_key: "test".into(),
                region: None,
                prefix: None,
            },
        };
        let result = nagoya::block_on(TestS3S3SyncPersistenceEngine::new_with_agent(config, agent));
        assert_eq!(
            result.is_ok(),
            host == "localhost" && approved,
            "{:?}",
            result.as_ref().err()
        );
        drop(result);
        worker.join().unwrap();
        if std::path::Path::new(&directory).exists() {
            std::fs::remove_dir_all(directory).unwrap();
        }
    }
}
