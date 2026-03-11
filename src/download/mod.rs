mod api;
mod naming;
mod service;
mod transfer;

pub use api::request_fast_download_url;
pub use naming::{FilenameMode, destination_path, sanitize_file_name};
pub use service::{DownloadExecution, DownloadOptions, execute_download};
pub use transfer::{DownloadTransferResult, download_to_path, file_matches_md5, verify_file_md5};

#[cfg(test)]
mod tests {
    use std::fs;
    use std::net::TcpListener;
    use std::path::Path;
    use std::sync::mpsc;
    use std::thread;
    use std::time::{SystemTime, UNIX_EPOCH};

    use reqwest::blocking::Client;
    use tiny_http::{Method, Response, Server};

    use crate::records::RecordSummary;

    use super::{
        DownloadTransferResult, FilenameMode, destination_path, download_to_path, file_matches_md5,
        request_fast_download_url, sanitize_file_name, verify_file_md5,
    };

    #[test]
    fn sanitizes_destination_filenames() {
        assert_eq!(
            sanitize_file_name("bad/name:here?.pdf"),
            "bad_name_here_.pdf"
        );

        let record = RecordSummary {
            rid: 1,
            aa_id: "aa-1".into(),
            md5: Some("abcdef0123456789abcdef0123456789".into()),
            title: None,
            author: None,
            original_filename: Some("bad/name.pdf".into()),
            extension: Some("pdf".into()),
            local_path: None,
        };

        let destination = destination_path(
            Path::new("/tmp/output"),
            &record,
            FilenameMode::Original,
            None,
        )
        .unwrap();
        assert!(destination.ends_with("bad_name.pdf"));
    }

    #[test]
    fn supports_output_name_and_filename_modes() {
        let record = RecordSummary {
            rid: 1,
            aa_id: "aa-1".into(),
            md5: Some("abcdef0123456789abcdef0123456789".into()),
            title: Some("Deep Learning".into()),
            author: Some("Ian Goodfellow".into()),
            original_filename: Some("nested/original.pdf".into()),
            extension: Some("pdf".into()),
            local_path: None,
        };

        assert!(
            destination_path(Path::new("/tmp/output"), &record, FilenameMode::AaId, None)
                .unwrap()
                .ends_with("aa-1.pdf")
        );
        assert!(
            destination_path(Path::new("/tmp/output"), &record, FilenameMode::Md5, None)
                .unwrap()
                .ends_with("abcdef0123456789abcdef0123456789.pdf")
        );
        assert!(
            destination_path(Path::new("/tmp/output"), &record, FilenameMode::Flat, None)
                .unwrap()
                .ends_with("Deep Learning -- Ian Goodfellow.pdf")
        );
        assert!(
            destination_path(
                Path::new("/tmp/output"),
                &record,
                FilenameMode::Flat,
                Some("custom/name.pdf"),
            )
            .unwrap()
            .ends_with("custom_name.pdf")
        );
    }

    #[test]
    fn verifies_existing_file_md5() {
        let root = std::env::temp_dir().join(format!(
            "arcana-download-md5-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("file.bin");
        fs::write(&path, b"hello").unwrap();

        let expected = format!("{:x}", md5::compute(b"hello"));
        assert!(file_matches_md5(&path, &expected).unwrap());
        assert!(verify_file_md5(&path, &expected).is_ok());
        assert!(verify_file_md5(&path, "deadbeef").is_err());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn requests_download_url_from_api() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);

        let server = Server::http(address).unwrap();
        let (tx, rx) = mpsc::channel();

        let handle = thread::spawn(move || {
            let request = server.recv().unwrap();
            assert_eq!(request.method(), &Method::Get);
            assert!(
                request
                    .url()
                    .contains("md5=d6e1dc51a50726f00ec438af21952a45")
            );
            let response = Response::from_string(
                r#"{"download_url":"http://example.invalid/file.pdf","error":null}"#,
            )
            .with_status_code(200);
            request.respond(response).unwrap();
            tx.send(()).unwrap();
        });

        let client = Client::builder().build().unwrap();
        let url = request_fast_download_url(
            &client,
            &format!("http://{address}/dyn/api/fast_download.json"),
            "d6e1dc51a50726f00ec438af21952a45",
            "secret-key",
            None,
            None,
        )
        .unwrap();

        assert_eq!(url, "http://example.invalid/file.pdf");
        rx.recv().unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn surfaces_api_errors_with_md5_context() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);

        let server = Server::http(address).unwrap();
        let (tx, rx) = mpsc::channel();

        let handle = thread::spawn(move || {
            let request = server.recv().unwrap();
            let response =
                Response::from_string(r#"{"error":"invalid key"}"#).with_status_code(403);
            request.respond(response).unwrap();
            tx.send(()).unwrap();
        });

        let client = Client::builder().build().unwrap();
        let error = request_fast_download_url(
            &client,
            &format!("http://{address}/dyn/api/fast_download.json"),
            "d6e1dc51a50726f00ec438af21952a45",
            "bad-key",
            None,
            None,
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("d6e1dc51a50726f00ec438af21952a45"));
        assert!(error.contains("invalid key"));
        rx.recv().unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn resumes_from_partial_download() {
        let root = std::env::temp_dir().join(format!(
            "arcana-download-resume-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        let destination = root.join("book.pdf");
        let partial = root.join("book.pdf.part");
        fs::write(&partial, b"hello ").unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);

        let server = Server::http(address).unwrap();
        let (tx, rx) = mpsc::channel();

        let handle = thread::spawn(move || {
            let request = server.recv().unwrap();
            let range_header = request
                .headers()
                .iter()
                .find(|header| header.field.as_str().as_str().eq_ignore_ascii_case("Range"))
                .unwrap();
            assert_eq!(range_header.value.as_str(), "bytes=6-");
            let response = Response::from_data(b"world".to_vec()).with_status_code(206);
            request.respond(response).unwrap();
            tx.send(()).unwrap();
        });

        let client = Client::builder().build().unwrap();
        let transfer = download_to_path(
            &client,
            &format!("http://{address}/file.pdf"),
            &destination,
            false,
        )
        .unwrap();

        assert_eq!(
            transfer,
            DownloadTransferResult {
                resumed_partial: true,
                restarted_partial: false,
            }
        );
        assert_eq!(fs::read(&destination).unwrap(), b"hello world");
        assert!(!partial.exists());
        rx.recv().unwrap();
        handle.join().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn rejects_directory_destinations() {
        let root = std::env::temp_dir().join(format!(
            "arcana-download-dir-dest-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        let destination = root.join("existing-dir");
        fs::create_dir_all(&destination).unwrap();

        let client = Client::builder().build().unwrap();
        let error = download_to_path(&client, "http://127.0.0.1:1/unused", &destination, false)
            .unwrap_err()
            .to_string();
        assert!(error.contains("not a regular file"));

        let _ = fs::remove_dir_all(root);
    }
}
