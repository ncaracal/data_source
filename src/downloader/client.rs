use crate::downloader::types::{DownloadResult, DownloadTask};
use futures::stream::{self, StreamExt};
use reqwest::Client;
use std::path::Path;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::{debug, info};

pub struct Downloader {
    client: Client,
    concurrency: usize,
}

impl Downloader {
    pub fn new(concurrency: usize) -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(300))
            .build()
            .expect("Failed to create HTTP client");

        Self { client, concurrency }
    }

    /// Download multiple files concurrently
    pub async fn download_all(&self, tasks: Vec<DownloadTask>) -> Vec<DownloadResult> {
        stream::iter(tasks)
            .map(|task| self.download_one(task))
            .buffer_unordered(self.concurrency)
            .collect()
            .await
    }

    /// Download a single file
    async fn download_one(&self, task: DownloadTask) -> DownloadResult {
        // Check if file already exists (resume support)
        if task.output_path.exists() {
            debug!("Skipping existing file: {}", task.filename);
            return DownloadResult::Skipped(task.filename);
        }

        // Ensure parent directory exists
        if let Some(parent) = task.output_path.parent() {
            if let Err(e) = fs::create_dir_all(parent).await {
                return DownloadResult::Error(format!("Failed to create directory: {}", e));
            }
        }

        // Download file
        info!("Downloading: {}", task.url);
        match self.client.get(&task.url).send().await {
            Ok(response) => {
                if response.status() == reqwest::StatusCode::NOT_FOUND {
                    debug!("File not found on server: {}", task.url);
                    return DownloadResult::NotFound(task.filename);
                }

                if !response.status().is_success() {
                    return DownloadResult::Error(format!(
                        "HTTP error {}: {}",
                        response.status(),
                        task.url
                    ));
                }

                match response.bytes().await {
                    Ok(bytes) => {
                        // Write to temp file first, then rename
                        let temp_path = task.output_path.with_extension("tmp");
                        match fs::File::create(&temp_path).await {
                            Ok(mut file) => {
                                if let Err(e) = file.write_all(&bytes).await {
                                    let _ = fs::remove_file(&temp_path).await;
                                    return DownloadResult::Error(format!("Write failed: {}", e));
                                }
                                if let Err(e) = fs::rename(&temp_path, &task.output_path).await {
                                    let _ = fs::remove_file(&temp_path).await;
                                    return DownloadResult::Error(format!("Rename failed: {}", e));
                                }
                                info!("Downloaded: {}", task.filename);
                                DownloadResult::Success(task.output_path)
                            }
                            Err(e) => DownloadResult::Error(format!("Create file failed: {}", e)),
                        }
                    }
                    Err(e) => DownloadResult::Error(format!("Failed to read response: {}", e)),
                }
            }
            Err(e) => DownloadResult::Error(format!("Request failed: {}", e)),
        }
    }
}

#[allow(dead_code)]
/// Check if a file exists in the download folder
pub fn file_exists_in_folder(folder: &Path, filename: &str) -> bool {
    folder.join(filename).exists()
}
