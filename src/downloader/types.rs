use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct DownloadTask {
    pub url: String,
    pub output_path: PathBuf,
    pub filename: String,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum DownloadResult {
    Success(PathBuf),
    Skipped(String),
    NotFound(String),
    Error(String),
}
