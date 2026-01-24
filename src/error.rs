use thiserror::Error;

#[derive(Error, Debug)]
#[allow(dead_code)]
pub enum AppError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Parquet error: {0}")]
    Polars(#[from] polars::error::PolarsError),

    #[error("ZIP extraction failed: {0}")]
    Zip(#[from] zip::result::ZipError),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("File not found on server: {0}")]
    NotFound(String),

    #[error("Unsupported exchange: {0}")]
    UnsupportedExchange(String),

    #[error("Parse error: {0}")]
    Parse(String),
}

pub type Result<T> = std::result::Result<T, AppError>;
