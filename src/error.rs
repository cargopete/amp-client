use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("transport: {0}")]
    Transport(#[from] tonic::transport::Error),

    #[error("grpc status: {0}")]
    Status(#[from] tonic::Status),

    #[error("flight: {0}")]
    Flight(#[from] arrow_flight::error::FlightError),

    #[error("arrow: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),

    #[error("auth: {0}")]
    Auth(String),

    #[error("config: {0}")]
    Config(String),
}

pub type Result<T> = std::result::Result<T, Error>;
