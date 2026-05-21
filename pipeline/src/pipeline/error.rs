use std::fmt;

use anyhow::Error;

// ── TwoPartCodecError ─────────────────────────────────────────────────────────

/// Low-level framing errors for the two-part (header + payload) codec used by
/// the TCP request plane.
#[derive(Debug)]
pub enum TwoPartCodecError {
    /// Underlying I/O read/write failed.
    Io(std::io::Error),
    /// Message byte length exceeds the permitted maximum.
    MessageTooLarge(usize),
    /// Frame structure does not match the two-part protocol.
    InvalidMessage(String),
    /// Checksum in the received frame does not match the computed value.
    ChecksumMismatch,
}

impl fmt::Display for TwoPartCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "two-part codec I/O error: {e}"),
            Self::MessageTooLarge(n) => write!(f, "two-part codec: message too large ({n} bytes)"),
            Self::InvalidMessage(msg) => write!(f, "two-part codec: invalid message: {msg}"),
            Self::ChecksumMismatch => write!(f, "two-part codec: checksum mismatch"),
        }
    }
}

impl std::error::Error for TwoPartCodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for TwoPartCodecError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

// ── PipelineError ─────────────────────────────────────────────────────────────

/// The unified error type for the pipeline module.
///
/// Each variant corresponds to a distinct **layer** of the pipeline system,
/// making it possible for callers to determine *where* a failure occurred
/// rather than just *that* it occurred.
///
/// Layer taxonomy:
/// - **Graph assembly** – `EdgeAlreadySet`, `NoEdge`, `NoNetworkEdge`
/// - **Async lifecycle** – `DetachedStreamReceiver`, `DetachedStreamSender`
/// - **Protocol codec** – `SerializationError`, `DeserializationError`,
///   `TwoPartCodec`, `SerdeJsonError`
/// - **Remote control / connection** – `ControlPlaneRequestError`,
///   `ConnectionFailed`, `InvalidPortnameFormat`
/// - **Business execution** – `GenerateError`
/// - **External dependencies** – NATS variants, `KeyValueError`,
///   `LocalIpAddressError`, `PrometheusError`
/// - **Capacity / degradation** – `ServiceOverloaded`
/// - **Evolution fallback** – `Generic`, `NatsError`
#[derive(Debug)]
pub enum PipelineError {
    // ── Graph assembly ────────────────────────────────────────────────────────

    /// A `Source`'s edge has already been set; a second `set_edge` call was
    /// rejected to preserve deterministic graph topology.
    EdgeAlreadySet,

    /// A `Source` attempted to forward data but no downstream `Edge` has been
    /// registered yet.
    NoEdge,

    /// A segment / network sink attempted to write data but its network edge
    /// (egress port) has not been bound yet.
    NoNetworkEdge,

    // ── Async lifecycle ───────────────────────────────────────────────────────

    /// The response arrived but the original caller's `oneshot::Receiver` is
    /// gone (task cancelled or dropped).
    DetachedStreamReceiver,

    /// The caller is still waiting for a response but the `oneshot::Sender`
    /// that should deliver it has been dropped.
    DetachedStreamSender,

    // ── Protocol codec ────────────────────────────────────────────────────────

    /// Failed to serialize a pipeline object into bytes before sending.
    SerializationError(String),

    /// Failed to deserialize received bytes back into a pipeline object.
    DeserializationError(String),

    /// The two-part (header + payload) binary framing protocol failed.
    TwoPartCodec(TwoPartCodecError),

    /// JSON serialization / deserialization failed.
    SerdeJsonError(serde_json::Error),

    // ── Remote control / connection ───────────────────────────────────────────

    /// A request to the control plane failed before the business engine was
    /// even reached.
    ControlPlaneRequestError(String),

    /// The streaming connection (response plane or data plane) could not be
    /// established.
    ConnectionFailed(String),

    /// The portname address string does not match the required format
    /// `namespace/servicegroup/portname`.
    InvalidPortnameFormat,

    // ── Business execution ────────────────────────────────────────────────────

    /// The real business engine's `generate()` call returned an error.
    GenerateError(Error),

    // ── NATS / message-system dependencies ───────────────────────────────────

    /// Failed to connect to the NATS server.
    NatsConnectError(Box<dyn std::error::Error + Send + Sync>),

    /// A NATS request/response transaction failed.
    NatsRequestError(Box<dyn std::error::Error + Send + Sync>),

    /// Failed to retrieve an existing NATS stream.
    NatsGetStreamError(Box<dyn std::error::Error + Send + Sync>),

    /// Failed to create a new NATS stream.
    NatsCreateStreamError(Box<dyn std::error::Error + Send + Sync>),

    /// Failed to create or access a NATS consumer.
    NatsConsumerError(Box<dyn std::error::Error + Send + Sync>),

    /// A NATS batch pull operation failed.
    NatsBatchError(Box<dyn std::error::Error + Send + Sync>),

    /// Failed to publish a message to a NATS subject.
    NatsPublishError(Box<dyn std::error::Error + Send + Sync>),

    /// Failed to subscribe to a NATS subject.
    NatsSubscriberError(Box<dyn std::error::Error + Send + Sync>),

    /// Generic NATS error not (yet) covered by a more specific variant.
    NatsError(Box<dyn std::error::Error + Send + Sync>),

    // ── Runtime / environment dependencies ───────────────────────────────────

    /// Could not determine the local IP address (needed to build
    /// `ConnectionInfo` for response-plane callbacks).
    LocalIpAddressError(String),

    /// A Prometheus metrics registration or operation failed.
    PrometheusError(String),

    /// A NATS KV store operation failed.
    /// Fields: `(error_description, bucket_name)`.
    KeyValueError(String, String),

    // ── Capacity / degradation ────────────────────────────────────────────────

    /// All available instances are currently busy; the caller should retry,
    /// shed load, or trigger scale-out.
    ServiceOverloaded(String),

    // ── Evolution fallback ────────────────────────────────────────────────────

    /// Catch-all for error scenarios not yet modelled as a dedicated variant.
    Generic(String),
}

impl fmt::Display for PipelineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EdgeAlreadySet => write!(f, "pipeline: edge already set on this source"),
            Self::NoEdge => write!(f, "pipeline: source has no downstream edge"),
            Self::NoNetworkEdge => write!(f, "pipeline: segment sink has no network edge"),
            Self::DetachedStreamReceiver => {
                write!(f, "pipeline: stream receiver detached (caller cancelled?)")
            }
            Self::DetachedStreamSender => {
                write!(f, "pipeline: stream sender detached (response path dropped)")
            }
            Self::SerializationError(msg) => write!(f, "pipeline: serialization failed: {msg}"),
            Self::DeserializationError(msg) => {
                write!(f, "pipeline: deserialization failed: {msg}")
            }
            Self::TwoPartCodec(e) => write!(f, "pipeline: {e}"),
            Self::SerdeJsonError(e) => write!(f, "pipeline: JSON error: {e}"),
            Self::ControlPlaneRequestError(msg) => {
                write!(f, "pipeline: control-plane request failed: {msg}")
            }
            Self::ConnectionFailed(msg) => {
                write!(f, "pipeline: streaming connection failed: {msg}")
            }
            Self::InvalidPortnameFormat => write!(
                f,
                "pipeline: invalid portname format (expected namespace/servicegroup/portname)"
            ),
            Self::GenerateError(e) => write!(f, "pipeline: engine generate error: {e}"),
            Self::NatsConnectError(e) => write!(f, "pipeline: NATS connect error: {e}"),
            Self::NatsRequestError(e) => write!(f, "pipeline: NATS request error: {e}"),
            Self::NatsGetStreamError(e) => write!(f, "pipeline: NATS get-stream error: {e}"),
            Self::NatsCreateStreamError(e) => write!(f, "pipeline: NATS create-stream error: {e}"),
            Self::NatsConsumerError(e) => write!(f, "pipeline: NATS consumer error: {e}"),
            Self::NatsBatchError(e) => write!(f, "pipeline: NATS batch error: {e}"),
            Self::NatsPublishError(e) => write!(f, "pipeline: NATS publish error: {e}"),
            Self::NatsSubscriberError(e) => write!(f, "pipeline: NATS subscribe error: {e}"),
            Self::NatsError(e) => write!(f, "pipeline: NATS error: {e}"),
            Self::LocalIpAddressError(msg) => {
                write!(f, "pipeline: local IP address error: {msg}")
            }
            Self::PrometheusError(msg) => write!(f, "pipeline: Prometheus error: {msg}"),
            Self::KeyValueError(msg, bucket) => {
                write!(f, "pipeline: KV error on bucket '{bucket}': {msg}")
            }
            Self::ServiceOverloaded(msg) => write!(f, "pipeline: service overloaded: {msg}"),
            Self::Generic(msg) => write!(f, "pipeline error: {msg}"),
        }
    }
}

impl std::error::Error for PipelineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::TwoPartCodec(e) => Some(e),
            Self::SerdeJsonError(e) => Some(e),
            Self::GenerateError(e) => Some(e.as_ref()),
            _ => None,
        }
    }
}

// ── Conversions ───────────────────────────────────────────────────────────────

impl From<TwoPartCodecError> for PipelineError {
    fn from(e: TwoPartCodecError) -> Self {
        Self::TwoPartCodec(e)
    }
}

impl From<serde_json::Error> for PipelineError {
    fn from(e: serde_json::Error) -> Self {
        Self::SerdeJsonError(e)
    }
}

impl From<anyhow::Error> for PipelineError {
    fn from(e: anyhow::Error) -> Self {
        // Try to downcast to a concrete `PipelineError` first; fall back to
        // wrapping as `GenerateError` so callers retain the full error chain.
        match e.downcast::<PipelineError>() {
            Ok(pe) => pe,
            Err(e) => Self::GenerateError(e),
        }
    }
}

// ── PipelineErrorExt ──────────────────────────────────────────────────────────

/// Extension methods on [`anyhow::Error`] that allow callers to recover
/// structured [`PipelineError`] semantics from an opaque error chain.
pub trait PipelineErrorExt {
    /// Attempt to downcast the error to `PipelineError`.
    /// Returns `Ok(PipelineError)` on success, `Err(self)` otherwise.
    fn try_into_pipeline_error(self) -> Result<PipelineError, anyhow::Error>;

    /// Returns `either::Either::Left(PipelineError)` when the downcast
    /// succeeds, or `either::Either::Right(anyhow::Error)` otherwise.
    ///
    /// Useful in match arms where both branches need to be handled differently.
    fn either_pipeline_error(
        self,
    ) -> Result<PipelineError, anyhow::Error>;
}

impl PipelineErrorExt for anyhow::Error {
    fn try_into_pipeline_error(self) -> Result<PipelineError, anyhow::Error> {
        self.downcast::<PipelineError>()
    }

    fn either_pipeline_error(
        self,
    ) -> Result<PipelineError, anyhow::Error> {
        self.downcast::<PipelineError>()
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_variants() {
        assert!(PipelineError::EdgeAlreadySet.to_string().contains("edge already set"));
        assert!(PipelineError::NoEdge.to_string().contains("no downstream edge"));
        assert!(PipelineError::NoNetworkEdge.to_string().contains("no network edge"));
        assert!(PipelineError::DetachedStreamReceiver
            .to_string()
            .contains("receiver detached"));
        assert!(PipelineError::DetachedStreamSender
            .to_string()
            .contains("sender detached"));
        assert!(PipelineError::SerializationError("oops".into())
            .to_string()
            .contains("oops"));
        assert!(PipelineError::DeserializationError("bad bytes".into())
            .to_string()
            .contains("bad bytes"));
        assert!(PipelineError::ConnectionFailed("refused".into())
            .to_string()
            .contains("refused"));
        assert!(PipelineError::InvalidPortnameFormat
            .to_string()
            .contains("namespace/servicegroup/portname"));
        assert!(PipelineError::ServiceOverloaded("all busy".into())
            .to_string()
            .contains("all busy"));
        assert!(PipelineError::Generic("unknown".into())
            .to_string()
            .contains("unknown"));
        assert!(PipelineError::KeyValueError("not found".into(), "my-bucket".into())
            .to_string()
            .contains("my-bucket"));
    }

    #[test]
    fn two_part_codec_error_display() {
        let e = TwoPartCodecError::MessageTooLarge(1024);
        assert!(e.to_string().contains("1024"));

        let e = TwoPartCodecError::InvalidMessage("bad frame".into());
        assert!(e.to_string().contains("bad frame"));

        let e = TwoPartCodecError::ChecksumMismatch;
        assert!(e.to_string().contains("checksum"));
    }

    #[test]
    fn from_two_part_codec_error() {
        let codec_err = TwoPartCodecError::ChecksumMismatch;
        let pe: PipelineError = codec_err.into();
        assert!(matches!(pe, PipelineError::TwoPartCodec(_)));
    }

    #[test]
    fn from_serde_json_error() {
        let json_err: serde_json::Error =
            serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
        let pe: PipelineError = json_err.into();
        assert!(matches!(pe, PipelineError::SerdeJsonError(_)));
    }

    #[test]
    fn anyhow_downcast_pipeline_error() {
        let original = PipelineError::ServiceOverloaded("test".into());
        let anyhow_err: anyhow::Error = anyhow::Error::new(original);
        let recovered = anyhow_err.try_into_pipeline_error().unwrap();
        assert!(matches!(recovered, PipelineError::ServiceOverloaded(_)));
    }

    #[test]
    fn anyhow_downcast_non_pipeline_error() {
        let anyhow_err: anyhow::Error =
            anyhow::Error::new(std::io::Error::new(std::io::ErrorKind::Other, "io"));
        let result = anyhow_err.try_into_pipeline_error();
        assert!(result.is_err()); // downcast failed, original error returned
    }
}
