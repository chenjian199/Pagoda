use std::convert::Infallible;
use std::fmt;
use std::str::FromStr;

#[path = "protocols/annotated.rs"]
pub mod annotated;
#[path = "protocols/maybe_error.rs"]
pub mod maybe_error;

pub use annotated::{Annotated, AnnotationsProvider};
pub use maybe_error::MaybeError;

pub type LeaseId = i64;
pub const PORTNAME_SCHEME: &str = "dyn://";
pub const DEFAULT_NAMESPACE: &str = "NS";
pub const DEFAULT_SERVICEGROUP: &str = "C";
pub const DEFAULT_PORTNAME: &str = "E";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceGroup {
    pub name: String,
    pub namespace: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PortnameId {
    pub namespace: String,
    pub servicegroup: String,
    pub name: String,
}

impl Default for PortnameId {
    fn default() -> Self {
        Self {
            namespace: DEFAULT_NAMESPACE.to_string(),
            servicegroup: DEFAULT_SERVICEGROUP.to_string(),
            name: DEFAULT_PORTNAME.to_string(),
        }
    }
}

impl PortnameId {
    pub fn as_url(&self) -> String {
        format!("{}{}.{}.{}", PORTNAME_SCHEME, self.namespace, self.servicegroup, self.name)
    }
}

impl fmt::Display for PortnameId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}/{}", self.namespace, self.servicegroup, self.name)
    }
}

impl From<&str> for PortnameId {
    fn from(value: &str) -> Self {
        let trimmed = value
            .trim()
            .trim_start_matches(PORTNAME_SCHEME)
            .trim_matches(|c| c == '/' || c == '.');
        let segments = trimmed
            .split(['.', '/'])
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>();

        match segments.len() {
            0 => Self::default(),
            1 => Self {
                namespace: DEFAULT_NAMESPACE.to_string(),
                servicegroup: segments[0].to_string(),
                name: DEFAULT_PORTNAME.to_string(),
            },
            2 => Self {
                namespace: segments[0].to_string(),
                servicegroup: segments[1].to_string(),
                name: DEFAULT_PORTNAME.to_string(),
            },
            _ => Self {
                namespace: segments[0].to_string(),
                servicegroup: segments[1].to_string(),
                name: segments[2..].join("_"),
            },
        }
    }
}

impl FromStr for PortnameId {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::from(s))
    }
}

impl PartialEq<Vec<&str>> for PortnameId {
    fn eq(&self, other: &Vec<&str>) -> bool {
        other.len() == 3
            && self.namespace == other[0]
            && self.servicegroup == other[1]
            && self.name == other[2]
    }
}

impl PartialEq<PortnameId> for Vec<&str> {
    fn eq(&self, other: &PortnameId) -> bool {
        other == self
    }
}

impl PartialEq<[&str; 3]> for PortnameId {
    fn eq(&self, other: &[&str; 3]) -> bool {
        self.namespace == other[0] && self.servicegroup == other[1] && self.name == other[2]
    }
}

impl PartialEq<PortnameId> for [&str; 3] {
    fn eq(&self, other: &PortnameId) -> bool {
        other == self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn portname_id_parses_short_and_full_forms() {
        assert_eq!(PortnameId::from("worker"), vec!["NS", "worker", "E"]);
        assert_eq!(PortnameId::from("llm.generate"), ["llm", "generate", "E"]);
        assert_eq!(
            PortnameId::from("dyn://default/prefill/v1"),
            ["default", "prefill", "v1"]
        );
        assert_eq!(
            PortnameId::from("ns.comp.portname.subpath"),
            ["ns", "comp", "portname_subpath"]
        );
    }

    #[test]
    fn root_reexports_stream_protocol_types() {
        let frame = Annotated::from_data("ok".to_string());
        assert!(frame.is_ok());
        assert_eq!(frame.into_result(), Ok(Some("ok".to_string())));
    }
}