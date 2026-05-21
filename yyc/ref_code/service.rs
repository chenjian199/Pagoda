use std::collections::VecDeque;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub subject: String,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NatsStatsMetrics {
    pub average_processing_time: u64,
    pub last_error: String,
    pub num_errors: u64,
    pub num_requests: u64,
    pub processing_time: u64,
    pub queue_group: String,
    pub data: String,
}

impl NatsStatsMetrics {
    pub fn decode<T>(&self, decoder: impl FnOnce(&str) -> Result<T, String>) -> Result<T, String> {
        decoder(&self.data)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortnameInfo {
    pub name: String,
    pub subject: String,
    pub data: Option<NatsStatsMetrics>,
}

impl PortnameInfo {
    pub fn id(&self) -> Result<i64, String> {
        let suffix = self
            .subject
            .rsplit('-')
            .next()
            .ok_or_else(|| "subject does not contain instance suffix".to_string())?;
        i64::from_str_radix(suffix, 16).map_err(|err| err.to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceInfo {
    pub name: String,
    pub id: String,
    pub version: String,
    pub started: String,
    pub portnames: Vec<PortnameInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceSet {
    services: Vec<ServiceInfo>,
}

impl ServiceSet {
    pub fn new(services: Vec<ServiceInfo>) -> Self {
        Self { services }
    }

    pub fn into_portnames(self) -> impl Iterator<Item = PortnameInfo> {
        self.services.into_iter().flat_map(|service| service.portnames)
    }

    pub fn services(&self) -> &[ServiceInfo] {
        &self.services
    }
}

pub trait NatsClientBackend {
    fn request(&self, subject: &str, payload: &[u8]) -> Result<Message, String>;
    fn scrape_service(&self, service_name: &str) -> Result<Vec<Message>, String>;
}

pub struct ServiceClient<B> {
    nats_client: B,
}

impl<B> ServiceClient<B>
where
    B: NatsClientBackend,
{
    pub fn new(nats_client: B) -> Self {
        Self { nats_client }
    }

    pub fn unary(&self, subject: impl Into<String>, payload: impl Into<Vec<u8>>) -> Result<Message, String> {
        let subject = subject.into();
        self.nats_client.request(&subject, &payload.into())
    }

    pub fn collect_services(
        &self,
        service_name: &str,
        timeout: Duration,
        decoder: impl Fn(&[u8]) -> Result<ServiceInfo, String>,
    ) -> Result<ServiceSet, String> {
        let mut services = Vec::new();
        let deadline = Instant::now() + timeout;
        let mut stream = VecDeque::from(self.nats_client.scrape_service(service_name)?);

        while Instant::now() < deadline {
            let Some(message) = stream.pop_front() else {
                break;
            };
            if message.payload.is_empty() {
                continue;
            }
            if let Ok(service) = decoder(&message.payload) {
                services.push(service);
            }
        }

        Ok(ServiceSet::new(services))
    }
}

pub const PROJECT_NAME: &str = "Pagoda";

pub fn build_nats_service(
    service_name: &str,
    description: Option<String>,
) -> Result<String, String> {
    let description = description.unwrap_or_else(|| format!("{} service {}", PROJECT_NAME, service_name));
    Ok(format!("{}|{}", service_name, description))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct MockNatsBackend {
        unary: Mutex<VecDeque<Message>>,
        scraped: Mutex<Vec<Message>>,
    }

    impl MockNatsBackend {
        fn new(unary: Vec<Message>, scraped: Vec<Message>) -> Self {
            Self {
                unary: Mutex::new(unary.into()),
                scraped: Mutex::new(scraped),
            }
        }
    }

    impl NatsClientBackend for MockNatsBackend {
        fn request(&self, _subject: &str, _payload: &[u8]) -> Result<Message, String> {
            self.unary
                .lock()
                .expect("unary queue poisoned")
                .pop_front()
                .ok_or_else(|| "no unary response".to_string())
        }

        fn scrape_service(&self, _service_name: &str) -> Result<Vec<Message>, String> {
            Ok(self.scraped.lock().expect("scrape queue poisoned").clone())
        }
    }

    fn decode_service(payload: &[u8]) -> Result<ServiceInfo, String> {
        let text = std::str::from_utf8(payload).map_err(|err| err.to_string())?;
        let parts = text.split('|').collect::<Vec<_>>();
        if parts.len() != 5 {
            return Err("invalid payload".to_string());
        }
        Ok(ServiceInfo {
            name: parts[0].to_string(),
            id: parts[1].to_string(),
            version: parts[2].to_string(),
            started: parts[3].to_string(),
            portnames: vec![PortnameInfo {
                name: parts[4].to_string(),
                subject: "default.generate-ab".to_string(),
                data: None,
            }],
        })
    }

    #[test]
    fn unary_returns_single_message() {
        let client = ServiceClient::new(MockNatsBackend::new(
            vec![Message {
                subject: "reply".to_string(),
                payload: b"ok".to_vec(),
            }],
            vec![],
        ));
        let response = client.unary("foo", b"payload".to_vec()).expect("unary should work");
        assert_eq!(response.payload, b"ok".to_vec());
    }

    #[test]
    fn collect_services_skips_invalid_payloads() {
        let scraped = vec![
            Message {
                subject: "srv".to_string(),
                payload: vec![],
            },
            Message {
                subject: "srv".to_string(),
                payload: b"bad".to_vec(),
            },
            Message {
                subject: "srv".to_string(),
                payload: b"worker|id-1|v1|now|generate".to_vec(),
            },
        ];
        let client = ServiceClient::new(MockNatsBackend::new(vec![], scraped));
        let services = client
            .collect_services("worker", Duration::from_millis(30), decode_service)
            .expect("collect should work");
        assert_eq!(services.services().len(), 1);
        assert_eq!(services.services()[0].name, "worker");
    }

    #[test]
    fn portname_info_decodes_hex_suffix() {
        let info = PortnameInfo {
            name: "generate".to_string(),
            subject: "default.generate-ff".to_string(),
            data: None,
        };
        assert_eq!(info.id(), Ok(255));
    }

    #[test]
    fn build_nats_service_uses_default_description() {
        let built = build_nats_service("worker.generate", None).expect("build should work");
        assert!(built.contains("Pagoda service worker.generate"));
    }
}