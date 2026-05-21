use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HealthStatus {
    Ready,
    NotReady,
}

#[derive(Clone, Debug)]
pub struct CancellationToken {
    cancelled: Arc<std::sync::atomic::AtomicBool>,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self {
            cancelled: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub fn child_token(&self) -> Self {
        self.clone()
    }

    pub fn cancel(&self) {
        self.cancelled
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled
            .load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum JsonValue {
    Null,
    Bool(bool),
    Number(i64),
    String(String),
    Array(Vec<JsonValue>),
    Object(HashMap<String, JsonValue>),
}

impl JsonValue {
    pub fn string(value: impl Into<String>) -> Self {
        Self::String(value.into())
    }

    pub fn number(value: i64) -> Self {
        Self::Number(value)
    }

    pub fn object(entries: impl IntoIterator<Item = (impl Into<String>, JsonValue)>) -> Self {
        let mut map = HashMap::new();
        for (key, value) in entries {
            map.insert(key.into(), value);
        }
        Self::Object(map)
    }
}

#[derive(Clone, Debug)]
pub struct SystemHealth {
    overall: Arc<Mutex<HealthStatus>>,
    portnames: Arc<RwLock<HashMap<String, HealthStatus>>>,
    start: Instant,
    health_path: String,
    live_path: String,
}

impl SystemHealth {
    pub fn new(health_path: impl Into<String>, live_path: impl Into<String>) -> Self {
        Self {
            overall: Arc::new(Mutex::new(HealthStatus::NotReady)),
            portnames: Arc::new(RwLock::new(HashMap::new())),
            start: Instant::now(),
            health_path: health_path.into(),
            live_path: live_path.into(),
        }
    }

    pub fn set_overall(&self, status: HealthStatus) {
        *self.overall.lock().expect("system health poisoned") = status;
    }

    pub fn set_portname(&self, name: &str, status: HealthStatus) {
        self.portnames
            .write()
            .expect("portname health poisoned")
            .insert(name.to_string(), status);
    }

    pub fn get_health_status(&self) -> (bool, HashMap<String, String>) {
        let details = self
            .portnames
            .read()
            .expect("portname health poisoned")
            .iter()
            .map(|(name, status)| {
                (
                    name.clone(),
                    match status {
                        HealthStatus::Ready => "ready".to_string(),
                        HealthStatus::NotReady => "notready".to_string(),
                    },
                )
            })
            .collect::<HashMap<_, _>>();

        let healthy = if details.is_empty() {
            *self.overall.lock().expect("system health poisoned") == HealthStatus::Ready
        } else {
            details.values().all(|value| value == "ready")
        };

        (healthy, details)
    }

    pub fn uptime(&self) -> Duration {
        self.start.elapsed()
    }

    pub fn health_path(&self) -> &str {
        &self.health_path
    }

    pub fn live_path(&self) -> &str {
        &self.live_path
    }
}

#[derive(Clone, Debug)]
pub struct MetricsRegistry {
    exposition: Arc<Mutex<String>>,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self {
            exposition: Arc::new(Mutex::new(String::new())),
        }
    }

    pub fn set_exposition(&self, text: impl Into<String>) {
        *self.exposition.lock().expect("metrics registry poisoned") = text.into();
    }

    pub fn prometheus_expfmt(&self) -> String {
        self.exposition.lock().expect("metrics registry poisoned").clone()
    }
}

type EngineHandler = Arc<dyn Fn(JsonValue) -> Result<JsonValue, String> + Send + Sync>;

#[derive(Clone)]
pub struct EngineRoutes {
    routes: Arc<RwLock<HashMap<String, EngineHandler>>>,
}

impl std::fmt::Debug for EngineRoutes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let route_count = self.routes.read().expect("engine routes poisoned").len();
        f.debug_struct("EngineRoutes")
            .field("route_count", &route_count)
            .finish()
    }
}

impl EngineRoutes {
    pub fn new() -> Self {
        Self {
            routes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn register(
        &self,
        path: &str,
        handler: impl Fn(JsonValue) -> Result<JsonValue, String> + Send + Sync + 'static,
    ) {
        self.routes
            .write()
            .expect("engine routes poisoned")
            .insert(path.to_string(), Arc::new(handler));
    }

    pub fn call(&self, path: &str, payload: JsonValue) -> Option<Result<JsonValue, String>> {
        self.routes
            .read()
            .expect("engine routes poisoned")
            .get(path)
            .cloned()
            .map(|handler| handler(payload))
    }
}

#[derive(Clone, Debug)]
pub struct DistributedRuntime {
    system_health: Arc<SystemHealth>,
    metrics: MetricsRegistry,
    engine_routes: EngineRoutes,
}

impl DistributedRuntime {
    pub fn new(system_health: SystemHealth) -> Self {
        Self {
            system_health: Arc::new(system_health),
            metrics: MetricsRegistry::new(),
            engine_routes: EngineRoutes::new(),
        }
    }

    pub fn system_health(&self) -> Arc<SystemHealth> {
        self.system_health.clone()
    }

    pub fn metrics(&self) -> &MetricsRegistry {
        &self.metrics
    }

    pub fn engine_routes(&self) -> &EngineRoutes {
        &self.engine_routes
    }
}

#[derive(Clone, Debug)]
pub struct DiscoveryMetadata {
    values: HashMap<String, String>,
}

impl DiscoveryMetadata {
    pub fn new(values: HashMap<String, String>) -> Self {
        Self { values }
    }

    pub fn as_json(&self) -> JsonValue {
        JsonValue::Object(
            self.values
                .iter()
                .map(|(key, value)| (key.clone(), JsonValue::string(value.clone())))
                .collect(),
        )
    }
}

pub struct SystemStatusServerInfo {
    pub socket_addr: SocketAddr,
    pub handle: Option<Arc<JoinHandle<()>>>,
}

impl SystemStatusServerInfo {
    pub fn new(socket_addr: SocketAddr, handle: Option<JoinHandle<()>>) -> Self {
        Self {
            socket_addr,
            handle: handle.map(Arc::new),
        }
    }

    pub fn address(&self) -> String {
        self.socket_addr.to_string()
    }

    pub fn hostname(&self) -> String {
        self.socket_addr.ip().to_string()
    }

    pub fn port(&self) -> u16 {
        self.socket_addr.port()
    }
}

impl Clone for SystemStatusServerInfo {
    fn clone(&self) -> Self {
        Self {
            socket_addr: self.socket_addr,
            handle: self.handle.clone(),
        }
    }
}

pub struct SystemStatusState {
    root_drt: Arc<DistributedRuntime>,
    discovery_metadata: Option<Arc<RwLock<DiscoveryMetadata>>>,
}

impl SystemStatusState {
    pub fn new(
        drt: Arc<DistributedRuntime>,
        discovery_metadata: Option<Arc<RwLock<DiscoveryMetadata>>>,
    ) -> Result<Self, String> {
        Ok(Self {
            root_drt: drt,
            discovery_metadata,
        })
    }

    pub fn drt(&self) -> &DistributedRuntime {
        &self.root_drt
    }

    pub fn discovery_metadata(&self) -> Option<&Arc<RwLock<DiscoveryMetadata>>> {
        self.discovery_metadata.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct HttpResponse {
    pub status: u16,
    pub body: JsonValue,
}

pub fn health_handler(state: &SystemStatusState) -> HttpResponse {
    let (healthy, portnames) = state.drt().system_health().get_health_status();
    HttpResponse {
        status: if healthy { 200 } else { 503 },
        body: JsonValue::object([
            (
                "status",
                JsonValue::string(if healthy { "ready" } else { "notready" }),
            ),
            (
                "uptime",
                JsonValue::number(state.drt().system_health().uptime().as_secs() as i64),
            ),
            (
                "portnames",
                JsonValue::Object(
                    portnames
                        .into_iter()
                        .map(|(key, value)| (key, JsonValue::string(value)))
                        .collect(),
                ),
            ),
        ]),
    }
}

pub fn metrics_handler(state: &SystemStatusState) -> HttpResponse {
    HttpResponse {
        status: 200,
        body: JsonValue::string(state.drt().metrics().prometheus_expfmt()),
    }
}

pub fn metadata_handler(state: &SystemStatusState) -> HttpResponse {
    match state.discovery_metadata() {
        Some(metadata) => HttpResponse {
            status: 200,
            body: metadata.read().expect("metadata poisoned").as_json(),
        },
        None => HttpResponse {
            status: 404,
            body: JsonValue::string("Discovery metadata not available"),
        },
    }
}

pub fn engine_route_handler(state: &SystemStatusState, path: &str, body: Option<JsonValue>) -> HttpResponse {
    match state
        .drt()
        .engine_routes()
        .call(path, body.unwrap_or_else(|| JsonValue::Object(HashMap::new())))
    {
        Some(Ok(value)) => HttpResponse { status: 200, body: value },
        Some(Err(err)) => HttpResponse {
            status: 500,
            body: JsonValue::string(err),
        },
        None => HttpResponse {
            status: 404,
            body: JsonValue::string("Route not found"),
        },
    }
}

pub fn spawn_system_status_server(
    host: &str,
    port: u16,
    cancel_token: CancellationToken,
    drt: Arc<DistributedRuntime>,
    discovery_metadata: Option<Arc<RwLock<DiscoveryMetadata>>>,
) -> Result<(SocketAddr, JoinHandle<()>), String> {
    let state = Arc::new(SystemStatusState::new(drt, discovery_metadata)?);
    let child = cancel_token.child_token();
    let address = format!("{}:{}", host, port)
        .parse::<SocketAddr>()
        .map_err(|err| err.to_string())?;
    let handle = thread::spawn(move || {
        while !child.is_cancelled() {
            let _ = state.drt().system_health().health_path();
            thread::sleep(Duration::from_millis(10));
        }
    });
    Ok((address, handle))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_handler_reports_readiness_and_uptime() {
        let drt = Arc::new(DistributedRuntime::new(SystemHealth::new("/health", "/live")));
        drt.system_health().set_portname("default.generate.v1", HealthStatus::Ready);
        let state = SystemStatusState::new(drt, None).expect("state should build");
        let response = health_handler(&state);
        assert_eq!(response.status, 200);
        match response.body {
            JsonValue::Object(map) => {
                assert_eq!(map.get("status"), Some(&JsonValue::string("ready")));
            }
            _ => panic!("expected object response"),
        }
    }

    #[test]
    fn metadata_handler_returns_404_when_missing() {
        let drt = Arc::new(DistributedRuntime::new(SystemHealth::new("/health", "/live")));
        let state = SystemStatusState::new(drt, None).expect("state should build");
        let response = metadata_handler(&state);
        assert_eq!(response.status, 404);
    }

    #[test]
    fn engine_route_handler_dispatches_registered_route() {
        let drt = Arc::new(DistributedRuntime::new(SystemHealth::new("/health", "/live")));
        drt.engine_routes().register("reload", |_| {
            Ok(JsonValue::object([("status", JsonValue::string("ok"))]))
        });
        let state = SystemStatusState::new(drt, None).expect("state should build");
        let response = engine_route_handler(&state, "reload", None);
        assert_eq!(response.status, 200);
    }

    #[test]
    fn spawn_server_returns_handle_and_stops_on_cancel() {
        let drt = Arc::new(DistributedRuntime::new(SystemHealth::new("/health", "/live")));
        let token = CancellationToken::new();
        let (addr, handle) = spawn_system_status_server("127.0.0.1", 8080, token.clone(), drt, None)
            .expect("spawn should succeed");
        assert_eq!(addr.to_string(), "127.0.0.1:8080");
        token.cancel();
        handle.join().expect("server thread should stop cleanly");
    }
}