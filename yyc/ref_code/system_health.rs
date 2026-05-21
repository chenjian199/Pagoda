use std::collections::HashMap;
use std::sync::{mpsc, Arc, Condvar, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HealthStatus {
    Ready,
    NotReady,
}

impl HealthStatus {
    pub fn as_http_text(&self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::NotReady => "notready",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Instance {
    pub namespace: String,
    pub servicegroup: String,
    pub portname: String,
    pub instance_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JsonValue {
    Null,
    Bool(bool),
    Number(i64),
    String(String),
    Object(HashMap<String, JsonValue>),
}

impl JsonValue {
    pub fn string(value: impl Into<String>) -> Self {
        Self::String(value.into())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HealthCheckTarget {
    pub instance: Instance,
    pub payload: JsonValue,
}

#[derive(Debug)]
pub struct PortnameSignal {
    generation: Mutex<u64>,
    condvar: Condvar,
}

impl PortnameSignal {
    pub fn new() -> Self {
        Self {
            generation: Mutex::new(0),
            condvar: Condvar::new(),
        }
    }

    pub fn notify(&self) {
        let mut generation = self.generation.lock().expect("signal poisoned");
        *generation += 1;
        self.condvar.notify_all();
    }
}

#[derive(Clone, Debug)]
pub struct Gauge {
    value: Arc<Mutex<f64>>,
}

impl Gauge {
    pub fn new() -> Self {
        Self {
            value: Arc::new(Mutex::new(0.0)),
        }
    }

    pub fn set(&self, value: f64) {
        *self.value.lock().expect("gauge poisoned") = value;
    }

    pub fn get(&self) -> f64 {
        *self.value.lock().expect("gauge poisoned")
    }
}

pub struct SystemHealth {
    system_health: Mutex<HealthStatus>,
    portname_health: Arc<RwLock<HashMap<String, HealthStatus>>>,
    health_check_targets: Arc<RwLock<HashMap<String, HealthCheckTarget>>>,
    health_check_notifiers: Arc<RwLock<HashMap<String, Arc<PortnameSignal>>>>,
    new_portname_tx: mpsc::Sender<String>,
    new_portname_rx: Arc<Mutex<Option<mpsc::Receiver<String>>>>,
    use_portname_health_status: Vec<String>,
    health_path: String,
    live_path: String,
    start_time: Instant,
    uptime_gauge: OnceLock<Gauge>,
}

impl SystemHealth {
    pub fn new(
        starting_health_status: HealthStatus,
        use_portname_health_status: Vec<String>,
        health_path: String,
        live_path: String,
    ) -> Self {
        let mut portname_health = HashMap::new();
        for portname in &use_portname_health_status {
            portname_health.insert(portname.clone(), starting_health_status.clone());
        }
        let (tx, rx) = mpsc::channel();

        Self {
            system_health: Mutex::new(starting_health_status),
            portname_health: Arc::new(RwLock::new(portname_health)),
            health_check_targets: Arc::new(RwLock::new(HashMap::new())),
            health_check_notifiers: Arc::new(RwLock::new(HashMap::new())),
            new_portname_tx: tx,
            new_portname_rx: Arc::new(Mutex::new(Some(rx))),
            use_portname_health_status,
            health_path,
            live_path,
            start_time: Instant::now(),
            uptime_gauge: OnceLock::new(),
        }
    }

    pub fn set_health_status(&self, status: HealthStatus) {
        *self.system_health.lock().expect("system health poisoned") = status;
    }

    pub fn set_portname_health_status(&self, portname: &str, status: HealthStatus) {
        self.portname_health
            .write()
            .expect("portname health poisoned")
            .insert(portname.to_string(), status);
    }

    pub fn get_health_status(&self) -> (bool, HashMap<String, String>) {
        let states = self
            .portname_health
            .read()
            .expect("portname health poisoned")
            .clone();
        let summary = states
            .iter()
            .map(|(name, status)| (name.clone(), status.as_http_text().to_string()))
            .collect::<HashMap<_, _>>();

        let target_names = if !self.use_portname_health_status.is_empty() {
            self.use_portname_health_status.clone()
        } else {
            self.get_health_check_portnames()
        };

        if !target_names.is_empty() {
            let all_ready = target_names.iter().all(|name| {
                states
                    .get(name)
                    .map(|status| *status == HealthStatus::Ready)
                    .unwrap_or(false)
            });
            return (all_ready, summary);
        }

        (
            *self.system_health.lock().expect("system health poisoned") == HealthStatus::Ready,
            summary,
        )
    }

    pub fn register_health_check_target(
        &self,
        portname_subject: &str,
        instance: Instance,
        payload: JsonValue,
    ) {
        let mut targets = self
            .health_check_targets
            .write()
            .expect("health check targets poisoned");
        if targets.contains_key(portname_subject) {
            return;
        }
        targets.insert(
            portname_subject.to_string(),
            HealthCheckTarget { instance, payload },
        );
        drop(targets);

        self.health_check_notifiers
            .write()
            .expect("notifiers poisoned")
            .entry(portname_subject.to_string())
            .or_insert_with(|| Arc::new(PortnameSignal::new()));
        self.portname_health
            .write()
            .expect("portname health poisoned")
            .entry(portname_subject.to_string())
            .or_insert(HealthStatus::NotReady);
        let _ = self.new_portname_tx.send(portname_subject.to_string());
    }

    pub fn has_health_check_targets(&self) -> bool {
        !self
            .health_check_targets
            .read()
            .expect("health check targets poisoned")
            .is_empty()
    }

    pub fn get_health_check_targets(&self) -> Vec<(String, HealthCheckTarget)> {
        self.health_check_targets
            .read()
            .expect("health check targets poisoned")
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    pub fn get_health_check_portnames(&self) -> Vec<String> {
        self.health_check_targets
            .read()
            .expect("health check targets poisoned")
            .keys()
            .cloned()
            .collect()
    }

    pub fn get_health_check_target(&self, portname: &str) -> Option<HealthCheckTarget> {
        self.health_check_targets
            .read()
            .expect("health check targets poisoned")
            .get(portname)
            .cloned()
    }

    pub fn get_portname_health_status(&self, portname: &str) -> Option<HealthStatus> {
        self.portname_health
            .read()
            .expect("portname health poisoned")
            .get(portname)
            .cloned()
    }

    pub fn get_portname_health_check_notifier(&self, portname: &str) -> Option<Arc<PortnameSignal>> {
        self.health_check_notifiers
            .read()
            .expect("notifiers poisoned")
            .get(portname)
            .cloned()
    }

    pub fn take_new_portname_receiver(&self) -> Option<mpsc::Receiver<String>> {
        self.new_portname_rx
            .lock()
            .expect("new portname receiver poisoned")
            .take()
    }

    pub fn uptime(&self) -> Duration {
        self.start_time.elapsed()
    }

    pub fn initialize_uptime_gauge(&self) -> Result<&Gauge, &'static str> {
        self.uptime_gauge.set(Gauge::new()).map_err(|_| "gauge already initialized")?;
        Ok(self.uptime_gauge.get().expect("gauge missing after init"))
    }

    pub fn update_uptime_gauge(&self) {
        if let Some(gauge) = self.uptime_gauge.get() {
            gauge.set(self.uptime().as_secs_f64());
        }
    }

    pub fn health_path(&self) -> &str {
        &self.health_path
    }

    pub fn live_path(&self) -> &str {
        &self.live_path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_portnames_drive_health_status() {
        let health = SystemHealth::new(
            HealthStatus::NotReady,
            vec!["default.generate.v1".to_string()],
            "/health".to_string(),
            "/live".to_string(),
        );
        let (healthy_before, _) = health.get_health_status();
        assert!(!healthy_before);

        health.set_portname_health_status("default.generate.v1", HealthStatus::Ready);
        let (healthy_after, details) = health.get_health_status();
        assert!(healthy_after);
        assert_eq!(details.get("default.generate.v1").map(String::as_str), Some("ready"));
    }

    #[test]
    fn registering_target_initializes_notifier_and_state() {
        let health = SystemHealth::new(
            HealthStatus::NotReady,
            Vec::new(),
            "/health".to_string(),
            "/live".to_string(),
        );
        health.register_health_check_target(
            "default.prefill.v1",
            Instance {
                namespace: "default".to_string(),
                servicegroup: "prefill".to_string(),
                portname: "v1".to_string(),
                instance_id: 9,
            },
            JsonValue::string("ping"),
        );

        assert!(health.has_health_check_targets());
        assert!(health.get_portname_health_check_notifier("default.prefill.v1").is_some());
        assert_eq!(
            health.get_portname_health_status("default.prefill.v1"),
            Some(HealthStatus::NotReady)
        );
    }

    #[test]
    fn receiver_can_only_be_taken_once() {
        let health = SystemHealth::new(
            HealthStatus::Ready,
            Vec::new(),
            "/health".to_string(),
            "/live".to_string(),
        );
        assert!(health.take_new_portname_receiver().is_some());
        assert!(health.take_new_portname_receiver().is_none());
    }

    #[test]
    fn uptime_gauge_tracks_elapsed_time() {
        let health = SystemHealth::new(
            HealthStatus::Ready,
            Vec::new(),
            "/health".to_string(),
            "/live".to_string(),
        );
        let gauge = health.initialize_uptime_gauge().expect("gauge should initialize");
        assert_eq!(gauge.get(), 0.0);
        std::thread::sleep(Duration::from_millis(20));
        health.update_uptime_gauge();
        assert!(gauge.get() > 0.0);
    }
}