use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HealthStatus {
    Ready,
    NotReady,
}

impl HealthStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Ready => "Ready",
            Self::NotReady => "NotReady",
        }
    }

    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready)
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

    pub fn bool(value: bool) -> Self {
        Self::Bool(value)
    }

    pub fn object(entries: impl IntoIterator<Item = (impl Into<String>, JsonValue)>) -> Self {
        let mut map = HashMap::new();
        for (key, value) in entries {
            map.insert(key.into(), value);
        }
        Self::Object(map)
    }
}

#[derive(Debug, Clone)]
pub struct HealthCheckError {
    message: String,
}

impl HealthCheckError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for HealthCheckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for HealthCheckError {}

type Result<T> = std::result::Result<T, HealthCheckError>;

#[derive(Clone, Debug)]
pub struct HealthCheckConfig {
    pub canary_wait_time: Duration,
    pub request_timeout: Duration,
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            canary_wait_time: Duration::from_secs(30),
            request_timeout: Duration::from_secs(5),
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

impl Instance {
    pub fn new(
        namespace: impl Into<String>,
        servicegroup: impl Into<String>,
        portname: impl Into<String>,
        instance_id: u64,
    ) -> Self {
        Self {
            namespace: namespace.into(),
            servicegroup: servicegroup.into(),
            portname: portname.into(),
            instance_id,
        }
    }
}

#[derive(Clone, Debug)]
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
        let mut generation = self.generation.lock().expect("portname signal poisoned");
        *generation += 1;
        self.condvar.notify_all();
    }

    pub fn current_generation(&self) -> u64 {
        *self.generation.lock().expect("portname signal poisoned")
    }

    pub fn wait_for_change(&self, last_seen: &mut u64, timeout: Duration) -> bool {
        let generation = self.generation.lock().expect("portname signal poisoned");
        let (generation, _) = self
            .condvar
            .wait_timeout_while(generation, timeout, |current| *current == *last_seen)
            .expect("portname signal poisoned");
        let changed = *generation != *last_seen;
        *last_seen = *generation;
        changed
    }
}

pub struct SystemHealth {
    system_health: Mutex<HealthStatus>,
    portname_health: RwLock<HashMap<String, HealthStatus>>,
    health_check_targets: RwLock<HashMap<String, HealthCheckTarget>>,
    health_check_notifiers: RwLock<HashMap<String, Arc<PortnameSignal>>>,
    new_portname_tx: mpsc::Sender<String>,
    new_portname_rx: Mutex<Option<mpsc::Receiver<String>>>,
    use_portname_health_status: Vec<String>,
    health_path: String,
    live_path: String,
    start_time: Instant,
}

impl SystemHealth {
    pub fn new(
        starting_health_status: HealthStatus,
        use_portname_health_status: Vec<String>,
        health_path: impl Into<String>,
        live_path: impl Into<String>,
    ) -> Self {
        let (tx, rx) = mpsc::channel();
        let mut portname_health = HashMap::new();
        for portname in &use_portname_health_status {
            portname_health.insert(portname.clone(), starting_health_status.clone());
        }

        Self {
            system_health: Mutex::new(starting_health_status),
            portname_health: RwLock::new(portname_health),
            health_check_targets: RwLock::new(HashMap::new()),
            health_check_notifiers: RwLock::new(HashMap::new()),
            new_portname_tx: tx,
            new_portname_rx: Mutex::new(Some(rx)),
            use_portname_health_status,
            health_path: health_path.into(),
            live_path: live_path.into(),
            start_time: Instant::now(),
        }
    }

    pub fn set_health_status(&self, status: HealthStatus) {
        *self.system_health.lock().expect("system health poisoned") = status;
    }

    pub fn get_health_status(&self) -> (bool, HashMap<String, String>) {
        let portname_health = self
            .portname_health
            .read()
            .expect("portname health poisoned")
            .clone();

        let explicit_targets = if self.use_portname_health_status.is_empty() {
            None
        } else {
            Some(self.use_portname_health_status.clone())
        };

        let computed_targets = if explicit_targets.is_none() {
            let targets = self.get_health_check_portnames();
            if targets.is_empty() {
                None
            } else {
                Some(targets)
            }
        } else {
            None
        };

        let summary = portname_health
            .iter()
            .map(|(portname, status)| (portname.clone(), status.as_str().to_string()))
            .collect::<HashMap<_, _>>();

        if let Some(targets) = explicit_targets.or(computed_targets) {
            let healthy = targets.iter().all(|portname| {
                portname_health
                    .get(portname)
                    .map(HealthStatus::is_ready)
                    .unwrap_or(false)
            });
            return (healthy, summary);
        }

        let healthy = self
            .system_health
            .lock()
            .expect("system health poisoned")
            .is_ready();
        (healthy, summary)
    }

    pub fn set_portname_health_status(&self, portname_subject: &str, status: HealthStatus) {
        self.portname_health
            .write()
            .expect("portname health poisoned")
            .insert(portname_subject.to_string(), status);
    }

    pub fn get_portname_health_status(&self, portname_subject: &str) -> Option<HealthStatus> {
        self.portname_health
            .read()
            .expect("portname health poisoned")
            .get(portname_subject)
            .cloned()
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
            .expect("health check notifiers poisoned")
            .entry(portname_subject.to_string())
            .or_insert_with(|| Arc::new(PortnameSignal::new()));

        self.portname_health
            .write()
            .expect("portname health poisoned")
            .entry(portname_subject.to_string())
            .or_insert(HealthStatus::NotReady);

        let _ = self.new_portname_tx.send(portname_subject.to_string());
    }

    pub fn get_health_check_target(&self, portname_subject: &str) -> Option<HealthCheckTarget> {
        self.health_check_targets
            .read()
            .expect("health check targets poisoned")
            .get(portname_subject)
            .cloned()
    }

    pub fn get_health_check_targets(&self) -> HashMap<String, HealthCheckTarget> {
        self.health_check_targets
            .read()
            .expect("health check targets poisoned")
            .clone()
    }

    pub fn get_health_check_portnames(&self) -> Vec<String> {
        self.health_check_targets
            .read()
            .expect("health check targets poisoned")
            .keys()
            .cloned()
            .collect()
    }

    pub fn get_portname_health_check_notifier(
        &self,
        portname_subject: &str,
    ) -> Option<Arc<PortnameSignal>> {
        self.health_check_notifiers
            .read()
            .expect("health check notifiers poisoned")
            .get(portname_subject)
            .cloned()
    }

    pub fn notify_portname_activity(&self, portname_subject: &str) {
        if let Some(notifier) = self.get_portname_health_check_notifier(portname_subject) {
            notifier.notify();
        }
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

    pub fn health_path(&self) -> &str {
        &self.health_path
    }

    pub fn live_path(&self) -> &str {
        &self.live_path
    }
}

#[derive(Clone)]
pub struct DistributedRuntime {
    system_health: Arc<SystemHealth>,
}

impl DistributedRuntime {
    pub fn new(system_health: SystemHealth) -> Self {
        Self {
            system_health: Arc::new(system_health),
        }
    }

    pub fn system_health(&self) -> Arc<SystemHealth> {
        self.system_health.clone()
    }

    pub fn namespace(&self, name: &str) -> Namespace {
        Namespace {
            drt: self.clone(),
            name: name.to_string(),
        }
    }
}

#[derive(Clone)]
pub struct Namespace {
    drt: DistributedRuntime,
    name: String,
}

impl Namespace {
    pub fn servicegroup(&self, name: &str) -> ServiceGroup {
        ServiceGroup {
            drt: self.drt.clone(),
            namespace: self.name.clone(),
            name: name.to_string(),
        }
    }
}

#[derive(Clone)]
pub struct ServiceGroup {
    drt: DistributedRuntime,
    namespace: String,
    name: String,
}

impl ServiceGroup {
    pub fn portname(&self, name: &str) -> PortName {
        PortName {
            drt: self.drt.clone(),
            namespace: self.namespace.clone(),
            servicegroup: self.name.clone(),
            name: name.to_string(),
        }
    }
}

#[derive(Clone)]
pub struct PortName {
    drt: DistributedRuntime,
    namespace: String,
    servicegroup: String,
    name: String,
}

impl PortName {
    pub fn subject(&self) -> String {
        format!("{}.{}.{}", self.namespace, self.servicegroup, self.name)
    }

    pub fn runtime(&self) -> &DistributedRuntime {
        &self.drt
    }
}

#[derive(Clone, Debug)]
pub struct ProbeResponse {
    pub first_response_ok: bool,
}

pub trait ProbeTransport: Send + Sync {
    fn wait_for_instances(&self, portname_subject: &str, timeout: Duration) -> Result<()>;

    fn direct(
        &self,
        portname_subject: &str,
        instance_id: u64,
        payload: JsonValue,
        timeout: Duration,
    ) -> Result<ProbeResponse>;
}

#[derive(Clone)]
struct Router {
    portname_subject: String,
    transport: Arc<dyn ProbeTransport>,
}

impl Router {
    fn wait_for_instances(&self, timeout: Duration) -> Result<()> {
        self.transport.wait_for_instances(&self.portname_subject, timeout)
    }

    fn direct(&self, instance_id: u64, payload: JsonValue, timeout: Duration) -> Result<ProbeResponse> {
        self.transport
            .direct(&self.portname_subject, instance_id, payload, timeout)
    }
}

struct HealthTask {
    stop: Arc<AtomicBool>,
    handle: JoinHandle<()>,
}

pub struct HealthCheckManager {
    drt: DistributedRuntime,
    config: HealthCheckConfig,
    transport: Arc<dyn ProbeTransport>,
    router_cache: Mutex<HashMap<String, Arc<Router>>>,
    portname_tasks: Mutex<HashMap<String, HealthTask>>,
    monitor_task: Mutex<Option<HealthTask>>,
    is_stopped: AtomicBool,
}

impl HealthCheckManager {
    pub fn new(
        drt: DistributedRuntime,
        config: HealthCheckConfig,
        transport: Arc<dyn ProbeTransport>,
    ) -> Self {
        Self {
            drt,
            config,
            transport,
            router_cache: Mutex::new(HashMap::new()),
            portname_tasks: Mutex::new(HashMap::new()),
            monitor_task: Mutex::new(None),
            is_stopped: AtomicBool::new(false),
        }
    }

    pub fn start(self: &Arc<Self>) -> Result<()> {
        for portname_subject in self.drt.system_health().get_health_check_portnames() {
            self.spawn_portname_health_check_task(portname_subject)?;
        }
        self.spawn_new_portname_monitor()
    }

    fn get_or_create_router(&self, cache_key: &str, portname: PortName) -> Result<Arc<Router>> {
        if let Some(router) = self
            .router_cache
            .lock()
            .expect("router cache poisoned")
            .get(cache_key)
            .cloned()
        {
            return Ok(router);
        }

        let router = Arc::new(Router {
            portname_subject: portname.subject(),
            transport: self.transport.clone(),
        });

        self.router_cache
            .lock()
            .expect("router cache poisoned")
            .insert(cache_key.to_string(), router.clone());
        Ok(router)
    }

    fn spawn_portname_health_check_task(self: &Arc<Self>, portname_subject: String) -> Result<()> {
        let notifier = self
            .drt
            .system_health()
            .get_portname_health_check_notifier(&portname_subject)
            .ok_or_else(|| HealthCheckError::new(format!("missing notifier for {}", portname_subject)))?;

        {
            let tasks = self
                .portname_tasks
                .lock()
                .expect("portname tasks poisoned");
            if tasks.contains_key(&portname_subject) {
                return Ok(());
            }
        }

        let manager = self.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = stop.clone();
        let subject_for_thread = portname_subject.clone();

        let handle = thread::spawn(move || {
            let mut generation = notifier.current_generation();
            while !stop_flag.load(Ordering::SeqCst) && !manager.is_stopped.load(Ordering::SeqCst) {
                let notified = notifier.wait_for_change(&mut generation, manager.config.canary_wait_time);
                if stop_flag.load(Ordering::SeqCst) || manager.is_stopped.load(Ordering::SeqCst) {
                    break;
                }
                if notified {
                    continue;
                }
                let _ = manager.send_health_check_request(&subject_for_thread);
            }
        });

        self.portname_tasks
            .lock()
            .expect("portname tasks poisoned")
            .insert(portname_subject, HealthTask { stop, handle });
        Ok(())
    }

    fn spawn_new_portname_monitor(self: &Arc<Self>) -> Result<()> {
        let rx = self
            .drt
            .system_health()
            .take_new_portname_receiver()
            .ok_or_else(|| HealthCheckError::new("new portname receiver already taken"))?;

        let mut monitor_slot = self.monitor_task.lock().expect("monitor task poisoned");
        if monitor_slot.is_some() {
            return Ok(());
        }

        let manager = self.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = stop.clone();
        let handle = thread::spawn(move || {
            while !stop_flag.load(Ordering::SeqCst) && !manager.is_stopped.load(Ordering::SeqCst) {
                match rx.recv_timeout(Duration::from_millis(100)) {
                    Ok(portname_subject) => {
                        let exists = manager
                            .portname_tasks
                            .lock()
                            .expect("portname tasks poisoned")
                            .contains_key(&portname_subject);
                        if exists {
                            break;
                        }
                        let _ = manager.spawn_portname_health_check_task(portname_subject);
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => continue,
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
        });

        *monitor_slot = Some(HealthTask { stop, handle });
        Ok(())
    }

    fn send_health_check_request(&self, portname_subject: &str) -> Result<()> {
        let target = self
            .drt
            .system_health()
            .get_health_check_target(portname_subject)
            .ok_or_else(|| HealthCheckError::new(format!("no health check target for {}", portname_subject)))?;

        let namespace = self.drt.namespace(&target.instance.namespace);
        let servicegroup = namespace.servicegroup(&target.instance.servicegroup);
        let portname = servicegroup.portname(&target.instance.portname);
        let router = self.get_or_create_router(portname_subject, portname)?;
        router.wait_for_instances(Duration::from_secs(10))?;

        let system_health = self.drt.system_health();
        let timeout = self.config.request_timeout;
        let payload = target.payload.clone();
        let instance_id = target.instance.instance_id;
        let subject = portname_subject.to_string();
        let router = router.clone();

        thread::spawn(move || {
            let status = match router.direct(instance_id, payload, timeout) {
                Ok(response) if response.first_response_ok => HealthStatus::Ready,
                Ok(_) => HealthStatus::NotReady,
                Err(_) => HealthStatus::NotReady,
            };
            system_health.set_portname_health_status(&subject, status);
        });

        Ok(())
    }

    pub fn stop(&self) {
        self.is_stopped.store(true, Ordering::SeqCst);

        if let Some(monitor) = self
            .monitor_task
            .lock()
            .expect("monitor task poisoned")
            .take()
        {
            monitor.stop.store(true, Ordering::SeqCst);
            let _ = monitor.handle.join();
        }

        let mut tasks = self
            .portname_tasks
            .lock()
            .expect("portname tasks poisoned");
        let drained = tasks.drain().map(|(_, task)| task).collect::<Vec<_>>();
        drop(tasks);

        for task in drained {
            task.stop.store(true, Ordering::SeqCst);
            let _ = task.handle.join();
        }
    }
}

impl Drop for HealthCheckManager {
    fn drop(&mut self) {
        self.is_stopped.store(true, Ordering::SeqCst);
    }
}

pub fn start_health_check_manager(
    drt: DistributedRuntime,
    config: Option<HealthCheckConfig>,
    transport: Arc<dyn ProbeTransport>,
) -> Result<()> {
    let manager = Arc::new(HealthCheckManager::new(
        drt,
        config.unwrap_or_default(),
        transport,
    ));
    manager.start()?;
    Ok(())
}

pub fn get_health_check_status(drt: &DistributedRuntime) -> Result<JsonValue> {
    let portname_subjects = drt.system_health().get_health_check_portnames();
    let mut portname_statuses = HashMap::new();

    for portname_subject in &portname_subjects {
        let status = drt
            .system_health()
            .get_portname_health_status(portname_subject)
            .unwrap_or(HealthStatus::NotReady);

        portname_statuses.insert(
            portname_subject.clone(),
            JsonValue::object([
                ("healthy", JsonValue::bool(status.is_ready())),
                ("status", JsonValue::string(status.as_str())),
            ]),
        );
    }

    let overall_healthy = portname_subjects.iter().all(|portname_subject| {
        drt.system_health()
            .get_portname_health_status(portname_subject)
            .map(|status| status.is_ready())
            .unwrap_or(false)
    });

    Ok(JsonValue::object([
        (
            "status",
            JsonValue::string(if overall_healthy { "ready" } else { "notready" }),
        ),
        (
            "portnames_checked",
            JsonValue::number(portname_subjects.len() as i64),
        ),
        ("portname_statuses", JsonValue::Object(portname_statuses)),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct MockProbeTransport {
        waits: Mutex<HashMap<String, bool>>,
        responses: Mutex<HashMap<String, VecDeque<Result<ProbeResponse>>>>,
        calls: Mutex<HashMap<String, usize>>,
    }

    impl MockProbeTransport {
        fn set_wait_ready(&self, portname_subject: &str, ready: bool) {
            self.waits
                .lock()
                .expect("wait map poisoned")
                .insert(portname_subject.to_string(), ready);
        }

        fn push_response(&self, portname_subject: &str, response: Result<ProbeResponse>) {
            self.responses
                .lock()
                .expect("response map poisoned")
                .entry(portname_subject.to_string())
                .or_default()
                .push_back(response);
        }

        fn call_count(&self, portname_subject: &str) -> usize {
            self.calls
                .lock()
                .expect("call map poisoned")
                .get(portname_subject)
                .copied()
                .unwrap_or(0)
        }
    }

    impl ProbeTransport for MockProbeTransport {
        fn wait_for_instances(&self, portname_subject: &str, _timeout: Duration) -> Result<()> {
            let ready = self
                .waits
                .lock()
                .expect("wait map poisoned")
                .get(portname_subject)
                .copied()
                .unwrap_or(true);
            if ready {
                Ok(())
            } else {
                Err(HealthCheckError::new("instances not ready"))
            }
        }

        fn direct(
            &self,
            portname_subject: &str,
            _instance_id: u64,
            _payload: JsonValue,
            _timeout: Duration,
        ) -> Result<ProbeResponse> {
            let mut calls = self.calls.lock().expect("call map poisoned");
            let entry = calls.entry(portname_subject.to_string()).or_insert(0);
            *entry += 1;
            drop(calls);

            self.responses
                .lock()
                .expect("response map poisoned")
                .get_mut(portname_subject)
                .and_then(VecDeque::pop_front)
                .unwrap_or_else(|| Ok(ProbeResponse { first_response_ok: true }))
        }
    }

    fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool) {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if predicate() {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(predicate(), "condition was not met before timeout");
    }

    fn build_runtime() -> DistributedRuntime {
        DistributedRuntime::new(SystemHealth::new(
            HealthStatus::NotReady,
            Vec::new(),
            "/health",
            "/live",
        ))
    }

    fn build_manager(
        drt: DistributedRuntime,
        canary_wait_time: Duration,
        request_timeout: Duration,
        transport: Arc<dyn ProbeTransport>,
    ) -> Arc<HealthCheckManager> {
        let manager = Arc::new(HealthCheckManager::new(
            drt,
            HealthCheckConfig {
                canary_wait_time,
                request_timeout,
            },
            transport,
        ));
        manager.start().expect("manager should start");
        manager
    }

    #[test]
    fn health_probe_marks_portname_ready() {
        let drt = build_runtime();
        let subject = "default.generate.v1";
        drt.system_health().register_health_check_target(
            subject,
            Instance::new("default", "generate", "v1", 7),
            JsonValue::object([("prompt", JsonValue::string("ping"))]),
        );

        let transport = Arc::new(MockProbeTransport::default());
        transport.set_wait_ready(subject, true);
        transport.push_response(subject, Ok(ProbeResponse { first_response_ok: true }));

        let manager = build_manager(
            drt.clone(),
            Duration::from_millis(40),
            Duration::from_millis(50),
            transport,
        );

        wait_until(Duration::from_millis(300), || {
            drt.system_health().get_portname_health_status(subject) == Some(HealthStatus::Ready)
        });

        let status = get_health_check_status(&drt).expect("status should build");
        match status {
            JsonValue::Object(map) => {
                assert_eq!(map.get("status"), Some(&JsonValue::string("ready")));
            }
            _ => panic!("expected object"),
        }

        manager.stop();
    }

    #[test]
    fn notifier_resets_canary_timer() {
        let drt = build_runtime();
        let subject = "default.prefill.v1";
        drt.system_health().register_health_check_target(
            subject,
            Instance::new("default", "prefill", "v1", 11),
            JsonValue::object([("prompt", JsonValue::string("hello"))]),
        );

        let transport = Arc::new(MockProbeTransport::default());
        transport.set_wait_ready(subject, true);
        let manager = build_manager(
            drt.clone(),
            Duration::from_millis(80),
            Duration::from_millis(50),
            transport.clone(),
        );

        for _ in 0..3 {
            thread::sleep(Duration::from_millis(30));
            drt.system_health().notify_portname_activity(subject);
        }

        assert_eq!(transport.call_count(subject), 0);

        wait_until(Duration::from_millis(300), || transport.call_count(subject) >= 1);
        manager.stop();
    }

    #[test]
    fn monitor_picks_up_new_portname_after_start() {
        let drt = build_runtime();
        let subject = "default.decode.v1";
        let transport = Arc::new(MockProbeTransport::default());
        transport.set_wait_ready(subject, true);
        transport.push_response(subject, Ok(ProbeResponse { first_response_ok: true }));

        let manager = build_manager(
            drt.clone(),
            Duration::from_millis(40),
            Duration::from_millis(50),
            transport,
        );

        drt.system_health().register_health_check_target(
            subject,
            Instance::new("default", "decode", "v1", 13),
            JsonValue::object([("prompt", JsonValue::string("decode"))]),
        );

        wait_until(Duration::from_millis(300), || {
            drt.system_health().get_portname_health_status(subject) == Some(HealthStatus::Ready)
        });

        manager.stop();
    }

    #[test]
    fn failed_probe_marks_portname_not_ready() {
        let drt = build_runtime();
        let subject = "default.generate.v2";
        drt.system_health().register_health_check_target(
            subject,
            Instance::new("default", "generate", "v2", 17),
            JsonValue::object([("prompt", JsonValue::string("ping"))]),
        );

        let transport = Arc::new(MockProbeTransport::default());
        transport.set_wait_ready(subject, true);
        transport.push_response(subject, Err(HealthCheckError::new("worker timeout")));

        let manager = build_manager(
            drt.clone(),
            Duration::from_millis(30),
            Duration::from_millis(30),
            transport,
        );

        wait_until(Duration::from_millis(300), || {
            drt.system_health().get_portname_health_status(subject) == Some(HealthStatus::NotReady)
        });

        manager.stop();
    }
}