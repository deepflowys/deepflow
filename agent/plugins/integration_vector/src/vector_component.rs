use std::sync::Arc;
use std::thread::JoinHandle;
use tokio::runtime::Runtime;

pub struct VectorComponent {}

impl VectorComponent {
    pub fn new(_: bool, _: serde_yaml::Value, _: Arc<Runtime>) -> Self {
        Self {}
    }

    pub fn start(&mut self) {}

    pub fn notify_stop(&mut self) -> Option<JoinHandle<()>> {
        None
    }

    pub fn on_config_change(&mut self, _: bool, _: serde_yaml::Value) {}
}
