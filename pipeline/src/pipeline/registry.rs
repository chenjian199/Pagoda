use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct Registry {
    shared_storage: HashMap<String, Arc<dyn Any + Send + Sync>>,
    unique_storage: HashMap<String, Box<dyn Any + Send + Sync>>,
}

impl Registry {
    pub fn new() -> Self {
        Self {
            shared_storage: HashMap::new(),
            unique_storage: HashMap::new(),
        }
    }

    pub fn contains_shared(&self, key: &str) -> bool {
        self.shared_storage.contains_key(key)
    }

    pub fn contains_unique(&self, key: &str) -> bool {
        self.unique_storage.contains_key(key)
    }

    pub fn insert_shared<V: Any + Send + Sync + 'static>(&mut self, key: impl Into<String>, value: V) {
        self.shared_storage.insert(key.into(), Arc::new(value));
    }

    pub fn get_shared<V: Any + Send + Sync + 'static>(&self, key: &str) -> Result<Arc<V>, String> {
        let arc = self
            .shared_storage
            .get(key)
            .ok_or_else(|| format!("key not found in shared_storage: {key}"))?;
        arc.clone()
            .downcast::<V>()
            .map_err(|_| format!("type mismatch for key: {key}"))
    }

    pub fn insert_unique<V: Any + Send + Sync + 'static>(&mut self, key: impl Into<String>, value: V) {
        self.unique_storage.insert(key.into(), Box::new(value));
    }

    pub fn take_unique<V: Any + Send + Sync + 'static>(&mut self, key: &str) -> Result<V, String> {
        let boxed = self
            .unique_storage
            .remove(key)
            .ok_or_else(|| format!("key not found in unique_storage: {key}"))?;
        boxed
            .downcast::<V>()
            .map(|b| *b)
            .map_err(|_| format!("type mismatch for key: {key}"))
    }

    pub fn clone_unique<V: Any + Send + Sync + Clone + 'static>(&self, key: &str) -> Result<V, String> {
        let boxed = self
            .unique_storage
            .get(key)
            .ok_or_else(|| format!("key not found in unique_storage: {key}"))?;
        boxed
            .downcast_ref::<V>()
            .cloned()
            .ok_or_else(|| format!("type mismatch for key: {key}"))
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_get_shared() {
        let mut registry = Registry::new();
        registry.insert_shared("shared1", 42);
        assert_eq!(*registry.get_shared::<i32>("shared1").unwrap(), 42);
        assert!(registry.get_shared::<f64>("shared1").is_err()); // Testing a downcast failure
    }

    #[test]
    fn test_insert_and_take_unique() {
        let mut registry = Registry::new();
        registry.insert_unique("unique1", "Hello".to_string());
        assert_eq!(registry.take_unique::<String>("unique1").unwrap(), "Hello");
        assert!(registry.take_unique::<String>("unique1").is_err()); // Key is now missing
    }

    #[test]
    fn test_insert_and_clone_then_take_unique() {
        let mut registry = Registry::new();

        registry.insert_unique("unique2", "World".to_string());

        assert_eq!(registry.clone_unique::<String>("unique2").unwrap(), "World");

        // When cloned, the object should still be available for taking
        assert!(registry.take_unique::<String>("unique2").is_ok());
    }

    #[test]
    fn test_failed_take_after_cloning() {
        let mut registry = Registry::new();

        registry.insert_unique("unique3", "Another".to_string());
        assert_eq!(
            registry.clone_unique::<String>("unique3").unwrap(),
            "Another"
        );

        // Cloned, then Take is OK
        assert_eq!(
            registry.take_unique::<String>("unique3").unwrap(),
            "Another"
        );

        // Take, then Take again should fail
        assert!(registry.take_unique::<String>("unique3").is_err());
    }
}