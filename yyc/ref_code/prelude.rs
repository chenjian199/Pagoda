#[path = "traits.rs"]
mod traits;

pub use traits::*;

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn prelude_reexports_traits_module() {
		let runtime = Runtime::new("rt-prelude");
		let drt = DistributedRuntime::new(runtime, "cluster-prelude");
		assert_eq!(drt.rt().id(), "rt-prelude");
	}
}