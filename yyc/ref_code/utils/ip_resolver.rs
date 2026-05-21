use std::env;
use std::net::{IpAddr, Ipv4Addr};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalIpResolutionError {
    AddressNotFound,
    ResolverFailed(String),
}

pub trait IpResolver: Send + Sync + 'static {
    fn local_ip(&self) -> Result<IpAddr, LocalIpResolutionError>;
    fn local_ipv6(&self) -> Result<IpAddr, LocalIpResolutionError>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SystemIpResolver;

impl IpResolver for SystemIpResolver {
    fn local_ip(&self) -> Result<IpAddr, LocalIpResolutionError> {
        Err(LocalIpResolutionError::AddressNotFound)
    }

    fn local_ipv6(&self) -> Result<IpAddr, LocalIpResolutionError> {
        Err(LocalIpResolutionError::AddressNotFound)
    }
}

fn resolve_local_ip_with_resolver<R: IpResolver>(resolver: R) -> IpAddr {
    resolver
        .local_ip()
        .or_else(|error| match error {
            LocalIpResolutionError::AddressNotFound => resolver.local_ipv6(),
            other => Err(other),
        })
        .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST))
}

pub fn format_ip_for_url(addr: IpAddr) -> String {
    match addr {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => format!("[{}]", v6),
    }
}

pub fn get_local_ip_for_advertise_with_resolver<R: IpResolver>(resolver: R) -> String {
    format_ip_for_url(resolve_local_ip_with_resolver(resolver))
}

pub fn get_local_ip_for_advertise() -> String {
    get_local_ip_for_advertise_with_resolver(SystemIpResolver)
}

pub fn get_http_rpc_host_with_resolver<R: IpResolver>(resolver: R) -> String {
    get_local_ip_for_advertise_with_resolver(resolver)
}

pub fn get_http_rpc_host() -> String {
    get_http_rpc_host_with_resolver(SystemIpResolver)
}

pub fn get_http_rpc_host_from_env() -> String {
    env::var("DYN_HTTP_RPC_HOST").unwrap_or_else(|_| get_http_rpc_host())
}

pub fn get_tcp_rpc_host_from_env() -> String {
    env::var("DYN_TCP_RPC_HOST").unwrap_or_else(|_| get_local_ip_for_advertise())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv6Addr;

    #[derive(Clone)]
    struct MockResolver {
        ipv4: Result<IpAddr, LocalIpResolutionError>,
        ipv6: Result<IpAddr, LocalIpResolutionError>,
    }

    impl IpResolver for MockResolver {
        fn local_ip(&self) -> Result<IpAddr, LocalIpResolutionError> {
            self.ipv4.clone()
        }

        fn local_ipv6(&self) -> Result<IpAddr, LocalIpResolutionError> {
            self.ipv6.clone()
        }
    }

    #[test]
    fn falls_back_to_ipv6_when_ipv4_is_missing() {
        let resolver = MockResolver {
            ipv4: Err(LocalIpResolutionError::AddressNotFound),
            ipv6: Ok(IpAddr::V6(Ipv6Addr::LOCALHOST)),
        };

        assert_eq!(get_http_rpc_host_with_resolver(resolver), "[::1]");
    }

    #[test]
    fn falls_back_to_localhost_when_resolution_fails() {
        let resolver = MockResolver {
            ipv4: Err(LocalIpResolutionError::AddressNotFound),
            ipv6: Err(LocalIpResolutionError::ResolverFailed("unavailable".to_string())),
        };

        assert_eq!(get_local_ip_for_advertise_with_resolver(resolver), "127.0.0.1");
    }
}