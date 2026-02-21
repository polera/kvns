use std::collections::HashMap;

pub const DEFAULT_MEMORY_LIMIT: usize = 1_073_741_824; // 1 GiB
pub const DEFAULT_PERSIST_INTERVAL_SECS: u64 = 300;   // 5 minutes

#[derive(Clone, Debug, PartialEq, Default)]
pub enum EvictionPolicy {
    #[default]
    None,
    Lru,
    Mru,
}

impl EvictionPolicy {
    fn from_str(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "lru" => Some(EvictionPolicy::Lru),
            "mru" => Some(EvictionPolicy::Mru),
            "none" => Some(EvictionPolicy::None),
            _ => None,
        }
    }
}

pub struct Config {
    pub port: u16,
    pub host: String,
    pub memory_limit: usize,
    pub metrics_port: u16,
    pub metrics_host: String,
    /// Path to the persistence file. `None` disables disk persistence.
    pub persist_path: Option<String>,
    /// How often (in seconds) to flush the store to disk when persistence is enabled.
    pub persist_interval_secs: u64,
    /// Fraction of `memory_limit` at which eviction triggers (0.0–1.0). Default: 1.0.
    pub eviction_threshold: f64,
    /// Global eviction policy. Default: `None` (disabled).
    pub eviction_policy: EvictionPolicy,
    /// Per-namespace eviction policy overrides.
    pub namespace_eviction_policies: HashMap<String, EvictionPolicy>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            port: 6480,
            host: "0.0.0.0".to_string(),
            memory_limit: DEFAULT_MEMORY_LIMIT,
            metrics_port: 9090,
            metrics_host: "0.0.0.0".to_string(),
            persist_path: None,
            persist_interval_secs: DEFAULT_PERSIST_INTERVAL_SECS,
            eviction_threshold: 1.0,
            eviction_policy: EvictionPolicy::None,
            namespace_eviction_policies: HashMap::new(),
        }
    }
}

impl Config {
    pub fn from_env() -> Self {
        Self::from_vars(
            std::env::var("KVNS_PORT").ok().as_deref(),
            std::env::var("KVNS_HOST").ok().as_deref(),
            std::env::var("KVNS_MEMORY_LIMIT").ok().as_deref(),
            std::env::var("KVNS_METRICS_PORT").ok().as_deref(),
            std::env::var("KVNS_METRICS_HOST").ok().as_deref(),
            std::env::var("KVNS_PERSIST_PATH").ok().as_deref(),
            std::env::var("KVNS_PERSIST_INTERVAL").ok().as_deref(),
            std::env::var("KVNS_EVICTION_THRESHOLD").ok().as_deref(),
            std::env::var("KVNS_EVICTION_POLICY").ok().as_deref(),
            std::env::var("KVNS_NS_EVICTION").ok().as_deref(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn from_vars(
        port: Option<&str>,
        host: Option<&str>,
        memory_limit: Option<&str>,
        metrics_port: Option<&str>,
        metrics_host: Option<&str>,
        persist_path: Option<&str>,
        persist_interval: Option<&str>,
        eviction_threshold: Option<&str>,
        eviction_policy: Option<&str>,
        ns_eviction: Option<&str>,
    ) -> Self {
        let defaults = Self::default();
        Self {
            port: port
                .and_then(|s| s.parse().ok())
                .unwrap_or(defaults.port),
            host: host
                .map(|s| s.to_string())
                .unwrap_or(defaults.host),
            memory_limit: memory_limit
                .and_then(|s| s.parse().ok())
                .unwrap_or(defaults.memory_limit),
            metrics_port: metrics_port
                .and_then(|s| s.parse().ok())
                .unwrap_or(defaults.metrics_port),
            metrics_host: metrics_host
                .map(|s| s.to_string())
                .unwrap_or(defaults.metrics_host),
            persist_path: persist_path.map(|s| s.to_string()),
            persist_interval_secs: persist_interval
                .and_then(|s| s.parse().ok())
                .unwrap_or(defaults.persist_interval_secs),
            eviction_threshold: eviction_threshold
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(defaults.eviction_threshold),
            eviction_policy: eviction_policy
                .and_then(EvictionPolicy::from_str)
                .unwrap_or(defaults.eviction_policy),
            namespace_eviction_policies: ns_eviction
                .map(Self::parse_ns_eviction)
                .unwrap_or_default(),
        }
    }

    /// Parse `"ns1:lru,ns2:mru"` into a `HashMap<String, EvictionPolicy>`.
    fn parse_ns_eviction(s: &str) -> HashMap<String, EvictionPolicy> {
        s.split(',')
            .filter_map(|pair| {
                let mut parts = pair.splitn(2, ':');
                let ns = parts.next()?.trim().to_string();
                let policy_str = parts.next()?.trim();
                let policy = EvictionPolicy::from_str(policy_str)?;
                Some((ns, policy))
            })
            .collect()
    }

    pub fn listen_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub fn metrics_listen_addr(&self) -> String {
        format!("{}:{}", self.metrics_host, self.metrics_port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_correct() {
        let c = Config::default();
        assert_eq!(c.port, 6480);
        assert_eq!(c.host, "0.0.0.0");
        assert_eq!(c.memory_limit, DEFAULT_MEMORY_LIMIT);
    }

    #[test]
    fn from_vars_all_none_returns_defaults() {
        let c = Config::from_vars(None, None, None, None, None, None, None, None, None, None);
        assert_eq!(c.port, 6480);
        assert_eq!(c.host, "0.0.0.0");
        assert_eq!(c.memory_limit, DEFAULT_MEMORY_LIMIT);
    }

    #[test]
    fn from_vars_port_override() {
        let c = Config::from_vars(Some("7000"), None, None, None, None, None, None, None, None, None);
        assert_eq!(c.port, 7000);
    }

    #[test]
    fn from_vars_host_override() {
        let c = Config::from_vars(None, Some("127.0.0.1"), None, None, None, None, None, None, None, None);
        assert_eq!(c.host, "127.0.0.1");
    }

    #[test]
    fn from_vars_memory_limit_override() {
        let c = Config::from_vars(None, None, Some("2048"), None, None, None, None, None, None, None);
        assert_eq!(c.memory_limit, 2048);
    }

    #[test]
    fn from_vars_invalid_port_falls_back_to_default() {
        let c = Config::from_vars(Some("not_a_port"), None, None, None, None, None, None, None, None, None);
        assert_eq!(c.port, 6480);
    }

    #[test]
    fn from_vars_invalid_memory_limit_falls_back_to_default() {
        let c = Config::from_vars(None, None, Some("not_a_number"), None, None, None, None, None, None, None);
        assert_eq!(c.memory_limit, DEFAULT_MEMORY_LIMIT);
    }

    #[test]
    fn listen_addr_formats_correctly() {
        let c = Config::default();
        assert_eq!(c.listen_addr(), "0.0.0.0:6480");
    }

    #[test]
    fn listen_addr_custom_host_and_port() {
        let c = Config::from_vars(Some("9000"), Some("127.0.0.1"), None, None, None, None, None, None, None, None);
        assert_eq!(c.listen_addr(), "127.0.0.1:9000");
    }

    #[test]
    fn metrics_defaults_are_correct() {
        let c = Config::default();
        assert_eq!(c.metrics_port, 9090);
        assert_eq!(c.metrics_host, "0.0.0.0");
    }

    #[test]
    fn from_vars_metrics_port_override() {
        let c = Config::from_vars(None, None, None, Some("9999"), None, None, None, None, None, None);
        assert_eq!(c.metrics_port, 9999);
    }

    #[test]
    fn from_vars_metrics_host_override() {
        let c = Config::from_vars(None, None, None, None, Some("127.0.0.1"), None, None, None, None, None);
        assert_eq!(c.metrics_host, "127.0.0.1");
    }

    #[test]
    fn from_vars_invalid_metrics_port_falls_back_to_default() {
        let c = Config::from_vars(None, None, None, Some("not_a_port"), None, None, None, None, None, None);
        assert_eq!(c.metrics_port, 9090);
    }

    #[test]
    fn metrics_listen_addr_formats_correctly() {
        let c = Config::default();
        assert_eq!(c.metrics_listen_addr(), "0.0.0.0:9090");
    }

    #[test]
    fn persist_defaults_are_disabled() {
        let c = Config::default();
        assert!(c.persist_path.is_none());
        assert_eq!(c.persist_interval_secs, DEFAULT_PERSIST_INTERVAL_SECS);
    }

    #[test]
    fn from_vars_persist_path_set() {
        let c = Config::from_vars(None, None, None, None, None, Some("/tmp/kvns.bin"), None, None, None, None);
        assert_eq!(c.persist_path.as_deref(), Some("/tmp/kvns.bin"));
    }

    #[test]
    fn from_vars_persist_interval_override() {
        let c = Config::from_vars(None, None, None, None, None, None, Some("60"), None, None, None);
        assert_eq!(c.persist_interval_secs, 60);
    }

    #[test]
    fn from_vars_persist_interval_invalid_falls_back_to_default() {
        let c = Config::from_vars(None, None, None, None, None, None, Some("not_a_number"), None, None, None);
        assert_eq!(c.persist_interval_secs, DEFAULT_PERSIST_INTERVAL_SECS);
    }

    // ── Eviction config ───────────────────────────────────────────────────────

    #[test]
    fn eviction_defaults() {
        let c = Config::default();
        assert_eq!(c.eviction_threshold, 1.0);
        assert_eq!(c.eviction_policy, EvictionPolicy::None);
        assert!(c.namespace_eviction_policies.is_empty());
    }

    #[test]
    fn eviction_policy_from_str_parses_lru() {
        assert_eq!(EvictionPolicy::from_str("lru"), Some(EvictionPolicy::Lru));
    }

    #[test]
    fn eviction_policy_from_str_parses_mru() {
        assert_eq!(EvictionPolicy::from_str("mru"), Some(EvictionPolicy::Mru));
    }

    #[test]
    fn eviction_policy_from_str_parses_none() {
        assert_eq!(EvictionPolicy::from_str("none"), Some(EvictionPolicy::None));
    }

    #[test]
    fn eviction_policy_from_str_case_insensitive() {
        assert_eq!(EvictionPolicy::from_str("LRU"), Some(EvictionPolicy::Lru));
        assert_eq!(EvictionPolicy::from_str("MRU"), Some(EvictionPolicy::Mru));
        assert_eq!(EvictionPolicy::from_str("NONE"), Some(EvictionPolicy::None));
    }

    #[test]
    fn eviction_policy_from_str_invalid_returns_none() {
        assert_eq!(EvictionPolicy::from_str("fifo"), None);
        assert_eq!(EvictionPolicy::from_str(""), None);
    }

    #[test]
    fn parse_ns_eviction_single_pair() {
        let map = Config::parse_ns_eviction("ns1:lru");
        assert_eq!(map.get("ns1"), Some(&EvictionPolicy::Lru));
    }

    #[test]
    fn parse_ns_eviction_multiple_pairs() {
        let map = Config::parse_ns_eviction("ns1:lru,ns2:mru");
        assert_eq!(map.get("ns1"), Some(&EvictionPolicy::Lru));
        assert_eq!(map.get("ns2"), Some(&EvictionPolicy::Mru));
    }

    #[test]
    fn parse_ns_eviction_empty_string_returns_empty_map() {
        let map = Config::parse_ns_eviction("");
        assert!(map.is_empty());
    }

    #[test]
    fn parse_ns_eviction_invalid_policy_skipped() {
        let map = Config::parse_ns_eviction("ns1:lru,ns2:fifo,ns3:mru");
        assert_eq!(map.get("ns1"), Some(&EvictionPolicy::Lru));
        assert!(map.get("ns2").is_none());
        assert_eq!(map.get("ns3"), Some(&EvictionPolicy::Mru));
    }

    #[test]
    fn from_vars_eviction_threshold_override() {
        let c = Config::from_vars(None, None, None, None, None, None, None, Some("0.75"), None, None);
        assert_eq!(c.eviction_threshold, 0.75);
    }

    #[test]
    fn from_vars_eviction_policy_override() {
        let c = Config::from_vars(None, None, None, None, None, None, None, None, Some("lru"), None);
        assert_eq!(c.eviction_policy, EvictionPolicy::Lru);
    }

    #[test]
    fn from_vars_ns_eviction_override() {
        let c = Config::from_vars(None, None, None, None, None, None, None, None, None, Some("ns1:lru,ns2:mru"));
        assert_eq!(c.namespace_eviction_policies.get("ns1"), Some(&EvictionPolicy::Lru));
        assert_eq!(c.namespace_eviction_policies.get("ns2"), Some(&EvictionPolicy::Mru));
    }

    #[test]
    fn from_vars_invalid_eviction_threshold_falls_back_to_default() {
        let c = Config::from_vars(None, None, None, None, None, None, None, Some("not_a_float"), None, None);
        assert_eq!(c.eviction_threshold, 1.0);
    }

    #[test]
    fn from_vars_invalid_eviction_policy_falls_back_to_default() {
        let c = Config::from_vars(None, None, None, None, None, None, None, None, Some("fifo"), None);
        assert_eq!(c.eviction_policy, EvictionPolicy::None);
    }
}
