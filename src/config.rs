use std::collections::HashMap;

pub const DEFAULT_MEMORY_LIMIT: usize = 1_073_741_824; // 1 GiB
pub const DEFAULT_PERSIST_INTERVAL_SECS: u64 = 300; // 5 minutes
pub const DEFAULT_MEMORY_CLAMP_LIMIT: usize = 70; // 70%
pub const DEFAULT_MAX_CLIENTS: usize = 10_000;
pub const DEFAULT_MAX_RESP_ARGS: usize = 1_024;
pub const DEFAULT_MAX_RESP_BULK_LEN: usize = 16 * 1024 * 1024; // 16 MiB
pub const DEFAULT_MAX_RESP_INLINE_LEN: usize = 64 * 1024; // 64 KiB

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum EvictionPolicy {
    #[default]
    None,
    /// Least-frequently-used: evict lowest-hit keys first.
    Lfu,
    /// Most-frequently-used: evict highest-hit keys first.
    Mfu,
    ExpireAfterRead,
}

impl EvictionPolicy {
    fn from_str(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "lfu" => Some(EvictionPolicy::Lfu),
            "mfu" => Some(EvictionPolicy::Mfu),
            "none" => Some(EvictionPolicy::None),
            "ear" | "expire_after_read" | "expireafterread" => {
                Some(EvictionPolicy::ExpireAfterRead)
            }
            _ => None,
        }
    }
}

/// Invalid configuration that should stop the server from starting rather than
/// have it run with a surprising default (e.g. eviction silently disabled).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// `KVNS_EVICTION_POLICY` was set to an unrecognised value.
    InvalidEvictionPolicy(String),
    /// A `namespace:policy` pair in `KVNS_NS_EVICTION` named an unknown policy.
    InvalidNsEvictionPolicy { namespace: String, policy: String },
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::InvalidEvictionPolicy(value) => write!(
                f,
                "invalid KVNS_EVICTION_POLICY {value:?}; expected one of: none, lfu, mfu, ear"
            ),
            ConfigError::InvalidNsEvictionPolicy { namespace, policy } => write!(
                f,
                "invalid eviction policy {policy:?} for namespace {namespace:?} in \
                 KVNS_NS_EVICTION; expected one of: none, lfu, mfu, ear"
            ),
        }
    }
}

impl std::error::Error for ConfigError {}

#[derive(Clone, Debug)]
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
    /// Route supported commands through the experimental sharded backend.
    pub sharded_mode: bool,
    /// Number of lock shards in sharded mode.
    pub shard_count: usize,
    /// When true, entry values are wrapped in `Arc` so periodic persistence
    /// snapshots clone pointers rather than deep-copying payload bytes.
    /// Trades ~3–5% write throughput for ~58% lower tail latency while a
    /// snapshot is in flight.  Disable if persistence is off and raw write
    /// throughput matters more than snapshot-time tail latency.
    pub shared_values: bool,
    /// Maximum concurrent client connections.
    pub max_clients: usize,
    /// Maximum RESP array element count accepted per command.
    pub max_resp_args: usize,
    /// Maximum RESP bulk-string byte length accepted.
    pub max_resp_bulk_len: usize,
    /// Maximum RESP inline/header line byte length accepted.
    pub max_resp_inline_len: usize,
}

impl Default for Config {
    fn default() -> Self {
        let shard_count = std::thread::available_parallelism()
            .map(|n| n.get().saturating_mul(4))
            .unwrap_or(16);
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
            sharded_mode: false,
            shard_count: shard_count.max(1),
            shared_values: true,
            max_clients: DEFAULT_MAX_CLIENTS,
            max_resp_args: DEFAULT_MAX_RESP_ARGS,
            max_resp_bulk_len: DEFAULT_MAX_RESP_BULK_LEN,
            max_resp_inline_len: DEFAULT_MAX_RESP_INLINE_LEN,
        }
    }
}

impl Config {
    fn env_parse<T: std::str::FromStr>(var: &str, default: T, validate: impl Fn(&T) -> bool) -> T {
        std::env::var(var)
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|v| validate(v))
            .unwrap_or(default)
    }

    pub fn from_env() -> Result<Self, ConfigError> {
        let mut cfg = Self::from_vars(
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
        )?;
        cfg.sharded_mode = std::env::var("KVNS_SHARDED_MODE")
            .ok()
            .as_deref()
            .and_then(Self::parse_bool)
            .unwrap_or(cfg.sharded_mode);
        cfg.shard_count = Self::env_parse("KVNS_SHARD_COUNT", cfg.shard_count, |v| *v > 0);
        cfg.shared_values = std::env::var("KVNS_SHARED_VALUES")
            .ok()
            .as_deref()
            .and_then(Self::parse_bool)
            .unwrap_or(cfg.shared_values);
        cfg.max_clients = Self::env_parse("KVNS_MAX_CLIENTS", cfg.max_clients, |v| *v > 0);
        cfg.max_resp_args = Self::env_parse("KVNS_MAX_RESP_ARGS", cfg.max_resp_args, |v| *v > 0);
        cfg.max_resp_bulk_len = Self::env_parse("KVNS_MAX_RESP_BULK_LEN", cfg.max_resp_bulk_len, |v| *v > 0);
        cfg.max_resp_inline_len = Self::env_parse("KVNS_MAX_RESP_INLINE_LEN", cfg.max_resp_inline_len, |v| *v > 0);
        Ok(cfg)
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
    ) -> Result<Self, ConfigError> {
        let defaults = Self::default();
        // Eviction settings fail loudly on a bad value rather than silently
        // falling back to "no eviction" — a misconfigured cache should not
        // quietly behave as if eviction were disabled.
        let eviction_policy = match eviction_policy {
            None => defaults.eviction_policy,
            Some(s) => EvictionPolicy::from_str(s)
                .ok_or_else(|| ConfigError::InvalidEvictionPolicy(s.to_string()))?,
        };
        let namespace_eviction_policies = match ns_eviction {
            None => HashMap::new(),
            Some(s) => Self::parse_ns_eviction(s)?,
        };
        Ok(Self {
            port: port.and_then(|s| s.parse().ok()).unwrap_or(defaults.port),
            host: host.map(|s| s.to_string()).unwrap_or(defaults.host),
            memory_limit: Self::parse_memory_limit(memory_limit, defaults.memory_limit),
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
            eviction_policy,
            namespace_eviction_policies,
            sharded_mode: defaults.sharded_mode,
            shard_count: defaults.shard_count,
            shared_values: defaults.shared_values,
            max_clients: defaults.max_clients,
            max_resp_args: defaults.max_resp_args,
            max_resp_bulk_len: defaults.max_resp_bulk_len,
            max_resp_inline_len: defaults.max_resp_inline_len,
        })
    }

    fn parse_bool(s: &str) -> Option<bool> {
        match s.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        }
    }

    fn parse_memory_limit(memory_limit: Option<&str>, default_limit: usize) -> usize {
        let Some(configured_limit) = memory_limit.and_then(|s| s.parse::<usize>().ok()) else {
            return default_limit;
        };

        match Self::total_system_memory_bytes() {
            Some(total_memory_bytes) => {
                Self::clamp_memory_limit_to_system(configured_limit, total_memory_bytes)
            }
            // If we cannot determine total memory, preserve previous behavior.
            None if configured_limit == 0 => default_limit,
            None => configured_limit,
        }
    }

    fn clamp_memory_limit_to_system(configured_limit: usize, total_memory_bytes: usize) -> usize {
        let cap = total_memory_bytes.saturating_mul(DEFAULT_MEMORY_CLAMP_LIMIT) / 100;
        if configured_limit == 0 || configured_limit > cap {
            cap
        } else {
            configured_limit
        }
    }

    fn total_system_memory_bytes() -> Option<usize> {
        Self::linux_total_memory_bytes().or_else(Self::sysctl_total_memory_bytes)
    }

    fn linux_total_memory_bytes() -> Option<usize> {
        let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
        let line = meminfo.lines().find(|line| line.starts_with("MemTotal:"))?;
        let kilobytes = line.split_whitespace().nth(1)?.parse::<usize>().ok()?;
        kilobytes.checked_mul(1024)
    }

    fn sysctl_total_memory_bytes() -> Option<usize> {
        use std::sync::OnceLock;
        // Cache the result: forking sysctl on every call would be wasteful and
        // blocks the calling thread.  This function is only called at startup,
        // but the OnceLock protects against accidental future re-use.
        static CACHED: OnceLock<Option<usize>> = OnceLock::new();
        *CACHED.get_or_init(|| {
            let output = std::process::Command::new("sysctl")
                .args(["-n", "hw.memsize"])
                .output()
                .ok()?;
            if !output.status.success() {
                return None;
            }
            String::from_utf8(output.stdout)
                .ok()
                .and_then(|s| s.trim().parse::<usize>().ok())
        })
    }

    /// Parse `"ns1:lfu,ns2:mfu"` into a `HashMap<String, EvictionPolicy>`.
    ///
    /// Empty segments (e.g. a trailing comma) are ignored, but a segment that
    /// names an unknown policy is rejected rather than silently dropped.
    fn parse_ns_eviction(s: &str) -> Result<HashMap<String, EvictionPolicy>, ConfigError> {
        s.split(',')
            .filter(|pair| !pair.trim().is_empty())
            .map(|pair| {
                let mut parts = pair.splitn(2, ':');
                let namespace = parts.next().unwrap_or("").trim().to_string();
                let policy_str = parts.next().unwrap_or("").trim();
                let policy = EvictionPolicy::from_str(policy_str).ok_or_else(|| {
                    ConfigError::InvalidNsEvictionPolicy {
                        namespace: namespace.clone(),
                        policy: policy_str.to_string(),
                    }
                })?;
                Ok((namespace, policy))
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

    /// Thin wrapper for tests that expect a valid configuration; panics if
    /// `from_vars` rejects the inputs. Tests that exercise the error path call
    /// `Config::from_vars` directly and inspect the `Result`.
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
    ) -> Config {
        Config::from_vars(
            port,
            host,
            memory_limit,
            metrics_port,
            metrics_host,
            persist_path,
            persist_interval,
            eviction_threshold,
            eviction_policy,
            ns_eviction,
        )
        .expect("test config should be valid")
    }

    #[test]
    fn defaults_are_correct() {
        let c = Config::default();
        assert_eq!(c.port, 6480);
        assert_eq!(c.host, "0.0.0.0");
        assert_eq!(c.memory_limit, DEFAULT_MEMORY_LIMIT);
        assert_eq!(c.max_clients, DEFAULT_MAX_CLIENTS);
        assert_eq!(c.max_resp_args, DEFAULT_MAX_RESP_ARGS);
        assert_eq!(c.max_resp_bulk_len, DEFAULT_MAX_RESP_BULK_LEN);
        assert_eq!(c.max_resp_inline_len, DEFAULT_MAX_RESP_INLINE_LEN);
    }

    #[test]
    fn from_vars_all_none_returns_defaults() {
        let c = from_vars(None, None, None, None, None, None, None, None, None, None);
        assert_eq!(c.port, 6480);
        assert_eq!(c.host, "0.0.0.0");
        assert_eq!(c.memory_limit, DEFAULT_MEMORY_LIMIT);
        assert_eq!(c.max_clients, DEFAULT_MAX_CLIENTS);
        assert_eq!(c.max_resp_args, DEFAULT_MAX_RESP_ARGS);
        assert_eq!(c.max_resp_bulk_len, DEFAULT_MAX_RESP_BULK_LEN);
        assert_eq!(c.max_resp_inline_len, DEFAULT_MAX_RESP_INLINE_LEN);
    }

    #[test]
    fn from_vars_port_override() {
        let c = from_vars(
            Some("7000"),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert_eq!(c.port, 7000);
    }

    #[test]
    fn from_vars_host_override() {
        let c = from_vars(
            None,
            Some("127.0.0.1"),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert_eq!(c.host, "127.0.0.1");
    }

    #[test]
    fn from_vars_memory_limit_override() {
        let c = from_vars(
            None,
            None,
            Some("2048"),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert_eq!(c.memory_limit, 2048);
    }

    #[test]
    fn clamp_memory_limit_keeps_values_within_cap() {
        assert_eq!(Config::clamp_memory_limit_to_system(69, 100), 69);
        assert_eq!(Config::clamp_memory_limit_to_system(70, 100), 70);
    }

    #[test]
    fn clamp_memory_limit_caps_values_above_cap() {
        assert_eq!(
            Config::clamp_memory_limit_to_system(71, 100),
            DEFAULT_MEMORY_CLAMP_LIMIT
        );
        assert_eq!(
            Config::clamp_memory_limit_to_system(200, 100),
            DEFAULT_MEMORY_CLAMP_LIMIT
        );
    }

    #[test]
    fn clamp_memory_limit_zero_uses_cap() {
        assert_eq!(
            Config::clamp_memory_limit_to_system(0, 100),
            DEFAULT_MEMORY_CLAMP_LIMIT
        );
    }

    #[test]
    fn from_vars_zero_memory_limit_uses_detected_cap() {
        let c = from_vars(
            None,
            None,
            Some("0"),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );

        let expected = Config::total_system_memory_bytes()
            .map(|total| Config::clamp_memory_limit_to_system(0, total))
            .unwrap_or(DEFAULT_MEMORY_LIMIT);
        assert_eq!(c.memory_limit, expected);
    }

    #[test]
    fn from_vars_memory_limit_above_detected_cap_is_clamped() {
        let Some(total_memory_bytes) = Config::total_system_memory_bytes() else {
            return;
        };

        let cap = Config::clamp_memory_limit_to_system(0, total_memory_bytes);
        let configured = cap.saturating_add(1);
        let configured_str = configured.to_string();
        let c = from_vars(
            None,
            None,
            Some(&configured_str),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert_eq!(c.memory_limit, cap);
    }

    #[test]
    fn from_vars_invalid_port_falls_back_to_default() {
        let c = from_vars(
            Some("not_a_port"),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert_eq!(c.port, 6480);
    }

    #[test]
    fn from_vars_invalid_memory_limit_falls_back_to_default() {
        let c = from_vars(
            None,
            None,
            Some("not_a_number"),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert_eq!(c.memory_limit, DEFAULT_MEMORY_LIMIT);
    }

    #[test]
    fn listen_addr_formats_correctly() {
        let c = Config::default();
        assert_eq!(c.listen_addr(), "0.0.0.0:6480");
    }

    #[test]
    fn listen_addr_custom_host_and_port() {
        let c = from_vars(
            Some("9000"),
            Some("127.0.0.1"),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
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
        let c = from_vars(
            None,
            None,
            None,
            Some("9999"),
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert_eq!(c.metrics_port, 9999);
    }

    #[test]
    fn from_vars_metrics_host_override() {
        let c = from_vars(
            None,
            None,
            None,
            None,
            Some("127.0.0.1"),
            None,
            None,
            None,
            None,
            None,
        );
        assert_eq!(c.metrics_host, "127.0.0.1");
    }

    #[test]
    fn from_vars_invalid_metrics_port_falls_back_to_default() {
        let c = from_vars(
            None,
            None,
            None,
            Some("not_a_port"),
            None,
            None,
            None,
            None,
            None,
            None,
        );
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
        let c = from_vars(
            None,
            None,
            None,
            None,
            None,
            Some("/tmp/kvns.bin"),
            None,
            None,
            None,
            None,
        );
        assert_eq!(c.persist_path.as_deref(), Some("/tmp/kvns.bin"));
    }

    #[test]
    fn from_vars_persist_interval_override() {
        let c = from_vars(
            None,
            None,
            None,
            None,
            None,
            None,
            Some("60"),
            None,
            None,
            None,
        );
        assert_eq!(c.persist_interval_secs, 60);
    }

    #[test]
    fn from_vars_persist_interval_invalid_falls_back_to_default() {
        let c = from_vars(
            None,
            None,
            None,
            None,
            None,
            None,
            Some("not_a_number"),
            None,
            None,
            None,
        );
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
    fn eviction_policy_from_str_parses_lfu() {
        assert_eq!(EvictionPolicy::from_str("lfu"), Some(EvictionPolicy::Lfu));
    }

    #[test]
    fn eviction_policy_from_str_parses_mfu() {
        assert_eq!(EvictionPolicy::from_str("mfu"), Some(EvictionPolicy::Mfu));
    }

    #[test]
    fn eviction_policy_from_str_parses_none() {
        assert_eq!(EvictionPolicy::from_str("none"), Some(EvictionPolicy::None));
    }

    #[test]
    fn eviction_policy_from_str_case_insensitive() {
        assert_eq!(EvictionPolicy::from_str("LFU"), Some(EvictionPolicy::Lfu));
        assert_eq!(EvictionPolicy::from_str("MFU"), Some(EvictionPolicy::Mfu));
        assert_eq!(EvictionPolicy::from_str("NONE"), Some(EvictionPolicy::None));
    }

    #[test]
    fn eviction_policy_from_str_invalid_returns_none() {
        assert_eq!(EvictionPolicy::from_str("fifo"), None);
        assert_eq!(EvictionPolicy::from_str(""), None);
    }

    #[test]
    fn eviction_policy_from_str_parses_ear() {
        assert_eq!(
            EvictionPolicy::from_str("ear"),
            Some(EvictionPolicy::ExpireAfterRead)
        );
        assert_eq!(
            EvictionPolicy::from_str("expire_after_read"),
            Some(EvictionPolicy::ExpireAfterRead)
        );
        assert_eq!(
            EvictionPolicy::from_str("expireafterread"),
            Some(EvictionPolicy::ExpireAfterRead)
        );
        assert_eq!(
            EvictionPolicy::from_str("EAR"),
            Some(EvictionPolicy::ExpireAfterRead)
        );
    }

    #[test]
    fn from_vars_ns_eviction_ear() {
        let c = from_vars(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some("session:ear"),
        );
        assert_eq!(
            c.namespace_eviction_policies.get("session"),
            Some(&EvictionPolicy::ExpireAfterRead)
        );
    }

    #[test]
    fn parse_ns_eviction_single_pair() {
        let map = Config::parse_ns_eviction("ns1:lfu").unwrap();
        assert_eq!(map.get("ns1"), Some(&EvictionPolicy::Lfu));
    }

    #[test]
    fn parse_ns_eviction_multiple_pairs() {
        let map = Config::parse_ns_eviction("ns1:lfu,ns2:mfu").unwrap();
        assert_eq!(map.get("ns1"), Some(&EvictionPolicy::Lfu));
        assert_eq!(map.get("ns2"), Some(&EvictionPolicy::Mfu));
    }

    #[test]
    fn parse_ns_eviction_empty_string_returns_empty_map() {
        let map = Config::parse_ns_eviction("").unwrap();
        assert!(map.is_empty());
    }

    #[test]
    fn parse_ns_eviction_trailing_comma_is_ignored() {
        let map = Config::parse_ns_eviction("ns1:lfu,").unwrap();
        assert_eq!(map.get("ns1"), Some(&EvictionPolicy::Lfu));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn parse_ns_eviction_invalid_policy_errors() {
        let err = Config::parse_ns_eviction("ns1:lfu,ns2:fifo,ns3:mfu").unwrap_err();
        assert_eq!(
            err,
            ConfigError::InvalidNsEvictionPolicy {
                namespace: "ns2".to_string(),
                policy: "fifo".to_string(),
            }
        );
    }

    #[test]
    fn from_vars_eviction_threshold_override() {
        let c = from_vars(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some("0.75"),
            None,
            None,
        );
        assert_eq!(c.eviction_threshold, 0.75);
    }

    #[test]
    fn from_vars_eviction_policy_override() {
        let c = from_vars(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some("lfu"),
            None,
        );
        assert_eq!(c.eviction_policy, EvictionPolicy::Lfu);
    }

    #[test]
    fn from_vars_ns_eviction_override() {
        let c = from_vars(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some("ns1:lfu,ns2:mfu"),
        );
        assert_eq!(
            c.namespace_eviction_policies.get("ns1"),
            Some(&EvictionPolicy::Lfu)
        );
        assert_eq!(
            c.namespace_eviction_policies.get("ns2"),
            Some(&EvictionPolicy::Mfu)
        );
    }

    #[test]
    fn from_vars_invalid_eviction_threshold_falls_back_to_default() {
        let c = from_vars(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some("not_a_float"),
            None,
            None,
        );
        assert_eq!(c.eviction_threshold, 1.0);
    }

    #[test]
    fn from_vars_invalid_eviction_policy_errors() {
        let err = Config::from_vars(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some("fifo"),
            None,
        )
        .unwrap_err();
        assert_eq!(
            err,
            ConfigError::InvalidEvictionPolicy("fifo".to_string())
        );
    }

    #[test]
    fn from_vars_invalid_ns_eviction_policy_errors() {
        let err = Config::from_vars(
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some("cache:bogus"),
        )
        .unwrap_err();
        assert_eq!(
            err,
            ConfigError::InvalidNsEvictionPolicy {
                namespace: "cache".to_string(),
                policy: "bogus".to_string(),
            }
        );
    }
}
