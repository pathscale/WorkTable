/// Tuning applied to the nagoya scheduler for a generated table.
///
/// The names describe what the table does with its work rather than how the
/// scheduler is built: `Locality` keeps a task on the worker that woke it,
/// `Spread` fans it out, and `Throughput` trades wake-up latency for batching.
///
/// **This list is a mirror.** The registry is `worktable::runtime::Flavor`,
/// which carries the stable discriminants and the tuning each name selects.
/// This enum exists so the DSL crate can parse a flavor without depending on
/// the runtime crate, and [`Flavor::ALL`] is what a drift test compares
/// against.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Flavor {
    #[default]
    Locality,
    Spread,
    Throughput,
    LowLatency,
    WideInjector,
}

impl Flavor {
    /// Every flavor, in the registry's discriminant order.
    pub const ALL: [Flavor; 5] = [
        Self::Locality,
        Self::Spread,
        Self::Throughput,
        Self::LowLatency,
        Self::WideInjector,
    ];

    /// The spelling that selects this flavor, identical to the one
    /// `WT_DEFAULT_RUNTIME` takes.
    pub fn name(self) -> &'static str {
        match self {
            Self::Locality => "locality",
            Self::Spread => "spread",
            Self::Throughput => "throughput",
            Self::LowLatency => "low_latency",
            Self::WideInjector => "wide_injector",
        }
    }

    /// The flavor a spelling selects, or `None`.
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|flavor| flavor.name() == name)
    }

    /// The marker type `worktable::runtime` exports for this flavor.
    pub fn type_name(self) -> &'static str {
        match self {
            Self::Locality => "Locality",
            Self::Spread => "Spread",
            Self::Throughput => "Throughput",
            Self::LowLatency => "LowLatency",
            Self::WideInjector => "WideInjector",
        }
    }
}

/// Async runtime a generated table is built against.
///
/// Nagoya is the default, in its locality flavor, so a declaration that says
/// nothing about a runtime gets the same table as one that writes
/// `runtime: nagoya`.
///
/// There is deliberately no variant for a backend WorkTable cannot generate
/// against. `forte`, `blocking` and `bwos` are recognised by the parser only
/// so that naming one produces a message saying so; they are a list of strings
/// there rather than variants here, because an enum variant is a promise that
/// something downstream can switch on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum RuntimeBackend {
    Nagoya(Flavor),
    Tokio,
}

impl Default for RuntimeBackend {
    fn default() -> Self {
        Self::Nagoya(Flavor::Locality)
    }
}

impl RuntimeBackend {
    /// The keyword that selects this backend, without its flavor. The flavor
    /// is a separate word in the surface syntax, so it is a separate name
    /// here too.
    pub fn name(self) -> &'static str {
        match self {
            Self::Nagoya(_) => "nagoya",
            Self::Tokio => "tokio",
        }
    }
}
