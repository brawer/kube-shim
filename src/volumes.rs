//! Generic ephemeral volumes (Phase 5): `spec.storageClassName` selects a
//! *performance tier*, not a reclaim policy -- nothing built here is ever
//! retained across job runs, so there's no `Delete`/`Retain` distinction to
//! make. See docs/IMPLEMENTATION_PLAN.md Phase 5 and "Generic Ephemeral
//! Volumes / Storage Tiers" under Key Implementation Details for the full
//! reasoning (in short: no real `StorageClass` resource, and no portable
//! numeric IOPS field either -- even real Kubernetes/CSI doesn't
//! standardize that, and these providers offer a handful of discrete
//! tiers, not a continuously dialable number).

pub const STORAGE_CLASS_STANDARD: &str = "kube-shim-standard";
pub const STORAGE_CLASS_FAST: &str = "kube-shim-fast";

/// The two hardcoded, internally known storage tiers. Deliberately not an
/// open/pluggable set -- see the module docs above.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageTier {
    Standard,
    Fast,
}

impl StorageTier {
    /// `None` (the field omitted entirely) and `Some(STORAGE_CLASS_STANDARD)`
    /// both mean the default tier, matching how an omitted
    /// `storageClassName` behaves in real Kubernetes. Any value other than
    /// the two known ones is rejected by the caller (see `admission.rs`),
    /// not silently coerced to a default.
    pub fn parse(storage_class_name: Option<&str>) -> Result<Self, &str> {
        match storage_class_name {
            None => Ok(Self::Standard),
            Some(STORAGE_CLASS_STANDARD) => Ok(Self::Standard),
            Some(STORAGE_CLASS_FAST) => Ok(Self::Fast),
            Some(other) => Err(other),
        }
    }

    /// UpCloud's own storage tier name for this performance tier (Phase 7+:
    /// `CloudProvider` implementations pass this straight through to the
    /// real `POST /1.3/storage` call). A small, hardcoded `match`, not a
    /// general pluggable-parameter mechanism -- see the module docs above.
    pub fn upcloud_tier(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Fast => "maxiops",
        }
    }
}

/// Parses a Kubernetes resource quantity (as used for
/// `resources.requests.storage`, e.g. `"250Gi"`, `"10G"`, `"500Mi"`) into a
/// whole number of gigabytes, rounding up so the provisioned volume is
/// never smaller than what was actually requested. Only the suffixes
/// that make sense for a scratch-disk-sized ephemeral volume are
/// supported -- binary (`Ki`/`Mi`/`Gi`/`Ti`) and decimal (`K`/`M`/`G`/`T`)
/// -- not the full Kubernetes quantity grammar (exponent notation,
/// sub-byte fractions, etc.), which nothing in this project's workloads
/// needs.
pub fn parse_storage_quantity_gb(quantity: &str) -> Result<u32, String> {
    let quantity = quantity.trim();
    let (number_str, multiplier_bytes): (&str, f64) = if let Some(n) = quantity.strip_suffix("Ki") {
        (n, 1024.0)
    } else if let Some(n) = quantity.strip_suffix("Mi") {
        (n, 1024.0 * 1024.0)
    } else if let Some(n) = quantity.strip_suffix("Gi") {
        (n, 1024.0 * 1024.0 * 1024.0)
    } else if let Some(n) = quantity.strip_suffix("Ti") {
        (n, 1024.0 * 1024.0 * 1024.0 * 1024.0)
    } else if let Some(n) = quantity.strip_suffix('K') {
        (n, 1_000.0)
    } else if let Some(n) = quantity.strip_suffix('M') {
        (n, 1_000_000.0)
    } else if let Some(n) = quantity.strip_suffix('G') {
        (n, 1_000_000_000.0)
    } else if let Some(n) = quantity.strip_suffix('T') {
        (n, 1_000_000_000_000.0)
    } else {
        (quantity, 1.0)
    };

    let number: f64 = number_str
        .parse()
        .map_err(|_| format!("not a valid storage quantity: \"{quantity}\""))?;
    if number < 0.0 {
        return Err(format!(
            "storage quantity must not be negative: \"{quantity}\""
        ));
    }

    let gb = (number * multiplier_bytes) / (1024.0 * 1024.0 * 1024.0);
    Ok(gb.ceil() as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_tier_defaults_to_standard_when_omitted() {
        assert_eq!(StorageTier::parse(None), Ok(StorageTier::Standard));
    }

    #[test]
    fn test_storage_tier_standard_explicit() {
        assert_eq!(
            StorageTier::parse(Some(STORAGE_CLASS_STANDARD)),
            Ok(StorageTier::Standard)
        );
    }

    #[test]
    fn test_storage_tier_fast() {
        assert_eq!(
            StorageTier::parse(Some(STORAGE_CLASS_FAST)),
            Ok(StorageTier::Fast)
        );
    }

    #[test]
    fn test_storage_tier_unknown_rejected() {
        assert_eq!(StorageTier::parse(Some("bogus")), Err("bogus"));
    }

    #[test]
    fn test_upcloud_tier_mapping() {
        assert_eq!(StorageTier::Standard.upcloud_tier(), "standard");
        assert_eq!(StorageTier::Fast.upcloud_tier(), "maxiops");
    }

    #[test]
    fn test_parse_storage_quantity_binary_suffixes() {
        assert_eq!(parse_storage_quantity_gb("250Gi"), Ok(250));
        assert_eq!(parse_storage_quantity_gb("1Ti"), Ok(1024));
        assert_eq!(parse_storage_quantity_gb("512Mi"), Ok(1)); // rounds up
        assert_eq!(parse_storage_quantity_gb("1Ki"), Ok(1)); // rounds up
    }

    #[test]
    fn test_parse_storage_quantity_decimal_suffixes() {
        assert_eq!(parse_storage_quantity_gb("10G"), Ok(10)); // decimal G < binary Gi
        assert_eq!(parse_storage_quantity_gb("1T"), Ok(932));
    }

    #[test]
    fn test_parse_storage_quantity_no_suffix_is_bytes() {
        assert_eq!(parse_storage_quantity_gb("1073741824"), Ok(1));
    }

    #[test]
    fn test_parse_storage_quantity_rejects_garbage() {
        assert!(parse_storage_quantity_gb("not-a-number").is_err());
        assert!(parse_storage_quantity_gb("").is_err());
    }

    #[test]
    fn test_parse_storage_quantity_rejects_negative() {
        assert!(parse_storage_quantity_gb("-5Gi").is_err());
    }
}
