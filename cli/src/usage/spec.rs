//! The top-level `usage:` block (#704): the pricing table cost estimates are
//! computed from, and the hosted-ELT comparison rate.
//!
//! Every figure here is a **rate the operator chooses**; the shipped
//! defaults are public list prices at the time of writing, in USD, and every
//! estimate faucet prints is labelled as an estimate whose inputs are shown.
//! No default guesses a region or a currency beyond that: a deployment in
//! another region or currency sets its own rates.

use faucet_core::JsonSchema;
use serde::{Deserialize, Serialize};

/// The `usage:` block.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UsageSpec {
    /// A YAML/JSON file holding a [`PricingSpec`], merged under the inline
    /// `pricing:` (inline wins). Relative to the working directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pricing_file: Option<String>,
    /// Inline pricing overrides. Any field left unset keeps the shipped
    /// default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pricing: Option<PricingOverrides>,
}

/// Rates cost estimates are computed from. All optional in config
/// ([`PricingOverrides`]); resolved with [`PricingSpec::default`] as the base.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PricingSpec {
    /// ISO 4217 code the rates are in. Informational — faucet never converts.
    pub currency: String,
    /// Price per GB of data leaving the source's cloud. Applied to the
    /// estimated bytes read when the source and sink kinds are not both
    /// local files. `0` (the default) = not charged; set it when your
    /// pipelines cross a cloud boundary.
    pub egress_per_gb: f64,
    /// Object storage request prices (S3 / GCS / Azure Blob), per 1,000
    /// requests: `read` covers list/get/head, `write` covers put.
    pub object_storage: ObjectStoragePricing,
    /// Warehouse compute.
    pub warehouse: WarehousePricing,
    /// What a per-row-priced hosted ELT service would charge for the same
    /// records written, per million rows. Powers the `hosted_equivalent`
    /// column; an estimate of a *different* product's bill, shown only next
    /// to its inputs.
    pub hosted_elt_per_million_rows: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObjectStoragePricing {
    pub read_per_1k_requests: f64,
    pub write_per_1k_requests: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WarehousePricing {
    /// BigQuery on-demand analysis, per TiB of bytes billed / processed.
    pub bigquery_per_tib_scanned: f64,
    /// BigQuery streaming inserts, per GiB streamed.
    pub bigquery_streaming_per_gib: f64,
    /// Snowflake, per credit (when a connector reports `credits`).
    pub snowflake_per_credit: f64,
}

impl Default for PricingSpec {
    fn default() -> Self {
        Self {
            currency: "USD".to_string(),
            egress_per_gb: 0.0,
            object_storage: ObjectStoragePricing {
                read_per_1k_requests: 0.0004,
                write_per_1k_requests: 0.005,
            },
            warehouse: WarehousePricing {
                bigquery_per_tib_scanned: 6.25,
                bigquery_streaming_per_gib: 0.05,
                snowflake_per_credit: 3.0,
            },
            hosted_elt_per_million_rows: 15.0,
        }
    }
}

/// The inline / file form of [`PricingSpec`]: every field optional.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PricingOverrides {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub currency: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub egress_per_gb: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_storage: Option<ObjectStorageOverrides>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warehouse: Option<WarehouseOverrides>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hosted_elt_per_million_rows: Option<f64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObjectStorageOverrides {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_per_1k_requests: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_per_1k_requests: Option<f64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct WarehouseOverrides {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bigquery_per_tib_scanned: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bigquery_streaming_per_gib: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snowflake_per_credit: Option<f64>,
}

impl PricingOverrides {
    /// Apply these overrides on top of `base`.
    pub fn apply(&self, mut base: PricingSpec) -> PricingSpec {
        if let Some(c) = &self.currency {
            base.currency = c.clone();
        }
        if let Some(v) = self.egress_per_gb {
            base.egress_per_gb = v;
        }
        if let Some(o) = &self.object_storage {
            if let Some(v) = o.read_per_1k_requests {
                base.object_storage.read_per_1k_requests = v;
            }
            if let Some(v) = o.write_per_1k_requests {
                base.object_storage.write_per_1k_requests = v;
            }
        }
        if let Some(w) = &self.warehouse {
            if let Some(v) = w.bigquery_per_tib_scanned {
                base.warehouse.bigquery_per_tib_scanned = v;
            }
            if let Some(v) = w.bigquery_streaming_per_gib {
                base.warehouse.bigquery_streaming_per_gib = v;
            }
            if let Some(v) = w.snowflake_per_credit {
                base.warehouse.snowflake_per_credit = v;
            }
        }
        if let Some(v) = self.hosted_elt_per_million_rows {
            base.hosted_elt_per_million_rows = v;
        }
        base
    }
}

impl PricingSpec {
    /// Every rate must be finite and non-negative.
    pub fn validate(&self) -> Result<(), String> {
        let rates = [
            ("egress_per_gb", self.egress_per_gb),
            (
                "object_storage.read_per_1k_requests",
                self.object_storage.read_per_1k_requests,
            ),
            (
                "object_storage.write_per_1k_requests",
                self.object_storage.write_per_1k_requests,
            ),
            (
                "warehouse.bigquery_per_tib_scanned",
                self.warehouse.bigquery_per_tib_scanned,
            ),
            (
                "warehouse.bigquery_streaming_per_gib",
                self.warehouse.bigquery_streaming_per_gib,
            ),
            (
                "warehouse.snowflake_per_credit",
                self.warehouse.snowflake_per_credit,
            ),
            (
                "hosted_elt_per_million_rows",
                self.hosted_elt_per_million_rows,
            ),
        ];
        for (name, v) in rates {
            if !v.is_finite() || v < 0.0 {
                return Err(format!(
                    "pricing.{name} must be a non-negative number, got {v}"
                ));
            }
        }
        if self.currency.trim().is_empty() {
            return Err("pricing.currency must not be empty".to_string());
        }
        Ok(())
    }
}

impl UsageSpec {
    /// Resolve the effective pricing: shipped defaults ← `pricing_file` ←
    /// inline `pricing`. `base_dir` resolves a relative `pricing_file`.
    pub fn resolve_pricing(
        &self,
        base_dir: Option<&std::path::Path>,
    ) -> Result<PricingSpec, String> {
        let mut pricing = PricingSpec::default();
        if let Some(file) = &self.pricing_file {
            let path = match base_dir {
                Some(d) if std::path::Path::new(file).is_relative() => d.join(file),
                _ => std::path::PathBuf::from(file),
            };
            let text = std::fs::read_to_string(&path)
                .map_err(|e| format!("usage.pricing_file `{}`: {e}", path.display()))?;
            let overrides: PricingOverrides = if path
                .extension()
                .and_then(|e| e.to_str())
                .is_some_and(|e| e.eq_ignore_ascii_case("json"))
            {
                serde_json::from_str(&text)
                    .map_err(|e| format!("usage.pricing_file `{}`: {e}", path.display()))?
            } else {
                serde_yaml::from_str(&text)
                    .map_err(|e| format!("usage.pricing_file `{}`: {e}", path.display()))?
            };
            pricing = overrides.apply(pricing);
        }
        if let Some(inline) = &self.pricing {
            pricing = inline.apply(pricing);
        }
        pricing.validate()?;
        Ok(pricing)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid_and_overrides_layer() {
        let base = PricingSpec::default();
        base.validate().unwrap();
        assert_eq!(base.currency, "USD");
        let spec: UsageSpec = serde_yaml::from_str(
            "pricing:\n  currency: EUR\n  egress_per_gb: 0.09\n  warehouse: { bigquery_per_tib_scanned: 5 }\n",
        )
        .unwrap();
        let p = spec.resolve_pricing(None).unwrap();
        assert_eq!(p.currency, "EUR");
        assert_eq!(p.egress_per_gb, 0.09);
        assert_eq!(p.warehouse.bigquery_per_tib_scanned, 5.0);
        assert_eq!(p.warehouse.snowflake_per_credit, 3.0, "untouched default");
        assert_eq!(p.object_storage.write_per_1k_requests, 0.005);
    }

    #[test]
    fn pricing_file_layers_under_inline_and_bad_rates_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("pricing.yaml"),
            "hosted_elt_per_million_rows: 40\nobject_storage: { read_per_1k_requests: 0.001 }\n",
        )
        .unwrap();
        let spec: UsageSpec = serde_yaml::from_str(
            "pricing_file: pricing.yaml\npricing: { hosted_elt_per_million_rows: 30 }\n",
        )
        .unwrap();
        let p = spec.resolve_pricing(Some(dir.path())).unwrap();
        assert_eq!(p.hosted_elt_per_million_rows, 30.0, "inline wins");
        assert_eq!(p.object_storage.read_per_1k_requests, 0.001, "file applied");

        std::fs::write(dir.path().join("bad.json"), r#"{"egress_per_gb": -1}"#).unwrap();
        let spec: UsageSpec = serde_yaml::from_str("pricing_file: bad.json\n").unwrap();
        let err = spec.resolve_pricing(Some(dir.path())).unwrap_err();
        assert!(err.contains("egress_per_gb"), "{err}");
        let spec: UsageSpec = serde_yaml::from_str("pricing_file: missing.yaml\n").unwrap();
        assert!(spec.resolve_pricing(Some(dir.path())).is_err());
        let spec: UsageSpec = serde_yaml::from_str("pricing: { currency: \" \" }\n").unwrap();
        assert!(spec.resolve_pricing(None).unwrap_err().contains("currency"));
    }
}
