//! An object store built from a connection's own storage options, with
//! nothing read from the process environment. delta-rs fills options a config
//! leaves out (keys, tokens, service-account files) from the environment, so a
//! task that may not use the host's identity reads its table through this
//! store instead.

#[cfg(any(feature = "s3", feature = "azure", feature = "gcs"))]
use deltalake::logstore::object_store::ObjectStoreScheme;
use deltalake::logstore::object_store::{self, ObjectStore};
use std::collections::HashMap;
use std::sync::Arc;

/// `options` with keys lower-cased and trailing slashes trimmed from URL
/// values, as delta-rs prepares them.
#[cfg_attr(
    not(any(feature = "s3", feature = "azure", feature = "gcs")),
    allow(dead_code)
)]
pub(crate) fn normalized(options: &HashMap<String, String>) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = options
        .iter()
        .map(|(k, v)| {
            let key = k.to_ascii_lowercase();
            let trim =
                v.starts_with("http://") || v.starts_with("https://") || key.ends_with("_url");
            let value = if trim {
                v.trim_end_matches('/')
            } else {
                v.as_str()
            };
            (key, value.to_string())
        })
        .collect();
    out.sort();
    out
}

/// The store for `url` built from `options` alone, or `None` when the scheme
/// is local or its backend is not compiled in (delta-rs builds those, and
/// reads nothing from the environment for a local table).
pub(crate) fn build(
    url: &url::Url,
    options: &HashMap<String, String>,
) -> object_store::Result<Option<Arc<dyn ObjectStore>>> {
    #[cfg(any(feature = "s3", feature = "azure", feature = "gcs"))]
    {
        let Ok((scheme, _)) = ObjectStoreScheme::parse(url) else {
            return Ok(None);
        };
        let options = normalized(options);
        match scheme {
            #[cfg(feature = "s3")]
            ObjectStoreScheme::AmazonS3 => return s3(url, &options).map(Some),
            #[cfg(feature = "azure")]
            ObjectStoreScheme::MicrosoftAzure => return azure(url, &options).map(Some),
            #[cfg(feature = "gcs")]
            ObjectStoreScheme::GoogleCloudStorage => return gcs(url, &options).map(Some),
            _ => {}
        }
    }
    let _ = (url, options);
    Ok(None)
}

#[cfg(feature = "s3")]
fn s3(url: &url::Url, options: &[(String, String)]) -> object_store::Result<Arc<dyn ObjectStore>> {
    use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
    use std::str::FromStr;
    let mut builder = AmazonS3Builder::new().with_url(url.as_str());
    let mut copy_set = false;
    for (key, value) in options {
        if let Ok(key) = AmazonS3ConfigKey::from_str(key) {
            copy_set |= matches!(key, AmazonS3ConfigKey::CopyIfNotExists);
            builder = builder.with_config(key, value);
        }
    }
    if !copy_set {
        builder = builder.with_config(AmazonS3ConfigKey::CopyIfNotExists, "multipart");
    }
    Ok(Arc::new(builder.build()?))
}

#[cfg(feature = "azure")]
fn azure(
    url: &url::Url,
    options: &[(String, String)],
) -> object_store::Result<Arc<dyn ObjectStore>> {
    use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
    use std::str::FromStr;
    let mut builder = MicrosoftAzureBuilder::new().with_url(url.as_str());
    for (key, value) in options {
        if let Ok(key) = AzureConfigKey::from_str(key) {
            builder = builder.with_config(key, value);
        }
    }
    Ok(Arc::new(builder.build()?))
}

#[cfg(feature = "gcs")]
fn gcs(url: &url::Url, options: &[(String, String)]) -> object_store::Result<Arc<dyn ObjectStore>> {
    use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
    use std::str::FromStr;
    let mut builder = GoogleCloudStorageBuilder::new().with_url(url.as_str());
    for (key, value) in options {
        if let Ok(key) = GoogleConfigKey::from_str(key) {
            builder = builder.with_config(key, value);
        }
    }
    Ok(Arc::new(builder.build()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_are_lowercased_and_url_values_trimmed() {
        let options: HashMap<String, String> = [
            ("AWS_ENDPOINT_URL", "http://minio:9000/"),
            ("AWS_REGION", "eu-west-1"),
            ("custom", "https://x/"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
        assert_eq!(
            normalized(&options),
            vec![
                ("aws_endpoint_url".into(), "http://minio:9000".into()),
                ("aws_region".into(), "eu-west-1".into()),
                ("custom".into(), "https://x".into()),
            ]
        );
    }

    #[test]
    fn a_local_table_is_left_to_delta_rs() {
        let url = url::Url::parse("file:///tmp/t").unwrap();
        assert!(build(&url, &HashMap::new()).unwrap().is_none());
    }
}
