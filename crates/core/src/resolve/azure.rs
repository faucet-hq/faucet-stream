//! Azure Storage connection strings (`AccountName=…;AccountKey=…;…`), read
//! with the rules the Azure Blob connectors' client applies, so a host
//! checking a string and the client opening it agree on every key.

/// A key of an Azure Storage connection string the client reads. Keys are
/// matched exactly, in this case; any other spelling is ignored by the
/// client.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[non_exhaustive]
pub enum AzureConnKey {
    AccountName,
    AccountKey,
    SharedAccessSignature,
    BlobEndpoint,
    EndpointSuffix,
    DefaultEndpointsProtocol,
    /// Read only when its value is `true` (any case).
    UseDevelopmentStorage,
}

impl AzureConnKey {
    /// Every key the client reads.
    pub const ALL: [AzureConnKey; 7] = [
        AzureConnKey::AccountName,
        AzureConnKey::AccountKey,
        AzureConnKey::SharedAccessSignature,
        AzureConnKey::BlobEndpoint,
        AzureConnKey::EndpointSuffix,
        AzureConnKey::DefaultEndpointsProtocol,
        AzureConnKey::UseDevelopmentStorage,
    ];

    /// The key as the client spells it.
    pub fn as_str(self) -> &'static str {
        match self {
            AzureConnKey::AccountName => "AccountName",
            AzureConnKey::AccountKey => "AccountKey",
            AzureConnKey::SharedAccessSignature => "SharedAccessSignature",
            AzureConnKey::BlobEndpoint => "BlobEndpoint",
            AzureConnKey::EndpointSuffix => "EndpointSuffix",
            AzureConnKey::DefaultEndpointsProtocol => "DefaultEndpointsProtocol",
            AzureConnKey::UseDevelopmentStorage => "UseDevelopmentStorage",
        }
    }

    fn parse(key: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|k| k.as_str() == key)
    }
}

impl std::fmt::Display for AzureConnKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Whether `name` is a valid Azure storage account name: 3 to 24 lowercase
/// letters and digits (the service's own naming rule).
pub fn is_valid_azure_account_name(name: &str) -> bool {
    (3..=24).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
}

/// An Azure Storage connection string as the client reads it:
///
/// - segments are separated by `;` and trimmed; an empty segment, one with no
///   `=`, or one with an empty value is skipped;
/// - a key runs to the first `=` and is trimmed; only the exact spellings of
///   [`AzureConnKey`] count;
/// - a value is the rest of the segment, trimmed (it may hold `=`);
/// - when a key repeats, the last one counts.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AzureConnSettings {
    entries: Vec<(AzureConnKey, String)>,
    other_case: Vec<String>,
}

impl AzureConnSettings {
    /// Read `value`.
    pub fn read(value: &str) -> Self {
        let mut out = Self::default();
        for segment in value.split(';') {
            let segment = segment.trim();
            if segment.is_empty() {
                continue;
            }
            let Some((key, v)) = segment.split_once('=') else {
                continue;
            };
            let (key, v) = (key.trim(), v.trim());
            if v.is_empty() {
                continue;
            }
            match AzureConnKey::parse(key) {
                Some(AzureConnKey::UseDevelopmentStorage) if !v.eq_ignore_ascii_case("true") => {}
                Some(k) => out.entries.push((k, v.to_string())),
                None => {
                    if AzureConnKey::ALL
                        .iter()
                        .any(|k| k.as_str().eq_ignore_ascii_case(key))
                        && !out.other_case.iter().any(|o| o == key)
                    {
                        out.other_case.push(key.to_string());
                    }
                }
            }
        }
        out
    }

    /// The keys the client reads, in the order written, with their values.
    pub fn entries(&self) -> &[(AzureConnKey, String)] {
        &self.entries
    }

    /// Every value written for `key`, in order.
    pub fn all(&self, key: AzureConnKey) -> impl Iterator<Item = &str> {
        self.entries
            .iter()
            .filter(move |(k, _)| *k == key)
            .map(|(_, v)| v.as_str())
    }

    /// The value the client uses for `key`: the last one written.
    pub fn get(&self, key: AzureConnKey) -> Option<&str> {
        self.all(key).last()
    }

    /// Keys written in another case than one the client reads
    /// (`accountname`, `BLOBENDPOINT`): the client ignores them.
    pub fn other_case_keys(&self) -> &[String] {
        &self.other_case
    }

    /// The `DefaultEndpointsProtocol`, lower case.
    pub fn protocol(&self) -> Option<String> {
        self.get(AzureConnKey::DefaultEndpointsProtocol)
            .map(str::to_ascii_lowercase)
    }

    /// Whether the string selects the storage emulator
    /// (`UseDevelopmentStorage=true`).
    pub fn use_development_storage(&self) -> bool {
        self.get(AzureConnKey::UseDevelopmentStorage).is_some()
    }

    /// The blob endpoint the client is given: the last `BlobEndpoint`, else,
    /// with an `EndpointSuffix`, `<protocol>://<AccountName>.blob.<suffix>`
    /// (protocol `https` unless `DefaultEndpointsProtocol` says otherwise).
    pub fn endpoint(&self) -> Option<String> {
        if let Some(e) = self.get(AzureConnKey::BlobEndpoint) {
            return Some(e.to_string());
        }
        let account = self.get(AzureConnKey::AccountName)?;
        let suffix = self.get(AzureConnKey::EndpointSuffix)?;
        let protocol = self.protocol().unwrap_or_else(|| "https".into());
        Some(format!("{protocol}://{account}.blob.{suffix}"))
    }

    /// The account endpoint the string alone leads the client to: the
    /// [`endpoint`](Self::endpoint), else, with an `AccountName`, the
    /// service's default `https://<account>.blob.core.windows.net`.
    pub fn account_url(&self) -> Option<String> {
        self.endpoint().or_else(|| {
            self.get(AzureConnKey::AccountName)
                .map(|a| format!("https://{a}.blob.core.windows.net"))
        })
    }

    /// Why the string cannot be used as written, if it cannot: an
    /// `AccountName` that is not a storage account name, or a
    /// `DefaultEndpointsProtocol` other than `http` / `https`. Names the
    /// key, never the value.
    pub fn problem(&self) -> Option<String> {
        if self
            .all(AzureConnKey::AccountName)
            .any(|a| !is_valid_azure_account_name(a))
        {
            return Some(
                "`AccountName` must be an Azure storage account name: 3 to 24 lowercase \
                 letters and digits"
                    .into(),
            );
        }
        if self
            .all(AzureConnKey::DefaultEndpointsProtocol)
            .any(|p| !p.eq_ignore_ascii_case("http") && !p.eq_ignore_ascii_case("https"))
        {
            return Some("`DefaultEndpointsProtocol` must be `http` or `https`".into());
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn azure_settings_read_exact_case_keys_last_wins() {
        let s = AzureConnSettings::read(
            " AccountName = one ;;AccountKey=a2V5==;accountname=two;AccountName=three;\
             EndpointSuffix=;noequals;UseDevelopmentStorage=false;BLOBENDPOINT=http://x",
        );
        assert_eq!(s.get(AzureConnKey::AccountName), Some("three"));
        assert_eq!(s.get(AzureConnKey::AccountKey), Some("a2V5=="));
        assert_eq!(s.get(AzureConnKey::EndpointSuffix), None);
        assert!(!s.use_development_storage());
        assert_eq!(s.get(AzureConnKey::BlobEndpoint), None);
        assert_eq!(s.other_case_keys(), ["accountname", "BLOBENDPOINT"]);
        assert_eq!(s.all(AzureConnKey::AccountName).count(), 2);
        assert_eq!(s.entries().len(), 3);
        assert_eq!(s.endpoint(), None);
        assert_eq!(
            s.account_url().as_deref(),
            Some("https://three.blob.core.windows.net")
        );
        assert!(AzureConnSettings::read("UseDevelopmentStorage=TRUE").use_development_storage());
        assert_eq!(AzureConnKey::BlobEndpoint.to_string(), "BlobEndpoint");
    }

    #[test]
    fn the_endpoint_follows_the_client() {
        let built = AzureConnSettings::read(
            "AccountName=acct;EndpointSuffix=core.chinacloudapi.cn;DefaultEndpointsProtocol=HTTP",
        );
        assert_eq!(
            built.endpoint().as_deref(),
            Some("http://acct.blob.core.chinacloudapi.cn")
        );
        assert_eq!(built.protocol().as_deref(), Some("http"));
        let explicit =
            AzureConnSettings::read("AccountName=acct;EndpointSuffix=x;BlobEndpoint=https://b/");
        assert_eq!(explicit.endpoint().as_deref(), Some("https://b/"));
        assert_eq!(explicit.account_url().as_deref(), Some("https://b/"));
        assert_eq!(AzureConnSettings::read("AccountKey=k").account_url(), None);
    }

    #[test]
    fn account_names_and_protocols_follow_the_service_rules() {
        for ok in ["abc", "devstoreaccount1", "a23456789012345678901234"] {
            assert!(is_valid_azure_account_name(ok), "{ok}");
        }
        for bad in [
            "ab",
            "Acct",
            "evil.example#",
            "a234567890123456789012345",
            "a-b",
        ] {
            assert!(!is_valid_azure_account_name(bad), "{bad}");
        }
        assert!(
            AzureConnSettings::read("AccountName=acct;DefaultEndpointsProtocol=https")
                .problem()
                .is_none()
        );
        let bad = AzureConnSettings::read("AccountName=evil.example#;AccountName=acct");
        assert!(bad.problem().unwrap().contains("`AccountName`"));
        assert!(!bad.problem().unwrap().contains("evil"));
        let proto = AzureConnSettings::read("DefaultEndpointsProtocol=https://evil.example/?");
        assert!(
            proto
                .problem()
                .unwrap()
                .contains("DefaultEndpointsProtocol")
        );
    }
}
