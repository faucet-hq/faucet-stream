//! AWS SDK configuration for the `iam` and `redshift_data_api` methods.

use crate::config::RedshiftAwsCredentials;

/// Load the SDK configuration from region and credentials. `region: None`
/// defers to the SDK default chain; credential resolution is delegated to
/// `aws-config`, so rotating credentials refresh on their own.
pub(crate) async fn sdk_config(
    region: Option<&str>,
    credentials: &RedshiftAwsCredentials,
) -> aws_config::SdkConfig {
    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
    if let Some(region) = region {
        loader = loader.region(aws_config::Region::new(region.to_owned()));
    }
    match credentials {
        RedshiftAwsCredentials::Default | RedshiftAwsCredentials::WebIdentity => {}
        RedshiftAwsCredentials::Profile { name } => {
            loader = loader.profile_name(name);
        }
        RedshiftAwsCredentials::AccessKey {
            access_key_id,
            secret_access_key,
            session_token,
        } => {
            loader = loader.credentials_provider(aws_sdk_redshiftdata::config::Credentials::new(
                access_key_id.clone(),
                secret_access_key.clone(),
                session_token.clone(),
                None,
                "faucet-config",
            ));
        }
        RedshiftAwsCredentials::AssumeRole {
            role_arn,
            session_name,
            external_id,
        } => {
            let mut builder = aws_config::sts::AssumeRoleProvider::builder(role_arn)
                .session_name(session_name.as_deref().unwrap_or("faucet-stream"));
            if let Some(region) = region {
                builder = builder.region(aws_config::Region::new(region.to_owned()));
            }
            if let Some(external_id) = external_id {
                builder = builder.external_id(external_id);
            }
            loader = loader.credentials_provider(builder.build().await);
        }
    }
    loader.load().await
}

/// An AWS SDK error with its whole source chain, for an error message.
pub(crate) fn describe<E: std::error::Error>(e: &E) -> String {
    let mut out = e.to_string();
    let mut next = e.source();
    while let Some(cause) = next {
        let text = cause.to_string();
        if !out.contains(&text) {
            out.push_str(": ");
            out.push_str(&text);
        }
        next = cause.source();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct Outer(Inner);
    #[derive(Debug)]
    struct Inner;
    impl std::fmt::Display for Outer {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "service error")
        }
    }
    impl std::fmt::Display for Inner {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "ValidationException: bad sql")
        }
    }
    impl std::error::Error for Inner {}
    impl std::error::Error for Outer {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }

    #[test]
    fn describe_joins_the_source_chain() {
        assert_eq!(
            describe(&Outer(Inner)),
            "service error: ValidationException: bad sql"
        );
    }
}
