//! The object store of a URL, configured from provider variables.

use std::sync::Arc;

use slatedb::object_store::path::Path;
use slatedb::object_store::{ObjectStore, parse_url_opts};

use crate::error::{Error, Result, StoreUrlError};

/// The prefixes, in lower case, of the variables that configure a provider.
const PROVIDER_PREFIXES: [&str; 3] = ["aws_", "google_", "azure_"];

/// The object store at `url`, such as `s3://bucket/prefix` or
/// `file:///var/lib/app`, with the path that the URL gives within the store.
/// The provider options are the variables of `env` whose name starts with
/// `AWS_`, `GOOGLE_` or `AZURE_` in any case, with the name in lower case as
/// the option key, so `open_url(url, std::env::vars())` configures the store
/// from the environment. Every other variable of `env` is ignored.
///
/// # Errors
///
/// [`Error::InvalidStoreUrl`] when `url` does not parse
/// ([`StoreUrlError::Url`]), or when the object store rejects it
/// ([`StoreUrlError::Store`]): its scheme is unknown or requires a backend
/// feature that is not enabled, or the provider rejects an option.
pub fn open_url<I, K, V>(url: &str, env: I) -> Result<(Arc<dyn ObjectStore>, Path)>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    let invalid = |source| Error::InvalidStoreUrl {
        url: url.to_string(),
        source,
    };
    let parsed = url::Url::parse(url).map_err(|err| invalid(StoreUrlError::Url(err)))?;
    let (store, path) = parse_url_opts(&parsed, provider_options(env))
        .map_err(|err| invalid(StoreUrlError::Store(err)))?;
    Ok((Arc::from(store), path))
}

/// The variables of `env` whose name starts with a provider prefix in any case,
/// with the name in lower case as the key.
fn provider_options<I, K, V>(env: I) -> impl Iterator<Item = (String, V)>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
{
    env.into_iter().filter_map(|(key, value)| {
        let key = key.as_ref().to_ascii_lowercase();
        PROVIDER_PREFIXES
            .iter()
            .any(|prefix| key.starts_with(prefix))
            .then_some((key, value))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_options_keep_the_provider_variables_in_lower_case() {
        let env = [
            ("AWS_REGION", "eu-west-1"),
            ("Google_Bucket", "b"),
            ("azure_storage_account_name", "a"),
            ("TOKEN", "t"),
            ("ENDPOINT", "e"),
            ("MY_AWS_KEY", "k"),
        ];
        let options: Vec<_> = provider_options(env).collect();
        assert_eq!(
            options,
            vec![
                ("aws_region".to_string(), "eu-west-1"),
                ("google_bucket".to_string(), "b"),
                ("azure_storage_account_name".to_string(), "a"),
            ]
        );
    }

    #[test]
    fn open_url_returns_the_store_and_the_path_of_the_url() {
        let (_store, path) = open_url("memory:///queues/main", [("TOKEN", "t")]).unwrap();
        assert_eq!(path.as_ref(), "queues/main");
        assert!(matches!(
            open_url("no scheme", std::iter::empty::<(&str, &str)>()),
            Err(Error::InvalidStoreUrl {
                source: StoreUrlError::Url(_),
                ..
            })
        ));
        assert!(matches!(
            open_url("unknown://bucket", std::iter::empty::<(&str, &str)>()),
            Err(Error::InvalidStoreUrl {
                source: StoreUrlError::Store(_),
                ..
            })
        ));
    }
}
