// Kafka clients are host tools outside celld's execution boundary: the
// signing clock and the credential chain's timeouts are the host's.
#![allow(clippy::disallowed_methods)]

//! The librdkafka side of [`Auth`]: a client context that answers
//! librdkafka's OAUTHBEARER token refreshes with Amazon MSK IAM tokens.
//!
//! librdkafka asks for a token when the client starts and again at 80% of
//! the last token's lifetime, on the thread that polls the client: the
//! producer's polling thread, or the loader's poll thread. [`Tokens`]
//! answers on that thread. It resolves AWS credentials through the SDK's
//! default chain on a small runtime of its own, so a refresh never blocks a
//! runtime worker and works whichever runtime, if any, the caller runs.
//! The chain is loaded on the first refresh and kept; credentials are
//! resolved again on every refresh, so rotated or re-assumed credentials
//! are picked up within one token's lifetime.

use crate::Auth;
use aws_config::BehaviorVersion;
use aws_config::Region;
use aws_credential_types::provider::ProvideCredentials as _;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_credential_types::Credentials;
use aws_sigv4::http_request::SignableBody;
use aws_sigv4::http_request::SignableRequest;
use aws_sigv4::http_request::SignatureLocation;
use aws_sigv4::http_request::SigningSettings;
use aws_smithy_runtime_api::client::identity::Identity;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use rdkafka::client::OAuthToken;
use rdkafka::ClientContext;
use std::error::Error;
use std::sync::Mutex;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

/// How long a token's presigned request is valid, as AWS's signers have it.
pub const TOKEN_LIFETIME: Duration = Duration::from_secs(900);

/// The SigV4 service name MSK IAM tokens are signed for.
const SERVICE: &str = "kafka-cluster";

/// The `User-Agent` a token carries, outside its signature, as AWS's
/// signers add theirs.
const USER_AGENT: &str = concat!("celld-kafka-auth/", env!("CARGO_PKG_VERSION"));

/// How long loading the credential chain, or resolving credentials through
/// it, may take before a refresh fails. librdkafka asks again.
const SDK_TIMEOUT: Duration = Duration::from_secs(10);

type BoxError = Box<dyn Error + Send + Sync>;

/// The client context for a Kafka client that may sign in with [`Tokens`].
///
/// It always asks rdkafka for token refreshes. librdkafka only raises one
/// for SASL/OAUTHBEARER with its default token method, so a client that
/// authenticates otherwise, OIDC included, never sees one.
pub struct Context {
    tokens: Tokens,
}

impl Context {
    pub fn new(tokens: Tokens) -> Context {
        Context { tokens }
    }
}

impl ClientContext for Context {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn generate_oauth_token(&self, _: Option<&str>) -> Result<OAuthToken, Box<dyn Error>> {
        self.tokens.generate()
    }
}

/// The tokens a client's [`Auth`] signs in with, if it signs in with any.
pub struct Tokens {
    msk_iam: Option<MskIam>,
}

impl Tokens {
    /// The tokens for `auth` on a client of `brokers`, the comma-separated
    /// bootstrap list, whose names may carry the signing region.
    pub fn new(auth: Auth, brokers: &str) -> std::io::Result<Tokens> {
        let msk_iam = match auth {
            Auth::None => None,
            Auth::AwsMskIam => Some(MskIam {
                runtime: Some(
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .thread_name("kafka-auth")
                        .build()?,
                ),
                region: crate::msk_region(brokers),
                sdk: Mutex::new(None),
            }),
        };
        Ok(Tokens { msk_iam })
    }

    /// A fresh token, for librdkafka's refresh. Blocks while credentials
    /// resolve; call it off any runtime's worker threads.
    pub fn generate(&self) -> Result<OAuthToken, Box<dyn Error>> {
        let Some(msk_iam) = &self.msk_iam else {
            return Err(
                "this Kafka client's auth supplies no OAUTHBEARER tokens; set its \
                        auth to aws-msk-iam or configure librdkafka's sasl.oauthbearer.method"
                    .into(),
            );
        };
        msk_iam
            .generate()
            .map_err(|e| format!("Amazon MSK IAM token: {}", chain(e.as_ref())).into())
    }
}

struct MskIam {
    /// Taken on drop, which may happen on a runtime thread, where a runtime
    /// cannot be dropped in place.
    runtime: Option<tokio::runtime::Runtime>,
    /// The region the broker names carry.
    region: Option<String>,
    /// The credential chain and signing region, once loaded.
    sdk: Mutex<Option<Sdk>>,
}

#[derive(Clone)]
struct Sdk {
    credentials: SharedCredentialsProvider,
    region: String,
}

impl MskIam {
    fn generate(&self) -> Result<OAuthToken, BoxError> {
        let runtime = self.runtime.as_ref().expect("held until drop");
        let sdk = self.sdk(runtime)?;
        let credentials = runtime
            .block_on(async {
                tokio::time::timeout(SDK_TIMEOUT, sdk.credentials.provide_credentials()).await
            })
            .map_err(|_| format!("the AWS credential chain did not answer in {SDK_TIMEOUT:?}"))??;
        sign(&credentials, &sdk.region, SystemTime::now())
    }

    fn sdk(&self, runtime: &tokio::runtime::Runtime) -> Result<Sdk, BoxError> {
        let mut sdk = self.sdk.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(sdk) = &*sdk {
            return Ok(sdk.clone());
        }
        let mut loader = aws_config::defaults(BehaviorVersion::latest()).http_client(
            aws_smithy_http_client::Builder::new()
                .tls_provider(aws_smithy_http_client::tls::Provider::Rustls(
                    aws_smithy_http_client::tls::rustls_provider::CryptoMode::Ring,
                ))
                .build_https(),
        );
        if let Some(region) = &self.region {
            loader = loader.region(Region::new(region.clone()));
        }
        let config = runtime
            .block_on(async { tokio::time::timeout(SDK_TIMEOUT, loader.load()).await })
            .map_err(|_| format!("loading the AWS config took over {SDK_TIMEOUT:?}"))?;
        let Some(region) = config.region() else {
            return Err(
                "no AWS region: the broker names carry none, and the AWS config sets \
                        none; set AWS_REGION to the cluster's region"
                    .into(),
            );
        };
        let Some(credentials) = config.credentials_provider() else {
            return Err("the AWS config has no credential provider".into());
        };
        let loaded = Sdk {
            credentials,
            region: region.to_string(),
        };
        *sdk = Some(loaded.clone());
        Ok(loaded)
    }
}

impl Drop for MskIam {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

/// An MSK IAM token for `credentials` in `region`, signed at `now`: the
/// URL-safe, unpadded base64 of a GET of
/// `https://kafka.<region>.amazonaws.com/?Action=kafka-cluster:Connect`
/// presigned with SigV4 for [`TOKEN_LIFETIME`], plus a `User-Agent`. It
/// lives until the presigned request expires, or the credentials do if
/// they expire first.
pub fn sign(
    credentials: &Credentials,
    region: &str,
    now: SystemTime,
) -> Result<OAuthToken, BoxError> {
    let identity = Identity::from(credentials.clone());
    let mut settings = SigningSettings::default();
    settings.signature_location = SignatureLocation::QueryParams;
    settings.expires_in = Some(TOKEN_LIFETIME);
    let params = aws_sigv4::sign::v4::SigningParams::builder()
        .identity(&identity)
        .region(region)
        .name(SERVICE)
        .time(now)
        .settings(settings)
        .build()?
        .into();
    let endpoint = format!("https://kafka.{region}.amazonaws.com/?Action=kafka-cluster%3AConnect");
    let request = SignableRequest::new(
        "GET",
        endpoint.as_str(),
        std::iter::empty(),
        SignableBody::empty(),
    )?;
    let (instructions, _) = aws_sigv4::http_request::sign(request, &params)?.into_parts();
    let (_, query) = instructions.into_parts();
    let mut url = url::Url::parse(&endpoint)?;
    {
        let mut pairs = url.query_pairs_mut();
        for (name, value) in &query {
            pairs.append_pair(name, value);
        }
        pairs.append_pair("User-Agent", USER_AGENT);
    }
    let mut expires = now + TOKEN_LIFETIME;
    if let Some(credentials) = credentials.expiry() {
        expires = expires.min(credentials);
    }
    Ok(OAuthToken {
        token: URL_SAFE_NO_PAD.encode(url.as_str()),
        principal_name: String::new(),
        lifetime_ms: i64::try_from(expires.duration_since(UNIX_EPOCH)?.as_millis())?,
    })
}

/// An error with its sources, `outer: inner: …`, since librdkafka keeps only
/// the message.
fn chain(error: &(dyn Error + 'static)) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(inner) = source {
        let text = inner.to_string();
        if !message.ends_with(&text) {
            message.push_str(": ");
            message.push_str(&text);
        }
        source = inner.source();
    }
    message
}

#[cfg(test)]
mod tests;
