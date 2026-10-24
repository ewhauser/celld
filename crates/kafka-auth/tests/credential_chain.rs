// A host-side test: its clock and blocking pool are the host's.
#![allow(clippy::disallowed_methods)]

//! [`Tokens`] through the AWS SDK's default credential chain, with
//! credentials from the environment and no profile or instance metadata to
//! fall back on.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use celld_kafka_auth::client::Tokens;
use celld_kafka_auth::Auth;
use rdkafka::client::OAuthToken;
use rdkafka::producer::BaseProducer;
use rdkafka::producer::DeliveryResult;
use rdkafka::producer::Producer as _;
use rdkafka::producer::ProducerContext;
use rdkafka::ClientContext;
use std::error::Error;
use std::sync::Mutex;
use std::time::Duration;

const BROKERS: &str = "b-1.prod.abc123.c2.kafka.eu-central-1.amazonaws.com:9098,\
                       b-2.prod.abc123.c2.kafka.eu-central-1.amazonaws.com:9098";

fn params(token: &str) -> Vec<(String, String)> {
    let url = String::from_utf8(URL_SAFE_NO_PAD.decode(token).unwrap()).unwrap();
    url::Url::parse(&url)
        .unwrap()
        .query_pairs()
        .into_owned()
        .collect()
}

fn param<'a>(query: &'a [(String, String)], name: &str) -> &'a str {
    query
        .iter()
        .find(|(n, _)| n == name)
        .map(|(_, v)| v.as_str())
        .unwrap_or_else(|| panic!("no {name} in {query:?}"))
}

// One test, so nothing else in this process reads the environment while it
// changes.
#[test]
fn tokens_come_from_the_default_credential_chain() {
    for (name, value) in [
        ("AWS_ACCESS_KEY_ID", "AKIDCHAIN"),
        ("AWS_SECRET_ACCESS_KEY", "chain-secret"),
        ("AWS_SESSION_TOKEN", "chain-session"),
        ("AWS_CONFIG_FILE", "/nonexistent/celld-kafka-auth/config"),
        (
            "AWS_SHARED_CREDENTIALS_FILE",
            "/nonexistent/celld-kafka-auth/credentials",
        ),
        ("AWS_EC2_METADATA_DISABLED", "true"),
    ] {
        std::env::set_var(name, value);
    }
    for name in ["AWS_REGION", "AWS_DEFAULT_REGION", "AWS_PROFILE"] {
        std::env::remove_var(name);
    }

    // The region comes from the broker names.
    let tokens = Tokens::new(Auth::AwsMskIam, BROKERS).unwrap();
    let token = tokens.generate().unwrap();
    let query = params(&token.token);
    assert!(
        param(&query, "X-Amz-Credential").starts_with("AKIDCHAIN/"),
        "{query:?}"
    );
    assert!(
        param(&query, "X-Amz-Credential").contains("/eu-central-1/kafka-cluster/"),
        "{query:?}"
    );
    assert_eq!(param(&query, "X-Amz-Security-Token"), "chain-session");
    // A second refresh reuses the loaded chain.
    tokens.generate().unwrap();

    // Brokers with names of their own need the AWS config's region.
    let tokens = Tokens::new(Auth::AwsMskIam, "kafka.internal:9098").unwrap();
    let Err(error) = tokens.generate() else {
        panic!("a token with no region");
    };
    let message = error.to_string();
    assert!(message.contains("AWS_REGION"), "{message}");
    std::env::set_var("AWS_REGION", "ap-northeast-1");
    let tokens = Tokens::new(Auth::AwsMskIam, "kafka.internal:9098").unwrap();
    let query = params(&tokens.generate().unwrap().token);
    assert!(
        param(&query, "X-Amz-Credential").contains("/ap-northeast-1/kafka-cluster/"),
        "{query:?}"
    );

    // librdkafka may refresh on a blocking-pool thread of the host's
    // runtime, and the client may be dropped on one of its workers.
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let tokens = Tokens::new(Auth::AwsMskIam, BROKERS).unwrap();
        let tokens = tokio::task::spawn_blocking(move || {
            tokens.generate().map_err(|e| e.to_string()).unwrap();
            tokens
        })
        .await
        .unwrap();
        drop(tokens);
    });

    // librdkafka, as this workspace builds it, signs in with these
    // properties and asks the context for a token on the thread that polls
    // the client, before any broker answers.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let mut config = rdkafka::ClientConfig::new();
    config.set("bootstrap.servers", address.to_string());
    for (name, value) in Auth::AwsMskIam.properties() {
        config.set(*name, *value);
    }
    let producer: BaseProducer<Counting> = config
        .create_with_context(Counting {
            tokens: Tokens::new(Auth::AwsMskIam, BROKERS).unwrap(),
            signed: Mutex::new(Vec::new()),
        })
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while producer.context().signed.lock().unwrap().is_empty() {
        assert!(std::time::Instant::now() < deadline, "no token refresh");
        producer.poll(Duration::from_millis(100));
    }
    let signed = producer.context().signed.lock().unwrap();
    let token = signed[0].as_ref().unwrap();
    assert!(
        param(&params(token), "X-Amz-Credential").starts_with("AKIDCHAIN/"),
        "{token}"
    );
}

/// [`celld_kafka_auth::client::Context`], recording what it hands
/// librdkafka.
struct Counting {
    tokens: Tokens,
    signed: Mutex<Vec<Result<String, String>>>,
}

impl ClientContext for Counting {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn generate_oauth_token(&self, _: Option<&str>) -> Result<OAuthToken, Box<dyn Error>> {
        let token = self.tokens.generate();
        self.signed.lock().unwrap().push(match &token {
            Ok(token) => Ok(token.token.clone()),
            Err(e) => Err(e.to_string()),
        });
        token
    }
}

impl ProducerContext for Counting {
    type DeliveryOpaque = ();

    fn delivery(&self, _: &DeliveryResult<'_>, _: ()) {}
}
