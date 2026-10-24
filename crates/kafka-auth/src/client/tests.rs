use super::*;
use std::collections::BTreeMap;

const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
const SESSION_TOKEN: &str = "IQoJb3JpZ2luX2VjEXAMPLE/session+token==";

/// 2026-10-24T12:00:00Z.
fn noon() -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(1_792_843_200)
}

fn credentials(session_token: Option<&str>, expiry: Option<SystemTime>) -> Credentials {
    Credentials::new(
        "AKIDEXAMPLE",
        SECRET,
        session_token.map(str::to_string),
        expiry,
        "test",
    )
}

/// The presigned URL a token carries, and its query.
fn decode(token: &OAuthToken) -> (url::Url, BTreeMap<String, String>) {
    let url = String::from_utf8(URL_SAFE_NO_PAD.decode(&token.token).unwrap()).unwrap();
    let url = url::Url::parse(&url).unwrap();
    let query = url.query_pairs().into_owned().collect();
    (url, query)
}

/// The query of a URL botocore's SigV4QueryAuth presigned, as
/// aws-msk-iam-sasl-signer-python does, at [`noon`].
fn botocore(url: &str) -> BTreeMap<String, String> {
    url::Url::parse(url)
        .unwrap()
        .query_pairs()
        .into_owned()
        .collect()
}

#[test]
fn tokens_match_the_aws_signers() {
    for (session_token, expected) in [
        (
            None,
            "https://kafka.us-east-1.amazonaws.com/?Action=kafka-cluster%3AConnect\
             &X-Amz-Algorithm=AWS4-HMAC-SHA256\
             &X-Amz-Credential=AKIDEXAMPLE%2F20261024%2Fus-east-1%2Fkafka-cluster%2Faws4_request\
             &X-Amz-Date=20261024T120000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host\
             &X-Amz-Signature=d42a34746ed3d67578d508e5febf126374c3565456becf81d4d6bf73b68564d1",
        ),
        (
            Some(SESSION_TOKEN),
            "https://kafka.us-east-1.amazonaws.com/?Action=kafka-cluster%3AConnect\
             &X-Amz-Algorithm=AWS4-HMAC-SHA256\
             &X-Amz-Credential=AKIDEXAMPLE%2F20261024%2Fus-east-1%2Fkafka-cluster%2Faws4_request\
             &X-Amz-Date=20261024T120000Z&X-Amz-Expires=900&X-Amz-SignedHeaders=host\
             &X-Amz-Security-Token=IQoJb3JpZ2luX2VjEXAMPLE%2Fsession%2Btoken%3D%3D\
             &X-Amz-Signature=ca8619b7c83bf7295608001ba6d7bcbf935f28bfd5183874e18538b2c0bf5207",
        ),
    ] {
        let token = sign(&credentials(session_token, None), "us-east-1", noon()).unwrap();
        let (url, mut query) = decode(&token);
        assert_eq!(url.scheme(), "https");
        assert_eq!(url.host_str(), Some("kafka.us-east-1.amazonaws.com"));
        assert_eq!(url.path(), "/");
        // Outside the signature, as AWS's signers add theirs.
        assert_eq!(query.remove("User-Agent").as_deref(), Some(USER_AGENT));
        assert_eq!(query, botocore(expected));
        // The token is a URL-safe base64 string with no padding.
        assert!(!token.token.contains(['=', '+', '/']), "{}", token.token);
        assert_eq!(token.principal_name, "");
    }
}

#[test]
fn a_token_lives_as_long_as_its_request_or_credentials() {
    let token = sign(&credentials(None, None), "eu-west-1", noon()).unwrap();
    let presigned = noon() + TOKEN_LIFETIME;
    let ms = |t: SystemTime| t.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    assert_eq!(token.lifetime_ms, ms(presigned));
    let (url, query) = decode(&token);
    assert_eq!(url.host_str(), Some("kafka.eu-west-1.amazonaws.com"));
    assert_eq!(
        query["X-Amz-Credential"],
        "AKIDEXAMPLE/20261024/eu-west-1/kafka-cluster/aws4_request"
    );

    // Credentials that expire first cut the token short, so librdkafka
    // refreshes it before the broker sees them lapse.
    let soon = noon() + Duration::from_secs(300);
    let token = sign(
        &credentials(Some(SESSION_TOKEN), Some(soon)),
        "eu-west-1",
        noon(),
    )
    .unwrap();
    assert_eq!(token.lifetime_ms, ms(soon));
    let later = noon() + Duration::from_secs(3600);
    let token = sign(
        &credentials(Some(SESSION_TOKEN), Some(later)),
        "eu-west-1",
        noon(),
    )
    .unwrap();
    assert_eq!(token.lifetime_ms, ms(presigned));
}

#[test]
fn a_client_without_msk_iam_supplies_no_tokens() {
    let tokens = Tokens::new(Auth::None, "kafka-0.kafka:9092").unwrap();
    let Err(error) = tokens.generate() else {
        panic!("a token without MSK IAM");
    };
    let message = error.to_string();
    assert!(message.contains("no OAUTHBEARER tokens"), "{message}");
}

#[test]
fn errors_keep_their_sources() {
    #[derive(Debug)]
    struct Outer(std::io::Error);
    impl std::fmt::Display for Outer {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("resolve credentials")
        }
    }
    impl Error for Outer {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            Some(&self.0)
        }
    }
    let error = Outer(std::io::Error::other("connection refused"));
    assert_eq!(chain(&error), "resolve credentials: connection refused");
}
