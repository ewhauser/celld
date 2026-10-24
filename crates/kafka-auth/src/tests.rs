use super::*;

#[test]
fn auth_parses_its_names() {
    assert_eq!(Auth::parse("none").unwrap(), Auth::None);
    assert_eq!(Auth::parse(" aws-msk-iam ").unwrap(), Auth::AwsMskIam);
    for auth in [Auth::None, Auth::AwsMskIam] {
        assert_eq!(Auth::parse(auth.name()).unwrap(), auth);
    }
    let message = Auth::parse("AWS_MSK_IAM").unwrap_err().to_string();
    assert!(message.contains("none or aws-msk-iam"), "{message}");
    assert_eq!(Auth::default(), Auth::None);
}

#[test]
fn msk_iam_signs_in_over_tls_with_oauthbearer() {
    assert!(Auth::None.properties().is_empty());
    assert_eq!(
        Auth::AwsMskIam.properties(),
        &[
            ("security.protocol", "SASL_SSL"),
            ("sasl.mechanism", "OAUTHBEARER"),
        ]
    );
}

#[test]
fn msk_iam_refuses_properties_that_undo_it() {
    for (name, value) in [
        ("security.protocol", "SSL"),
        ("security.protocol", "sasl_plaintext"),
        ("sasl.mechanism", "SCRAM-SHA-512"),
        ("sasl.mechanisms", "PLAIN"),
        ("sasl.oauthbearer.method", "oidc"),
        ("enable.sasl.oauthbearer.unsecure.jwt", "true"),
    ] {
        let message = Auth::AwsMskIam
            .check_property(name, value)
            .unwrap_err()
            .to_string();
        assert!(message.contains(name), "{message}");
        assert!(message.contains("aws-msk-iam"), "{message}");
        // Without it, anything goes.
        Auth::None.check_property(name, value).unwrap();
    }
    for (name, value) in [
        ("security.protocol", "SASL_SSL"),
        ("security.protocol", "sasl_ssl"),
        ("sasl.mechanism", "OAUTHBEARER"),
        ("sasl.oauthbearer.method", "default"),
        ("enable.sasl.oauthbearer.unsecure.jwt", "false"),
        ("ssl.ca.location", "/etc/ssl/certs/ca.pem"),
        ("linger.ms", "5"),
    ] {
        Auth::AwsMskIam.check_property(name, value).unwrap();
    }
}

#[test]
fn msk_broker_names_carry_the_region() {
    for (brokers, region) in [
        (
            "b-1.prod.abc123.c2.kafka.us-east-1.amazonaws.com:9098",
            Some("us-east-1"),
        ),
        (
            "b-1.prod.abc123.c2.kafka.eu-west-2.amazonaws.com:9098, \
             b-2.prod.abc123.c2.kafka.eu-west-2.amazonaws.com:9098",
            Some("eu-west-2"),
        ),
        (
            "boot-abc123.c1.kafka-serverless.ap-southeast-2.amazonaws.com:9098",
            Some("ap-southeast-2"),
        ),
        (
            "b-1.prod.abc123.c3.kafka.cn-north-1.amazonaws.com.cn:9098",
            Some("cn-north-1"),
        ),
        // Brokers that name none are passed over.
        (
            "10.0.0.7:9098,b-2.prod.abc123.c2.kafka.us-west-2.amazonaws.com:9098",
            Some("us-west-2"),
        ),
        // Names of the cluster's own, and brokers that disagree, leave the
        // region to the AWS config.
        ("kafka-0.kafka:9092", None),
        ("kafka.internal.example.com:9098", None),
        (
            "b-1.a.c2.kafka.us-east-1.amazonaws.com:9098,b-1.b.c2.kafka.us-west-2.amazonaws.com:9098",
            None,
        ),
        ("b-1.a.c2.kafka.US-EAST-1.amazonaws.com:9098", None),
    ] {
        assert_eq!(msk_region(brokers).as_deref(), region, "{brokers}");
    }
}
