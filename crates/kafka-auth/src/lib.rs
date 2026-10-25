//! How the change export's Kafka clients authenticate.
//!
//! The node's Kafka sink (`CELLD_EXPORT_KAFKA_AUTH`) and the Snowflake
//! loader's Kafka source (`EXPORT_KAFKA_AUTH`) each name an [`Auth`].
//! [`Auth::None`] leaves authentication to the librdkafka properties file,
//! as it always has: TLS, SASL/PLAIN, SCRAM, OIDC. [`Auth::AwsMskIam`] signs
//! in to Amazon MSK with IAM access control: the client speaks
//! SASL/OAUTHBEARER over TLS, and each token is a SigV4-presigned
//! `kafka-cluster:Connect` request, built as AWS's own MSK signers build it
//! and signed with the AWS SDK's default credential chain.
//!
//! The signing region is the one the broker names carry
//! (`b-1.name.abc123.c2.kafka.us-east-1.amazonaws.com`), or else the SDK's
//! (`AWS_REGION`, the profile, instance metadata), for brokers reached
//! through names of their own.
//!
//! The `client` feature adds [`client`], the librdkafka client context that
//! hands librdkafka those tokens.

#[cfg(feature = "client")]
pub mod client;

/// A Kafka client's authentication.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Auth {
    /// Whatever the librdkafka properties say.
    #[default]
    None,
    /// Amazon MSK IAM access control: OAUTHBEARER tokens signed with AWS
    /// credentials.
    AwsMskIam,
}

/// Why an auth setting, or a property beside it, is refused.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("must be none or aws-msk-iam, not {0:?}")]
    Unknown(String),
    #[error("{name}={value} conflicts with aws-msk-iam, which {reason}")]
    Conflict {
        name: String,
        value: String,
        reason: &'static str,
    },
}

impl Auth {
    /// `none` or `aws-msk-iam`.
    pub fn parse(value: &str) -> Result<Auth, Error> {
        match value.trim() {
            "none" => Ok(Auth::None),
            "aws-msk-iam" => Ok(Auth::AwsMskIam),
            other => Err(Error::Unknown(other.to_string())),
        }
    }

    /// The setting's name, as [`Auth::parse`] reads it.
    pub fn name(self) -> &'static str {
        match self {
            Auth::None => "none",
            Auth::AwsMskIam => "aws-msk-iam",
        }
    }

    /// The librdkafka properties this auth needs. A client sets them after
    /// its own and before its properties file's, which
    /// [`Auth::check_property`] holds to them.
    pub fn properties(self) -> &'static [(&'static str, &'static str)] {
        match self {
            Auth::None => &[],
            // MSK serves IAM only over TLS.
            Auth::AwsMskIam => &[
                ("security.protocol", "SASL_SSL"),
                ("sasl.mechanism", "OAUTHBEARER"),
            ],
        }
    }

    /// Refuse a properties-file property that would undo this auth.
    pub fn check_property(self, name: &str, value: &str) -> Result<(), Error> {
        let reason = match self {
            Auth::None => return Ok(()),
            Auth::AwsMskIam => match name {
                "security.protocol" if !value.eq_ignore_ascii_case("SASL_SSL") => {
                    "signs in over SASL_SSL"
                }
                // `sasl.mechanisms` is librdkafka's alias.
                "sasl.mechanism" | "sasl.mechanisms" if value != "OAUTHBEARER" => {
                    "signs in with SASL/OAUTHBEARER"
                }
                // librdkafka's own token sources would replace the signed
                // tokens.
                "sasl.oauthbearer.method" if !value.eq_ignore_ascii_case("default") => {
                    "supplies its own OAUTHBEARER tokens"
                }
                "enable.sasl.oauthbearer.unsecure.jwt" if value != "false" => {
                    "supplies its own OAUTHBEARER tokens"
                }
                _ => return Ok(()),
            },
        };
        Err(Error::Conflict {
            name: name.to_string(),
            value: value.to_string(),
            reason,
        })
    }
}

/// The AWS region Amazon MSK broker names carry, when every broker in the
/// comma-separated `host:port` list that names one names the same one:
/// `…kafka.<region>.amazonaws.com` for a provisioned cluster,
/// `…kafka-serverless.<region>.amazonaws.com` for a serverless one, and
/// `.amazonaws.com.cn` in China.
pub fn msk_region(brokers: &str) -> Option<String> {
    let mut found: Option<&str> = None;
    for broker in brokers.split(',') {
        let broker = broker.trim();
        let host = broker.rsplit_once(':').map_or(broker, |(host, _)| host);
        let labels: Vec<&str> = host.split('.').collect();
        let Some(region) = labels.windows(3).find_map(|w| {
            let region_like = !w[1].is_empty()
                && w[1]
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
            (matches!(w[0], "kafka" | "kafka-serverless") && region_like && w[2] == "amazonaws")
                .then_some(w[1])
        }) else {
            continue;
        };
        match found {
            Some(other) if other != region => return None,
            _ => found = Some(region),
        }
    }
    found.map(str::to_string)
}

#[cfg(test)]
mod tests;
