use super::*;
use tokio::io::AsyncWriteExt;

pub(super) struct Kubernetes {
    pub context: String,
    pub namespace: String,
}
pub(super) fn field<'a>(v: &'a Value, path: &str) -> anyhow::Result<&'a str> {
    v.pointer(path)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .with_context(|| format!("missing {path}"))
}
pub(super) fn uid(v: &Value) -> anyhow::Result<&str> {
    field(v, "/metadata/uid")
}
pub(super) fn live(v: &Value) -> anyhow::Result<()> {
    uid(v)?;
    ensure!(
        v["metadata"]["deletionTimestamp"].is_null(),
        "resource is deleting"
    );
    Ok(())
}
pub(super) fn name(name: &str) -> anyhow::Result<()> {
    ensure!(
        !name.is_empty()
            && name.len() <= 253
            && name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"-.".contains(&b))
            && name.as_bytes()[0].is_ascii_alphanumeric()
            && name.as_bytes()[name.len() - 1].is_ascii_alphanumeric(),
        "invalid Kubernetes name"
    );
    Ok(())
}
impl Kubernetes {
    pub async fn call(&self, args: &[&str], input: Option<&Value>) -> anyhow::Result<Value> {
        use std::process::Stdio;
        let mut command = tokio::process::Command::new("kubectl");
        command.args([
            "--context",
            &self.context,
            "--namespace",
            &self.namespace,
            "--request-timeout=30s",
        ]);
        command
            .args(args)
            .stdin(if input.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command
            .spawn()
            .context("run kubectl (install it and configure cluster authentication)")?;
        if let Some(value) = input {
            let mut stdin = child.stdin.take().context("kubectl stdin unavailable")?;
            stdin.write_all(&serde_json::to_vec(value)?).await?;
            stdin.shutdown().await?;
        }
        let output = tokio::time::timeout(Duration::from_secs(45),child.wait_with_output()).await.context("kubectl timed out; mutation may have applied, inspect the resource before retrying")??;
        ensure!(
            output.status.success(),
            "kubectl failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        if output.stdout.iter().all(u8::is_ascii_whitespace) {
            return Ok(Value::Null);
        }
        Ok(serde_json::from_slice(&output.stdout)?)
    }
    pub async fn find(&self, kind: &str, name: &str) -> anyhow::Result<Option<Value>> {
        name_check(name)?;
        let v = self
            .call(
                &["get", kind, name, "--ignore-not-found=true", "-o", "json"],
                None,
            )
            .await?;
        Ok((!v.is_null()).then_some(v))
    }
    pub async fn get(&self, kind: &str, name: &str) -> anyhow::Result<Value> {
        self.find(kind, name)
            .await?
            .with_context(|| format!("{kind}/{name} not found"))
    }
    pub async fn create(&self, v: &Value) -> anyhow::Result<Value> {
        self.call(&["create", "-f", "-", "-o", "json"], Some(v))
            .await
    }
    pub async fn replace(&self, v: &Value, status: bool) -> anyhow::Result<Value> {
        field(v, "/metadata/resourceVersion")?;
        if status {
            self.call(&["patch",RESERVATION,field(v,"/metadata/name")?,"--subresource=status","--type=merge","-p",&serde_json::to_string(&json!({"metadata":{"resourceVersion":field(v,"/metadata/resourceVersion")?},"status":v["status"]}))?,"-o","json"],None).await
        } else {
            self.call(&["replace", "-f", "-", "-o", "json"], Some(v))
                .await
        }
    }
    pub async fn delete_preview(&self, p: &Value) -> anyhow::Result<()> {
        let ns = field(p, "/metadata/namespace")?;
        name_check(ns)?;
        let n = field(p, "/metadata/name")?;
        name_check(n)?;
        let path = format!("/apis/{API}/namespaces/{ns}/celldpreviews/{n}");
        self.call(&["delete","--raw",&path,"-f","-"],Some(&json!({"apiVersion":"v1","kind":"DeleteOptions","preconditions":{"uid":uid(p)?,"resourceVersion":field(p,"/metadata/resourceVersion")?}}))).await?;
        Ok(())
    }
}
fn name_check(value: &str) -> anyhow::Result<()> {
    name(value)
}
