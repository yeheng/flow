use std::{future::Future, sync::Arc};

use flow_domain::{journal_state::PluginBinding, Node};
use futures::future::BoxFuture;

pub struct DispatchRequest<'a> {
    pub run_id: &'a str,
    pub node: &'a Node,
    pub plugin_binding: Option<&'a PluginBinding>,
}

pub trait DispatchLease<Work, Error>: Send {
    /// Remote leases bind an attempt ID before the application commits it.
    fn dispatch_id(&self) -> Option<&str>;

    fn execute(self: Box<Self>, work: Work) -> BoxFuture<'static, Result<(), Error>>;
}

pub type Reservation<Work, Error> = Box<dyn DispatchLease<Work, Error>>;

pub trait DispatchPort<Work, Error>: Send + Sync {
    fn reserve<'a>(
        self: Arc<Self>,
        request: DispatchRequest<'a>,
    ) -> BoxFuture<'a, Result<Option<Reservation<Work, Error>>, Error>>;

    fn uses_plugin_affinity(&self) -> bool;
    fn parked_count(&self) -> usize;
    fn prewarm(&self) -> BoxFuture<'_, Result<usize, String>>;
    fn shutdown(&self) -> BoxFuture<'_, ()>;
}

/// Holds the lease across the attempt commit. Dropping it at any point releases
/// the reservation through the adapter's own lease cleanup.
pub struct AdmittedDispatch<Work, Error>(Result<Reservation<Work, Error>, Error>);

impl<Work, Error> AdmittedDispatch<Work, Error> {
    pub async fn execute(self, work: Work) -> Result<(), Error> {
        self.0?.execute(work).await
    }
}

/// Defer if there is no compatible capacity. A reservation failure still goes
/// through an attempt so the application can commit its failure evidence.
pub async fn admit<Work, Error, Attempt, Commit>(
    port: Arc<dyn DispatchPort<Work, Error>>,
    request: DispatchRequest<'_>,
    begin: impl FnOnce(Option<String>) -> Commit,
) -> Result<Option<(Attempt, AdmittedDispatch<Work, Error>)>, Error>
where
    Commit: Future<Output = Result<Attempt, Error>>,
{
    let reservation = match port.reserve(request).await {
        Ok(Some(reservation)) => Ok(reservation),
        Ok(None) => return Ok(None),
        Err(error) => Err(error),
    };
    let id = reservation
        .as_ref()
        .ok()
        .and_then(|lease| lease.dispatch_id());
    let attempt = begin(id.map(str::to_owned)).await?;
    Ok(Some((attempt, AdmittedDispatch(reservation))))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    struct Port(Arc<AtomicBool>);
    struct Lease(Arc<AtomicBool>);

    impl Drop for Lease {
        fn drop(&mut self) {
            self.0.store(false, Ordering::SeqCst);
        }
    }

    impl DispatchLease<(), &'static str> for Lease {
        fn dispatch_id(&self) -> Option<&str> {
            Some("reserved-attempt")
        }

        fn execute(self: Box<Self>, _: ()) -> BoxFuture<'static, Result<(), &'static str>> {
            panic!("an uncommitted attempt must not execute")
        }
    }

    impl DispatchPort<(), &'static str> for Port {
        fn reserve<'a>(
            self: Arc<Self>,
            _: DispatchRequest<'a>,
        ) -> BoxFuture<'a, Result<Option<Reservation<(), &'static str>>, &'static str>> {
            Box::pin(async move {
                self.0.store(true, Ordering::SeqCst);
                Ok(Some(Box::new(Lease(self.0.clone())) as Reservation<_, _>))
            })
        }

        fn uses_plugin_affinity(&self) -> bool {
            false
        }

        fn parked_count(&self) -> usize {
            0
        }

        fn prewarm(&self) -> BoxFuture<'_, Result<usize, String>> {
            Box::pin(async { Ok(0) })
        }

        fn shutdown(&self) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }
    }

    #[tokio::test]
    async fn failed_attempt_commit_drops_the_reserved_lease() {
        let held = Arc::new(AtomicBool::new(false));
        let node = serde_json::from_value(serde_json::json!({"id": "n", "type": "start"})).unwrap();
        let request = DispatchRequest {
            run_id: "run",
            node: &node,
            plugin_binding: None,
        };
        let result = admit(Arc::new(Port(held.clone())), request, |id| {
            let held = &held;
            async move {
                assert!(held.load(Ordering::SeqCst));
                assert_eq!(id.as_deref(), Some("reserved-attempt"));
                Err::<(), _>("commit failed")
            }
        })
        .await;
        assert!(matches!(result, Err("commit failed")));
        assert!(!held.load(Ordering::SeqCst));
    }
}
