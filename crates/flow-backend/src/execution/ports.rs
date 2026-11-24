use std::{collections::BTreeMap, sync::Arc};

use flow_application::dispatch::{AdmittedDispatch, DispatchLease, DispatchRequest, Reservation};
use flow_engine::{observation::ObservationStore, Node};
use flow_journal::StoredValue;
use futures::future::BoxFuture;

use super::remote::{RemoteDispatcher, RemoteReservation};
use super::IpcDispatcher;
use crate::{journal::JournalError, journal_execution::Attempt};

pub(crate) type ExecutionPort =
    dyn flow_application::dispatch::DispatchPort<DispatchWork, JournalError>;
pub(crate) type NodeDispatch = AdmittedDispatch<DispatchWork, JournalError>;
type DispatchReservation = Reservation<DispatchWork, JournalError>;
type Result<T> = std::result::Result<T, JournalError>;

pub(crate) struct DispatchWork {
    pub attempt: Attempt,
    pub node: Node,
    pub predecessors: BTreeMap<String, StoredValue>,
    pub observations: Option<ObservationStore>,
}

impl flow_application::dispatch::DispatchPort<DispatchWork, JournalError> for IpcDispatcher {
    fn reserve<'a>(
        self: Arc<Self>,
        _request: DispatchRequest<'a>,
    ) -> BoxFuture<'a, Result<Option<DispatchReservation>>> {
        Box::pin(async move { Ok(Some(Box::new(IpcLease(self)) as DispatchReservation)) })
    }

    fn uses_plugin_affinity(&self) -> bool {
        false
    }

    fn parked_count(&self) -> usize {
        IpcDispatcher::parked_count(self)
    }

    fn prewarm(&self) -> BoxFuture<'_, std::result::Result<usize, String>> {
        Box::pin(async {
            IpcDispatcher::prewarm(self, 1)
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn shutdown(&self) -> BoxFuture<'_, ()> {
        Box::pin(IpcDispatcher::shutdown(self))
    }
}

struct IpcLease(Arc<IpcDispatcher>);

impl DispatchLease<DispatchWork, JournalError> for IpcLease {
    fn dispatch_id(&self) -> Option<&str> {
        None
    }

    fn execute(self: Box<Self>, work: DispatchWork) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            self.0
                .execute(
                    &work.attempt,
                    &work.node,
                    work.predecessors,
                    work.observations,
                )
                .await
        })
    }
}

impl flow_application::dispatch::DispatchPort<DispatchWork, JournalError> for RemoteDispatcher {
    fn reserve<'a>(
        self: Arc<Self>,
        request: DispatchRequest<'a>,
    ) -> BoxFuture<'a, Result<Option<DispatchReservation>>> {
        Box::pin(async move {
            let reservation = self
                .reserve_node(request.run_id, request.node, request.plugin_binding)
                .await?;
            Ok(reservation.map(|reservation| {
                Box::new(RemoteLease {
                    dispatcher: self,
                    reservation,
                }) as DispatchReservation
            }))
        })
    }

    fn uses_plugin_affinity(&self) -> bool {
        true
    }

    fn parked_count(&self) -> usize {
        0
    }

    fn prewarm(&self) -> BoxFuture<'_, std::result::Result<usize, String>> {
        Box::pin(async { Ok(0) })
    }

    fn shutdown(&self) -> BoxFuture<'_, ()> {
        Box::pin(self.manager().shutdown())
    }
}

struct RemoteLease {
    dispatcher: Arc<RemoteDispatcher>,
    reservation: RemoteReservation,
}

impl DispatchLease<DispatchWork, JournalError> for RemoteLease {
    fn dispatch_id(&self) -> Option<&str> {
        Some(self.reservation.dispatch_id())
    }

    fn execute(self: Box<Self>, work: DispatchWork) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            self.dispatcher
                .execute(
                    &work.attempt,
                    self.reservation,
                    &work.node,
                    work.predecessors,
                    work.observations,
                )
                .await
        })
    }
}
