use std::{collections::BTreeMap, sync::Arc};

use crate::observation::ObservationStore;
use flow_domain::Node;
use flow_journal::StoredValue;

use super::remote::{RemoteDispatcher, RemoteReservation};
use super::IpcDispatcher;
use crate::{journal::JournalError, journal_execution::Attempt};

pub(crate) enum ExecutionPort {
    Local {
        dispatcher: Arc<IpcDispatcher>,
        prewarm: bool,
    },
    Remote(Arc<RemoteDispatcher>),
}

impl ExecutionPort {
    pub async fn shutdown(&self) {
        match self {
            Self::Local { dispatcher, .. } => dispatcher.shutdown().await,
            Self::Remote(dispatcher) => dispatcher.manager().shutdown().await,
        }
    }
}

pub(crate) enum NodeDispatch {
    Local(Arc<IpcDispatcher>),
    Remote(Arc<RemoteDispatcher>, RemoteReservation),
}

impl NodeDispatch {
    pub fn dispatch_id(&self) -> Option<&str> {
        match self {
            Self::Local(_) => None,
            Self::Remote(_, reservation) => Some(reservation.dispatch_id()),
        }
    }

    pub async fn execute(
        self,
        attempt: &Attempt,
        node: &Node,
        predecessors: BTreeMap<String, StoredValue>,
        observations: Option<ObservationStore>,
    ) -> Result<(), JournalError> {
        match self {
            Self::Local(dispatcher) => {
                dispatcher
                    .execute(attempt, node, predecessors, observations)
                    .await
            }
            Self::Remote(dispatcher, reservation) => {
                dispatcher
                    .execute(attempt, reservation, node, predecessors, observations)
                    .await
            }
        }
    }
}
