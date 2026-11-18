use crate::execution_protocol::{Message, ProtocolError, TRANSFER_CHUNK_BYTES};

pub(crate) const STREAM_CAPABILITY: &str = "job-transfer-v1";
pub(crate) const JOB_INPUT: &str = "job:input";
pub(crate) const JOB_OUTPUT: &str = "job:output";
pub const MAX_STREAMED_JOB_BYTES: usize = 8 * 1024 * 1024;

pub(crate) fn frames<'a>(
    dispatch: &'a str,
    key: &'a str,
    value: &'a [u8],
) -> impl Iterator<Item = Message> + 'a {
    value
        .chunks(TRANSFER_CHUNK_BYTES)
        .enumerate()
        .map(move |(index, chunk)| Message::TransferChunk {
            dispatch_id: dispatch.into(),
            transfer_id: key.into(),
            offset: (index * TRANSFER_CHUNK_BYTES) as u64,
            bytes: chunk.to_vec(),
            digest: flow_facts::codec::digest(chunk),
        })
        .chain(std::iter::once_with(move || Message::InputReady {
            dispatch_id: dispatch.into(),
            transfer_id: key.into(),
            total_bytes: value.len() as u64,
            digest: flow_facts::codec::digest(value),
        }))
}

#[derive(Default)]
pub(crate) struct Transfer {
    bytes: Vec<u8>,
    ready: bool,
}

impl Transfer {
    pub(crate) fn receive(&mut self, key: &str, message: Message) -> Result<(), ProtocolError> {
        match message {
            Message::TransferChunk {
                transfer_id,
                offset,
                bytes,
                digest,
                ..
            } if !self.ready
                && transfer_id == key
                && offset == self.bytes.len() as u64
                && !bytes.is_empty()
                && bytes.len() <= TRANSFER_CHUNK_BYTES
                && self.bytes.len() + bytes.len() <= MAX_STREAMED_JOB_BYTES
                && flow_facts::codec::digest(&bytes) == digest =>
            {
                self.bytes.extend(bytes)
            }
            Message::InputReady {
                transfer_id,
                total_bytes,
                digest,
                ..
            } if !self.ready
                && transfer_id == key
                && total_bytes == self.bytes.len() as u64
                && flow_facts::codec::digest(&self.bytes) == digest =>
            {
                self.ready = true
            }
            _ => return Err(ProtocolError::InvalidPayload("invalid job transfer".into())),
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> Option<Vec<u8>> {
        self.ready.then_some(self.bytes)
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.ready
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.bytes.is_empty() && !self.ready
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn corrupted_or_incomplete_value_never_becomes_ready() {
        let mut transfer = Transfer::default();
        let mut frames = frames("d", JOB_INPUT, b"valid");
        let mut chunk = frames.next().unwrap();
        if let Message::TransferChunk { bytes, .. } = &mut chunk {
            bytes[0] = b'x';
        }
        assert!(transfer.receive(JOB_INPUT, chunk).is_err());
        assert!(transfer.receive(JOB_INPUT, frames.next().unwrap()).is_err());
        assert!(transfer.finish().is_none());
    }
}
