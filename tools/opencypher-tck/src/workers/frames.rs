//! Length-delimited local worker messages. Both directions reject oversized
//! frames before allocating their payload; serialization is bounded as well.

use crate::corpus;
use serde::{de::DeserializeOwned, Serialize};
use std::io::{self, Write};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub(super) const MAX_BYTES: usize = 8 * 1024 * 1024;

struct Buffer(Vec<u8>);

impl Write for Buffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_BYTES.saturating_sub(self.0.len()) {
            return Err(io::Error::other("WorkerFrameLimit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(super) async fn write<W: AsyncWrite + Unpin, T: Serialize>(
    writer: &mut W,
    value: &T,
) -> corpus::Result<()> {
    let mut buffer = Buffer(Vec::new());
    serde_json::to_writer(&mut buffer, value)?;
    writer.write_u32(buffer.0.len().try_into()?).await?;
    writer.write_all(&buffer.0).await?;
    writer.flush().await?;
    Ok(())
}

pub(super) async fn read<R: AsyncRead + Unpin, T: DeserializeOwned>(
    reader: &mut R,
) -> corpus::Result<Option<T>> {
    // A clean EOF is permitted only between messages. A partial prefix or
    // payload is a protocol failure, never a successfully completed scenario.
    let mut prefix = [0; size_of::<u32>()];
    if reader.read(&mut prefix[..1]).await? == 0 {
        return Ok(None);
    }
    reader.read_exact(&mut prefix[1..]).await?;
    let length = u32::from_be_bytes(prefix) as usize;
    if length > MAX_BYTES {
        return Err("WorkerFrameLimit".into());
    }
    let mut payload = vec![0; length];
    reader.read_exact(&mut payload).await?;
    Ok(Some(serde_json::from_slice(&payload)?))
}

#[cfg(test)]
#[path = "../tests/worker_frames.rs"]
mod tests;
