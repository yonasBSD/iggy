use crate::state::command::EntryCommand;
use crate::state::{State, StateEntry};
use crate::streaming::persistence::persister::Persister;
use crate::streaming::utils::file;
use crate::versioning::SemanticVersion;
use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use iggy::bytes_serializable::BytesSerializable;
use iggy::error::IggyError;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::timestamp::IggyTimestamp;
use log::debug;
use std::fmt::Debug;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, BufReader};
use tracing::{error, info};

const BUF_READER_CAPACITY_BYTES: usize = 512 * 1000;

#[derive(Debug)]
pub struct FileState {
    current_index: AtomicU64,
    entries_count: AtomicU64,
    current_leader: AtomicU32,
    term: AtomicU64,
    version: u32,
    path: String,
    persister: Arc<dyn Persister>,
}

impl FileState {
    pub fn new(path: &str, version: &SemanticVersion, persister: Arc<dyn Persister>) -> Self {
        Self {
            current_index: AtomicU64::new(0),
            entries_count: AtomicU64::new(0),
            current_leader: AtomicU32::new(0),
            term: AtomicU64::new(0),
            path: path.into(),
            persister,
            version: version.get_numeric_version().expect("Invalid version"),
        }
    }

    pub fn current_index(&self) -> u64 {
        self.current_index.load(Ordering::SeqCst)
    }

    pub fn entries_count(&self) -> u64 {
        self.entries_count.load(Ordering::SeqCst)
    }

    pub fn term(&self) -> u64 {
        self.term.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl State for FileState {
    async fn init(&self) -> Result<Vec<StateEntry>, IggyError> {
        if !Path::new(&self.path).exists() {
            info!("State file does not exist, creating a new one");
            self.persister.overwrite(&self.path, &[]).await?;
        }

        let entries = self.load_entries().await?;
        let entries_count = entries.len() as u64;
        self.entries_count.store(entries_count, Ordering::SeqCst);
        if entries_count == 0 {
            self.current_index.store(0, Ordering::SeqCst);
        } else {
            let last_index = entries[entries_count as usize - 1].index;
            self.current_index.store(last_index, Ordering::SeqCst);
        }

        return Ok(entries);
    }

    async fn load_entries(&self) -> Result<Vec<StateEntry>, IggyError> {
        if !Path::new(&self.path).exists() {
            return Err(IggyError::StateFileNotFound);
        }

        let file = file::open(&self.path).await?;
        let file_size = file.metadata().await?.len();
        if file_size == 0 {
            info!("State file is empty");
            return Ok(Vec::new());
        }

        info!(
            "Loading state, file size: {}",
            IggyByteSize::from(file_size).as_human_string()
        );
        let mut entries = Vec::new();
        let mut total_size: u64 = 0;
        let mut reader = BufReader::with_capacity(BUF_READER_CAPACITY_BYTES, file);
        let mut current_index = 0;
        let mut entries_count = 0;
        loop {
            let index = reader.read_u64_le().await?;
            if entries_count > 0 && index != current_index + 1 {
                error!(
                    "State file is corrupted, expected index: {}, got: {}",
                    current_index + 1,
                    index
                );
                return Err(IggyError::StateFileCorrupted);
            }

            current_index = index;
            entries_count += 1;
            let term = reader.read_u64_le().await?;
            let leader_id = reader.read_u32_le().await?;
            let version = reader.read_u32_le().await?;
            let flags = reader.read_u64_le().await?;
            let timestamp = IggyTimestamp::from(reader.read_u64_le().await?);
            let user_id = reader.read_u32_le().await?;
            let context_length = reader.read_u32_le().await? as usize;
            let mut context = BytesMut::with_capacity(context_length);
            context.put_bytes(0, context_length);
            reader.read_exact(&mut context).await?;
            let code = reader.read_u32_le().await?;
            let command_length = reader.read_u32_le().await? as usize;
            let mut command = BytesMut::with_capacity(command_length);
            command.put_bytes(0, command_length);
            reader.read_exact(&mut command).await?;
            let mut entry_command = BytesMut::with_capacity(4 + 4 + command_length);
            entry_command.put_u32_le(code);
            entry_command.put_u32_le(command_length as u32);
            entry_command.extend(command);
            let command = EntryCommand::from_bytes(entry_command.freeze())?;
            let entry = StateEntry::new(
                index,
                term,
                leader_id,
                version,
                flags,
                timestamp,
                user_id,
                context.freeze(),
                command,
            );
            debug!("Read state entry: {entry}");
            entries.push(entry);
            total_size += 8
                + 8
                + 4
                + 4
                + 8
                + 8
                + 4
                + 4
                + context_length as u64
                + 4
                + 4
                + command_length as u64;
            if total_size == file_size {
                break;
            }
        }

        info!("Loaded {entries_count} state entries, current index: {current_index}");
        Ok(entries)
    }

    async fn apply(&self, user_id: u32, command: EntryCommand) -> Result<(), IggyError> {
        debug!("Applying state entry with command: {command}, user ID: {user_id}");
        let entry = StateEntry {
            index: if self.entries_count.load(Ordering::SeqCst) == 0 {
                0
            } else {
                self.current_index.fetch_add(1, Ordering::SeqCst) + 1
            },
            term: self.term.load(Ordering::SeqCst),
            leader_id: self.current_leader.load(Ordering::SeqCst),
            version: self.version,
            flags: 0,
            timestamp: IggyTimestamp::now(),
            user_id,
            command,
            context: Bytes::new(),
        };

        self.entries_count.fetch_add(1, Ordering::SeqCst);
        self.persister.append(&self.path, &entry.to_bytes()).await?;
        debug!("Applied state entry: {entry}");
        Ok(())
    }
}
