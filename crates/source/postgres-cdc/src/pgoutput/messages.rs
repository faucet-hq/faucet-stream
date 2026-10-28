//! pgoutput message types — high-level structs decoded from the wire.

use faucet_core::FaucetError;

/// pgoutput message kind byte (the first byte of every payload).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageKind {
    Begin,
    Commit,
    Origin,
    Relation,
    Type,
    Insert,
    Update,
    Delete,
    Truncate,
}

impl MessageKind {
    pub fn from_byte(b: u8) -> Result<Self, FaucetError> {
        Ok(match b {
            b'B' => Self::Begin,
            b'C' => Self::Commit,
            b'O' => Self::Origin,
            b'R' => Self::Relation,
            b'Y' => Self::Type,
            b'I' => Self::Insert,
            b'U' => Self::Update,
            b'D' => Self::Delete,
            b'T' => Self::Truncate,
            other => {
                return Err(FaucetError::Source(format!(
                    "pgoutput: unknown message kind {:?} (0x{other:02X})",
                    other as char
                )));
            }
        })
    }

    pub fn as_byte(&self) -> u8 {
        match self {
            Self::Begin => b'B',
            Self::Commit => b'C',
            Self::Origin => b'O',
            Self::Relation => b'R',
            Self::Type => b'Y',
            Self::Insert => b'I',
            Self::Update => b'U',
            Self::Delete => b'D',
            Self::Truncate => b'T',
        }
    }
}

/// REPLICA IDENTITY setting reported in each Relation message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaIdentity {
    Default,
    Nothing,
    Full,
    Index,
}

impl ReplicaIdentity {
    pub fn from_byte(b: u8) -> Result<Self, FaucetError> {
        Ok(match b {
            b'd' => Self::Default,
            b'n' => Self::Nothing,
            b'f' => Self::Full,
            b'i' => Self::Index,
            other => {
                return Err(FaucetError::Source(format!(
                    "pgoutput: unknown replica identity {:?} (0x{other:02X})",
                    other as char
                )));
            }
        })
    }

    pub fn as_byte(&self) -> u8 {
        match self {
            Self::Default => b'd',
            Self::Nothing => b'n',
            Self::Full => b'f',
            Self::Index => b'i',
        }
    }
}

/// One column descriptor inside a Relation message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDesc {
    /// Bit 0 = part of the replica identity key.
    pub flags: u8,
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
}

/// Decoded BEGIN message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Begin {
    pub final_lsn: u64,
    pub commit_ts: i64, // microseconds since 2000-01-01 UTC
    pub xid: u32,
}

/// Decoded COMMIT message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Commit {
    pub flags: u8,
    pub commit_lsn: u64,
    pub end_lsn: u64,
    pub commit_ts: i64, // microseconds since 2000-01-01 UTC
}

/// Decoded RELATION message — registers a relation OID and its column layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation {
    pub oid: u32,
    pub namespace: String,
    pub name: String,
    pub replica_identity: ReplicaIdentity,
    pub columns: Vec<ColumnDesc>,
}

/// Decoded TupleData column entry (one cell of a row).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TupleCell {
    /// SQL NULL.
    Null,
    /// Unchanged TOAST column — value not in the WAL; only the name is known.
    UnchangedToast,
    /// Text-encoded value (every column arrives this way when pgoutput is
    /// started in text mode, which is the default).
    Text(String),
}

/// Decoded TupleData — one row's worth of cells.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TupleData {
    pub cells: Vec<TupleCell>,
}

/// Decoded INSERT message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Insert {
    pub relation_oid: u32,
    pub new: TupleData,
}

/// What kind of "old tuple" precedes the UPDATE's NEW tuple (if any).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateOldKind {
    /// No old tuple in the message (replica identity DEFAULT, key unchanged).
    None,
    /// 'K' — only the replica-identity-key columns are in the old tuple.
    Key,
    /// 'O' — the full old tuple (REPLICA IDENTITY FULL).
    Full,
}

/// Decoded UPDATE message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Update {
    pub relation_oid: u32,
    pub old_kind: UpdateOldKind,
    pub old: Option<TupleData>,
    pub new: TupleData,
}

/// What kind of "old tuple" the DELETE carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteOldKind {
    /// `K` — only the replica-identity-key columns are in the old tuple.
    Key,
    /// `O` — the full old tuple (REPLICA IDENTITY FULL).
    Full,
}

/// Decoded DELETE message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delete {
    pub relation_oid: u32,
    pub old_kind: DeleteOldKind,
    pub old: TupleData,
}

/// Decoded TRUNCATE message — may target several relations at once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Truncate {
    pub relation_oids: Vec<u32>,
    pub cascade: bool,
    pub restart_identity: bool,
}

/// One fully-decoded pgoutput message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    Begin(Begin),
    Commit(Commit),
    /// `O` — replication origin. We accept and ignore (not relevant for v1).
    Origin,
    Relation(Relation),
    /// `Y` — custom-type registration. Accept and ignore for v1; if we ever
    /// see a column whose type OID is one of these, we fall back to text.
    Type,
    Insert(Insert),
    Update(Update),
    Delete(Delete),
    Truncate(Truncate),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_kind_byte_round_trip() {
        for byte in *b"BCORYIUDT" {
            let kind = MessageKind::from_byte(byte).expect("known kind");
            assert_eq!(kind.as_byte(), byte);
        }
    }

    #[test]
    fn message_kind_unknown_byte_errors() {
        assert!(MessageKind::from_byte(b'Z').is_err());
    }

    #[test]
    fn relation_replica_identity_round_trip() {
        for byte in *b"dnfi" {
            let id = ReplicaIdentity::from_byte(byte).expect("known");
            assert_eq!(id.as_byte(), byte);
        }
        assert!(ReplicaIdentity::from_byte(b'x').is_err());
    }
}
