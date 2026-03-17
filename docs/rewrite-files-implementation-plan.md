# RewriteFiles Implementation Plan

## Status

`RewriteFiles` is **not implemented** in iceberg-rust v0.8.0.

### What exists
- `Operation::Replace` in `spec/snapshot.rs:61` — correct operation type for compaction
- `SnapshotProduceOperation::delete_entries()` in `transaction/snapshot.rs:71` — stub, always returns `Ok(vec![])`
- `ManifestStatus::Deleted` in the manifest spec — ready to use
- No `deleted_data_files` field in `SnapshotProducer`

### What's missing
- No `deleted_data_files` in `SnapshotProducer`
- No `write_deleted_manifest()` in `SnapshotProducer`
- `delete_entries()` is `#[allow(unused)]` and unimplemented
- No `RewriteFilesAction` transaction action
- No `transaction.rewrite_files()` method on `Transaction`

### Existing GitHub issue
An open Apache issue tracks `RewriteFiles` support for iceberg-rust (referenced as #1606 in contributor mailing list). A contributor described a working implementation but a merged PR does not exist as of v0.8.0.

---

## Iceberg Spec: What RewriteFiles Must Do

From the Iceberg spec, a `Replace` snapshot means:
> "Data and delete files were added and removed without changing table data."

This is the correct operation type for **compaction** — replacing many small files with fewer larger ones, with identical logical content.

The new snapshot's manifest list must contain:
1. **Carried-forward manifests** — existing manifests that contain none of the deleted files (reused by reference, zero I/O)
2. **New added manifest** — lists the replacement files as `ManifestStatus::Added`
3. **New deleted manifest** — lists the old files as `ManifestStatus::Deleted`

---

## File-by-File Changes

### 1. `crates/iceberg/src/transaction/snapshot.rs`

#### 1a. Add `deleted_data_files` to `SnapshotProducer`

```rust
pub(crate) struct SnapshotProducer<'a> {
    pub(crate) table: &'a Table,
    snapshot_id: i64,
    commit_uuid: Uuid,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    deleted_data_files: Vec<DataFile>,    // NEW
    manifest_counter: RangeFrom<u64>,
}
```

Update `SnapshotProducer::new()` to accept `deleted_data_files: Vec<DataFile>`.

#### 1b. Add `write_deleted_manifest()`

```rust
async fn write_deleted_manifest(&mut self) -> Result<ManifestFile> {
    let deleted_data_files = std::mem::take(&mut self.deleted_data_files);
    if deleted_data_files.is_empty() {
        return Err(Error::new(
            ErrorKind::PreconditionFailed,
            "No deleted data files found when writing a deleted manifest file",
        ));
    }

    let snapshot_id = self.snapshot_id;
    let format_version = self.table.metadata().format_version();

    let manifest_entries = deleted_data_files.into_iter().map(|data_file| {
        let builder = ManifestEntry::builder()
            .status(ManifestStatus::Deleted)
            .snapshot_id(snapshot_id)
            .data_file(data_file);
        builder.build()
    });

    let mut writer = self.new_manifest_writer(ManifestContentType::Data)?;
    for entry in manifest_entries {
        writer.add_entry(entry)?;
    }
    writer.write_manifest_file().await
}
```

#### 1c. Wire `delete_entries()` into `manifest_file()`

Replace the `// # TODO Support process delete entries` comment block:

```rust
// Process deleted entries (e.g., for RewriteFiles / compaction)
if !self.deleted_data_files.is_empty() {
    let deleted_manifest = self.write_deleted_manifest().await?;
    manifest_files.push(deleted_manifest);
}
```

#### 1d. Add `validate_deleted_data_files_exist()`

```rust
pub(crate) async fn validate_deleted_data_files_exist(&self) -> Result<()> {
    let files_to_delete: HashSet<&str> = self
        .deleted_data_files
        .iter()
        .map(|df| df.file_path.as_str())
        .collect();

    if files_to_delete.is_empty() {
        return Ok(());
    }

    let mut found: HashSet<&str> = HashSet::new();

    if let Some(snapshot) = self.table.metadata().current_snapshot() {
        let manifest_list = snapshot
            .load_manifest_list(self.table.file_io(), &self.table.metadata_ref())
            .await?;

        for manifest_entry in manifest_list.entries() {
            let manifest = manifest_entry.load_manifest(self.table.file_io()).await?;
            for entry in manifest.entries() {
                if entry.is_alive() && files_to_delete.contains(entry.file_path()) {
                    found.insert(entry.file_path());
                }
            }
        }
    }

    let not_found: Vec<&&str> = files_to_delete
        .iter()
        .filter(|f| !found.contains(**f))
        .collect();

    if !not_found.is_empty() {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!(
                "Cannot delete files that are not in the current snapshot: {}",
                not_found.iter().map(|f| **f).collect::<Vec<_>>().join(", ")
            ),
        ));
    }

    Ok(())
}
```

#### 1e. Update `manifest_file()` guard

The existing guard:
```rust
if self.added_data_files.is_empty() && self.snapshot_properties.is_empty() {
    return Err(...);
}
```
Must be updated to also allow an operation with only deletions:
```rust
if self.added_data_files.is_empty()
    && self.deleted_data_files.is_empty()
    && self.snapshot_properties.is_empty()
{
    return Err(...);
}
```

---

### 2. `crates/iceberg/src/transaction/rewrite.rs` (new file)

```rust
// Licensed to the Apache Software Foundation (ASF) under one
// ... (ASF license header)

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::Result;
use crate::spec::{DataFile, ManifestEntry, ManifestFile, Operation};
use crate::table::Table;
use crate::transaction::snapshot::{
    DefaultManifestProcess, SnapshotProduceOperation, SnapshotProducer,
};
use crate::transaction::{ActionCommit, TransactionAction};

/// `RewriteFilesAction` atomically removes a set of existing data files and
/// adds replacement data files in a single `Replace` snapshot.
///
/// This is the correct action for **compaction**: the logical table content
/// does not change, only the physical file layout.
///
/// # Example
/// ```ignore
/// let action = tx.rewrite_files()
///     .delete_data_files(old_files)
///     .add_data_files(new_compacted_files);
/// let tx = action.apply(tx).unwrap();
/// tx.commit(&catalog).await?;
/// ```
pub struct RewriteFilesAction {
    commit_uuid: Option<Uuid>,
    key_metadata: Option<Vec<u8>>,
    snapshot_properties: HashMap<String, String>,
    added_data_files: Vec<DataFile>,
    deleted_data_files: Vec<DataFile>,
}

impl RewriteFilesAction {
    pub(crate) fn new() -> Self {
        Self {
            commit_uuid: None,
            key_metadata: None,
            snapshot_properties: HashMap::default(),
            added_data_files: vec![],
            deleted_data_files: vec![],
        }
    }

    /// Files to add (the new, compacted replacement files).
    pub fn add_data_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.added_data_files.extend(files);
        self
    }

    /// Files to delete (the old files being replaced).
    pub fn delete_data_files(mut self, files: impl IntoIterator<Item = DataFile>) -> Self {
        self.deleted_data_files.extend(files);
        self
    }

    /// Set commit UUID.
    pub fn set_commit_uuid(mut self, commit_uuid: Uuid) -> Self {
        self.commit_uuid = Some(commit_uuid);
        self
    }

    /// Set key metadata for manifest files.
    pub fn set_key_metadata(mut self, key_metadata: Vec<u8>) -> Self {
        self.key_metadata = Some(key_metadata);
        self
    }

    /// Set snapshot summary properties.
    pub fn set_snapshot_properties(mut self, props: HashMap<String, String>) -> Self {
        self.snapshot_properties = props;
        self
    }
}

#[async_trait]
impl TransactionAction for RewriteFilesAction {
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
        let snapshot_producer = SnapshotProducer::new(
            table,
            self.commit_uuid.unwrap_or_else(Uuid::now_v7),
            self.key_metadata.clone(),
            self.snapshot_properties.clone(),
            self.added_data_files.clone(),
            self.deleted_data_files.clone(),
        );

        // Validate files to delete actually exist in the current snapshot.
        snapshot_producer.validate_deleted_data_files_exist().await?;

        // Validate added files are not already referenced.
        snapshot_producer.validate_duplicate_files().await?;

        snapshot_producer
            .commit(RewriteFilesOperation, DefaultManifestProcess)
            .await
    }
}

// ---------------------------------------------------------------------------
// Internal SnapshotProduceOperation implementation
// ---------------------------------------------------------------------------

struct RewriteFilesOperation;

impl SnapshotProduceOperation for RewriteFilesOperation {
    fn operation(&self) -> Operation {
        // "Replace" = files added and removed without changing table data (compaction).
        Operation::Replace
    }

    async fn delete_entries(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestEntry>> {
        // The SnapshotProducer handles writing the deleted manifest directly.
        // This method is kept for trait compliance; return empty.
        Ok(vec![])
    }

    async fn existing_manifest(
        &self,
        snapshot_produce: &SnapshotProducer<'_>,
    ) -> Result<Vec<ManifestFile>> {
        let Some(snapshot) = snapshot_produce.table.metadata().current_snapshot() else {
            return Ok(vec![]);
        };

        // Collect the file paths being deleted so we can skip manifests that
        // contain them (those manifests will be replaced by the new deleted manifest).
        let deleted_paths: HashSet<&str> = snapshot_produce
            .deleted_data_files
            .iter()
            .map(|df| df.file_path.as_str())
            .collect();

        let manifest_list = snapshot
            .load_manifest_list(
                snapshot_produce.table.file_io(),
                &snapshot_produce.table.metadata_ref(),
            )
            .await?;

        let mut existing_manifests = Vec::new();
        for manifest_entry in manifest_list.entries() {
            // Check whether this manifest touches any deleted file.
            let manifest = manifest_entry.load_manifest(snapshot_produce.table.file_io()).await?;
            let touches_deleted = manifest.entries().iter().any(|e| {
                e.is_alive() && deleted_paths.contains(e.file_path())
            });

            if !touches_deleted {
                // Carry this manifest forward unchanged.
                existing_manifests.push(manifest_entry.clone());
            }
            // Manifests that DO touch deleted files are dropped here;
            // SnapshotProducer will write a new deleted manifest for those files.
        }

        Ok(existing_manifests)
    }
}
```

---

### 3. `crates/iceberg/src/transaction/mod.rs`

```rust
mod rewrite;   // add this line alongside the other mod declarations

use crate::transaction::rewrite::RewriteFilesAction;   // add to use list

impl Transaction {
    // ... existing methods ...

    /// Creates a rewrite files action (compaction).
    ///
    /// Atomically removes old data files and adds replacement files.
    /// The snapshot operation type is `Replace` — table data is logically unchanged.
    pub fn rewrite_files(&self) -> RewriteFilesAction {
        RewriteFilesAction::new()
    }
}
```

---

## Tests to Write

In `crates/iceberg/src/transaction/rewrite.rs`:

| Test | Description |
|---|---|
| `test_rewrite_basic` | Append 3 files, rewrite 2 into 1. Verify snapshot has `Operation::Replace`, final manifest list has 2 manifests (1 carried-forward + 1 new added + 1 new deleted). |
| `test_rewrite_deleted_file_not_in_snapshot` | Attempt to delete a file not in the snapshot. Expect `DataInvalid` error. |
| `test_rewrite_no_added_no_deleted_fails` | Call with no added and no deleted files. Expect error. |
| `test_rewrite_all_files` | Rewrite all files in the table. Verify the snapshot has no carried-forward manifests. |
| `test_rewrite_roundtrip_catalog` | Full round-trip through memory catalog. Load table after commit and verify file count. |
| `test_rewrite_preserves_unaffected_manifests` | Two partitions, rewrite only one. Verify the other partition's manifest is carried forward. |
| `test_rewrite_summary` | Verify snapshot summary contains correct `deleted-data-files`, `added-data-files`, `deleted-records`, `added-records` keys. |

---

## Usage Example (after implementation)

```rust
use iceberg::transaction::{ApplyTransactionAction, Transaction};

// Step 1: write new compacted files via a writer
let new_file: DataFile = /* ... compact old_file_1 + old_file_2 ... */;

// Step 2: commit the rewrite atomically
let tx = Transaction::new(&table);
let action = tx.rewrite_files()
    .delete_data_files(vec![old_file_1, old_file_2])
    .add_data_files(vec![new_file]);
let tx = action.apply(tx)?;
let updated_table = tx.commit(&catalog).await?;
```

---

## Relationship to Other Missing Operations

Once `RewriteFilesAction` is done, it establishes the `deleted_data_files` + delete-manifest infrastructure in `SnapshotProducer`. The same infrastructure can then be reused for:

- **`OverwriteFilesAction`** (`Operation::Overwrite`) — same producer plumbing, different `existing_manifest()` logic (filter by expression)
- **`ReplacePartitionsAction`** (`Operation::Overwrite`) — drop entire partitions, add replacements
- **`DeleteFilesAction`** (`Operation::Delete`) — only deletions, no additions

---

## References

- [Apache Iceberg Spec — Snapshots](https://iceberg.apache.org/spec/#snapshots)
- [iceberg-rust RewriteFiles issue (mailing list)](https://www.mail-archive.com/issues@iceberg.apache.org/msg184242.html)
- [iceberg-java RewriteFiles API](https://github.com/apache/iceberg/blob/main/api/src/main/java/org/apache/iceberg/RewriteFiles.java)
- `crates/iceberg/src/transaction/snapshot.rs` — `SnapshotProducer`
- `crates/iceberg/src/transaction/append.rs` — `FastAppendAction` (model to follow)
