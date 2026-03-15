# iceberg-rust vs iceberg-java: Gap Analysis & Roadmap

> Generated: 2026-03-15
> iceberg-rust version: 0.8.0
> iceberg-java reference version: 1.10.0

---

## 1. Executive Summary

`iceberg-rust` (v0.8.0) is a capable library covering the Iceberg V1/V2 core: Parquet reads/writes, partition transforms, equality/position deletes, five catalog backends, and a DataFusion integration. However, the Java reference implementation (v1.10.0) has moved substantially ahead on Format V3 features, write-mode ergonomics, file-format breadth, and ecosystem integrations.

The table below scores each area on a 0–3 scale (0 = absent, 1 = partial, 2 = mostly complete, 3 = full parity).

| Area | iceberg-java | iceberg-rust | Parity |
|---|---|---|---|
| Iceberg Spec V1/V2 core | ✅ | ✅ | 3/3 |
| Iceberg Spec V3 | ✅ 1.8–1.10 | ❌ | 0/3 |
| Parquet read/write | ✅ | ✅ | 3/3 |
| ORC read/write | ✅ | ❌ | 0/3 |
| Avro (data files) | ✅ | ❌ (metadata only) | 0/3 |
| LZ4 compression | ✅ | ❌ | 0/3 |
| Deletion Vectors (Puffin) | ✅ V3 | ❌ TODO | 0/3 |
| View support | ✅ full | ⚠️ metadata structs only | 1/3 |
| Branching & Tagging | ✅ | ❌ | 0/3 |
| Nested column selection | ✅ | ❌ | 0/3 |
| Copy-on-Write write mode | ✅ | ⚠️ implicit | 1/3 |
| Merge-on-Read read path | ✅ | ⚠️ partial (eq+pos deletes) | 2/3 |
| Overwrite / Replace | ✅ | ❌ | 0/3 |
| Row Lineage | ✅ V3 | ❌ | 0/3 |
| Variant type | ✅ V3 | ❌ | 0/3 |
| Geospatial types | ✅ V3 | ❌ | 0/3 |
| Default column values | ✅ V3 | ❌ | 0/3 |
| Multi-arg transforms | ✅ V3 | ❌ | 0/3 |
| Table encryption | ✅ V3 | ❌ | 0/3 |
| Nessie catalog | ✅ | ❌ | 0/3 |
| Spark integration | ✅ | ❌ | 0/3 |
| Flink integration | ✅ | ❌ | 0/3 |
| DataFusion integration | ❌ | ✅ | — |
| Azure ADLS (full) | ✅ | ⚠️ experimental, no conn str | 1/3 |
| Incremental reads | ✅ | ❌ | 0/3 |
| ArrowReaderOptions | ✅ | ⚠️ defined, not respected | 1/3 |

---

## 2. Detailed Gap Analysis

### Gap 1 — Format Version 3 (V3) Features *(HIGH priority)*

iceberg-java shipped full V3 support across 1.8.0–1.10.0. iceberg-rust has no V3 support today.

#### 1a. Deletion Vectors (DVs)
- **What**: Row-level deletes stored as compact Roaring Bitmaps inside Puffin files. Replaces positional-delete Avro files for V3 tables, dramatically improving merge speed.
- **Java**: Stable since 1.8.0. DVs are surfaced in the `position_deletes` metadata table.
- **Rust**: `delete_vector.rs` exists but is not wired to Puffin. `delete_file_index.rs:14` has `// TODO: Deletion Vector support`.
- **Files to touch**: `crates/iceberg/src/delete_vector.rs`, `crates/iceberg/src/delete_file_index.rs`, `crates/iceberg/src/arrow/caching_delete_file_loader.rs`, `crates/iceberg/src/puffin/`.

#### 1b. Variant Type
- **What**: A flexible semi-structured column type for JSON-like data without strict schema enforcement.
- **Java**: Available since 1.8.0.
- **Rust**: Not present. Would require new `PrimitiveType::Variant` variant in `crates/iceberg/src/spec/datatypes.rs` and Arrow mapping.

#### 1c. Geospatial Types
- **What**: `Geometry` and `Geography` column types for spatial analytics.
- **Java**: Available since 1.8.0.
- **Rust**: Not present. Requires new type variants and serialization.

#### 1d. Row Lineage
- **What**: Table metadata fields that allow engines to detect row-level changes between commits for incremental processing.
- **Java**: Available since 1.9.0.
- **Rust**: Not present. Requires `TableMetadata` additions and snapshot summary fields.

#### 1e. Default Column Values
- **What**: Schema-level default values for columns added during schema evolution.
- **Java**: Available since 1.9.0.
- **Rust**: Not present. Requires `NestedField` changes in `crates/iceberg/src/spec/datatypes.rs`.

#### 1f. Multi-Argument Transforms
- **What**: Partition transforms that take more than one input column.
- **Java**: Available since 1.9.0.
- **Rust**: Not present. Transform enum in `crates/iceberg/src/spec/transform.rs` only supports single-column.

#### 1g. Table Encryption
- **What**: Built-in column/file encryption with key management metadata.
- **Java**: Available since 1.10.0.
- **Rust**: Not present.

---

### Gap 2 — ORC File Format *(MEDIUM priority)*

- **Java**: Full read/write via `iceberg-orc` module.
- **Rust**: `FileFormat::Orc` enum variant exists in `crates/iceberg/src/spec/manifest/data_file.rs:176` but there is no reader or writer implementation. `crates/iceberg/src/writer/file_writer/mod.rs` lists "parquet, orc" in a comment but ORC is unimplemented.
- **Impact**: Tables created by Java engines using ORC cannot be read by iceberg-rust.

---

### Gap 3 — Avro Data Files *(MEDIUM priority)*

- **Java**: Supports Avro-format data files (not just Avro for metadata).
- **Rust**: Avro is used only for manifest/metadata serialization. There is no `FileFormat::Avro` data reader/writer.
- **Location**: `crates/iceberg/src/avro/mod.rs` — only handles spec serialization.

---

### Gap 4 — LZ4 Compression *(LOW-MEDIUM priority)*

- **Java**: Supports LZ4, zstd, gzip, snappy.
- **Rust**: `crates/iceberg/src/compression.rs` explicitly returns errors for LZ4: "LZ4 decompression is not supported currently". Same gap in `crates/iceberg/src/puffin/`.
- **Impact**: Cannot read tables or Puffin files compressed with LZ4.

---

### Gap 5 — View Support *(MEDIUM priority)*

- **Java**: Full view lifecycle — create, replace, load, drop, list; view versioning and dialect support.
- **Rust**:
  - Metadata structs exist: `crates/iceberg/src/spec/view_metadata.rs`, `view_metadata_builder.rs`, `view_version.rs`.
  - The `Catalog` trait in `crates/iceberg/src/catalog/mod.rs` has no view methods.
  - No catalog implementation handles view CRUD.
- **What's needed**: Add view methods to the `Catalog` trait and implement in at least the REST and Memory catalogs.

---

### Gap 6 — Branching & Tagging *(MEDIUM priority)*

- **Java**: Full branch and tag lifecycle — create, replace, fast-forward, cherry-pick; branch-level retention policies; used for WAP (Write-Audit-Publish) workflows.
- **Rust**: Snapshot references (`SnapshotRef`) exist in the spec but there are no transaction actions for creating/managing branches or tags.
- **Files to add**: New `transaction/branch.rs`, `transaction/tag.rs` action types; expose in `Transaction` builder.

---

### Gap 7 — Nested Field Column Selection *(MEDIUM priority)*

- **Java**: Supports selecting individual nested struct fields in scans, pushes projection to Parquet column chunk level.
- **Rust**: Explicit error in `crates/iceberg/src/scan/mod.rs`: *"Column {column_name} is not a direct child of schema but a nested field, which is not supported now"*.
- **Impact**: Queries on wide nested schemas must read all columns, inflating I/O.

---

### Gap 8 — Write Modes: Overwrite & Copy-on-Write *(MEDIUM priority)*

- **Java**: Provides `OverwriteFiles` (replace data matching a filter), `ReplacePartitions` (full partition replacement), and `RewriteFiles` (CoW rewrites). Both CoW and MoR strategies are explicit, documented APIs.
- **Rust**: Only `FastAppendAction` is implemented. There is no `OverwriteFiles` or `ReplacePartitions` transaction action. CoW rewrites are not available.
- **Files to add**: `crates/iceberg/src/transaction/overwrite.rs`, `crates/iceberg/src/transaction/replace_partitions.rs`.

---

### Gap 9 — Incremental Reads *(LOW-MEDIUM priority)*

- **Java**: `IncrementalAppendScan` reads only files added between two snapshots. Useful for streaming/CDC pipelines.
- **Rust**: No incremental scan API. Users must manually diff snapshot manifest lists.

---

### Gap 10 — Nessie Catalog *(LOW priority)*

- **Java**: Official `NessieCatalog` + Nessie's own REST catalog compatibility.
- **Rust**: No Nessie catalog. Since Nessie now speaks the Iceberg REST Catalog spec, the REST catalog in `crates/catalog/rest/` may work already — but this is untested and undocumented.

---

### Gap 11 — Engine Integrations *(LOW-MEDIUM priority)*

- **Java**: Spark (read/write, DML, streaming), Flink (read/write, streaming sink with schema evolution), Hive.
- **Rust**: Only DataFusion integration in `crates/integrations/datafusion/`. No Spark connector, no Flink connector. (Spark/Flink are JVM-based so native Rust connectors are architecture-dependent; the pragmatic path is via Arrow Flight or the REST catalog.)

---

### Gap 12 — ArrowReaderOptions Not Fully Respected *(LOW priority)*

- **Java**: Fine-grained reader options (batch size, row group filtering, dictionary encoding hints).
- **Rust**: `crates/iceberg/src/arrow/reader.rs` has a `TODO: ArrowReaderOptions not respected` comment. Options struct exists but is ignored in the read path.

---

### Gap 13 — Azure ADLS Full Support *(LOW priority)*

- **Rust**: `storage-azdls` is marked experimental. `crates/iceberg/src/io/storage/opendal/azdls.rs` explicitly notes "connection string currently not supported".
- **Java**: Full ADLS support including SAS tokens and connection strings.

---

### Gap 14 — Complex Type Conversions *(LOW priority)*

- `crates/iceberg/src/spec/values/datum.rs` has `// TODO: implement more type conversions`.
- `crates/iceberg/src/spec/values/literal.rs` has four `todo!()` / `unimplemented!()` call sites for Fixed, Binary, AboveMax, BelowMin literal conversions.

---

## 3. Implementation Roadmap

Gaps are grouped into four milestones ordered by ecosystem impact and implementation complexity.

---

### Milestone 1 — Write Completeness & MoR Correctness (v0.9)
*Target: makes iceberg-rust usable for production write workloads*

| Task | Effort | Files |
|---|---|---|
| **M1-1** Overwrite transaction action (`OverwriteFiles`) | M | `transaction/overwrite.rs` |
| **M1-2** Replace partitions action (`ReplacePartitions`) | M | `transaction/replace_partitions.rs` |
| **M1-3** Deletion Vector read path (Puffin → Roaring Bitmap) | L | `delete_vector.rs`, `delete_file_index.rs`, `arrow/caching_delete_file_loader.rs` |
| **M1-4** Fix `todo!()`/`unimplemented!()` in literal/datum conversion | S | `spec/values/literal.rs`, `spec/values/datum.rs` |
| **M1-5** Respect `ArrowReaderOptions` in reader | S | `arrow/reader.rs` |

**Effort key**: S = 1–3 days, M = 1–2 weeks, L = 2–4 weeks

---

### Milestone 2 — View & Branch/Tag Support (v0.10)
*Target: brings iceberg-rust to parity with common catalog operations*

| Task | Effort | Files |
|---|---|---|
| **M2-1** View methods on `Catalog` trait | S | `catalog/mod.rs` |
| **M2-2** View CRUD in Memory catalog | S | `catalog/memory/` |
| **M2-3** View CRUD in REST catalog | M | `crates/catalog/rest/` |
| **M2-4** Branch create/replace/fast-forward transaction actions | M | `transaction/branch.rs` |
| **M2-5** Tag create/replace transaction actions | S | `transaction/tag.rs` |
| **M2-6** Branch/tag retention policies | S | `spec/snapshot.rs` |
| **M2-7** Nested field column selection in scans | M | `scan/mod.rs`, `arrow/reader.rs` |
| **M2-8** Incremental append scan (`IncrementalAppendScan`) | M | `scan/incremental.rs` |

---

### Milestone 3 — Format V3 Core (v0.11)
*Target: V3 spec compliance for modern table formats*

| Task | Effort | Files |
|---|---|---|
| **M3-1** Deletion Vectors write path (DV → Puffin) | L | `puffin/`, `writer/`, `transaction/` |
| **M3-2** Default column values in schema | M | `spec/datatypes.rs`, `spec/schema/` |
| **M3-3** Row lineage metadata fields | M | `spec/table_metadata.rs`, `spec/snapshot.rs` |
| **M3-4** Variant type (spec + Arrow mapping) | L | `spec/datatypes.rs`, `arrow/schema.rs` |
| **M3-5** Geospatial types (Geometry, Geography) | L | `spec/datatypes.rs`, `arrow/schema.rs` |
| **M3-6** Multi-argument transforms | M | `spec/transform.rs`, `spec/partition.rs` |
| **M3-7** LZ4 compression support | S | `compression.rs`, `puffin/` |
| **M3-8** Table encryption key metadata | L | new `spec/encryption.rs`, `io/` |

---

### Milestone 4 — File Format & Storage Parity (v0.12)
*Target: reads all real-world Iceberg tables regardless of file format*

| Task | Effort | Files |
|---|---|---|
| **M4-1** ORC reader (via `orc-rust` crate or `arrow-orc`) | XL | `writer/file_writer/orc_writer.rs`, `arrow/reader.rs` |
| **M4-2** ORC writer | XL | `writer/file_writer/orc_writer.rs` |
| **M4-3** Avro data file reader | L | `avro/`, `arrow/reader.rs` |
| **M4-4** Azure ADLS connection string + SAS token | S | `io/storage/opendal/azdls.rs` |
| **M4-5** Nessie catalog validation (REST-based, add tests + docs) | S | `crates/catalog/rest/`, docs |

**Effort key**: XL = 1–2 months (major feature)

---

## 4. Priority Matrix

```
HIGH IMPACT, LOW EFFORT  (do first)
  M1-4  Fix unimplemented!() literal conversions
  M1-5  Respect ArrowReaderOptions
  M3-7  LZ4 compression
  M4-4  Azure ADLS connection string

HIGH IMPACT, MEDIUM EFFORT  (core milestones)
  M1-1  Overwrite transaction action
  M1-2  Replace partitions
  M1-3  Deletion Vector read path
  M2-1/2/3  View support
  M2-4/5  Branching & tagging
  M2-7  Nested column selection

HIGH IMPACT, HIGH EFFORT  (V3 strategic)
  M3-1  Deletion Vector write path
  M3-4  Variant type
  M3-5  Geospatial types
  M3-8  Table encryption
  M4-1/2  ORC read/write
```

---

## 5. Dependencies & Prerequisites

Before starting the milestones above, the following prerequisites should be in place:

1. **Roaring Bitmap crate** — needed for Deletion Vectors (M1-3, M3-1). `roaring` crate is the standard choice.
2. **ORC crate evaluation** — `orc-rust` or `arrow-orc`; needs benchmarking vs. Java ORC reader before committing (M4-1).
3. **geozero / geo-types** — candidate crates for Geospatial type Arrow integration (M3-5).
4. **Key Management Interface** — table encryption (M3-8) requires deciding on a KMS abstraction (AWS KMS, GCP KMS, or generic KMIP).

---

## 6. Testing Strategy

Each milestone should add:

- **Unit tests** alongside each new source file
- **Integration tests** in `crates/integration_tests/tests/` using real catalog backends (existing Docker-based test setup)
- **Compatibility tests**: write from iceberg-java, read from iceberg-rust (and vice versa) for each new feature
- **Spec compliance tests**: the official Iceberg spec test suite should be run for each V3 feature

---

## Sources

- [Apache Iceberg Releases](https://iceberg.apache.org/releases/)
- [Apache Iceberg Spec](https://iceberg.apache.org/spec/)
- [Apache Iceberg 1.10 Blog (Google)](https://opensource.googleblog.com/2025/09/apache-iceberg-110-maturing-the-v3-spec-the-rest-api-and-google-contributions.html)
- [Branching and Tagging Docs](https://iceberg.apache.org/docs/latest/branching/)
- [GitHub – apache/iceberg (Java)](https://github.com/apache/iceberg)
- [GitHub – apache/iceberg-rust](https://github.com/apache/iceberg-rust)
