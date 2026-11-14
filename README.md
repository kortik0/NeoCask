[![Tests](https://github.com/kortik0/NeoCask/actions/workflows/tests.yml/badge.svg)](https://github.com/kortik0/NeoCask/actions/workflows/tests.yml)

# NeoCask

**NeoCask** is a from-scratch C# implementation of the [Bitcask](https://riak.com/assets/bitcask-intro.pdf) storage model described by Riak.

The project was built as an experiment in understanding how a simple persistent key-value storage engine can be designed and implemented from the ground up, following the original Bitcask design rather than relying on an existing storage engine.

## Overview

NeoCask is a log-structured key-value store where data is written sequentially to append-only files, while an in-memory key directory keeps track of where the latest value of each key is stored.

A write does not update data in place. Instead, a new record is appended to the active data file and the key directory is updated to point to it.

This approach makes writes simple and sequential while moving the complexity towards recovery, stale records, and storage compaction.

## Features

* Append-only data files
* In-memory key directory
* Persistent key/value records
* CRC32 integrity validation
* Automatic active-file rotation based on file size
* Tombstones for deleted keys
* Hint files for faster key-directory reconstruction
* Merge/compaction of immutable data files
* Recovery of a corrupted or partially written tail
* Configurable maximum data-file size
* Basic concurrent access protection

## Storage Format

Each record is stored sequentially in a `.ncl` data file:

```text
+--------+-----------+----------+------------+-----+-------+
| CRC32  | Timestamp | Key Size | Value Size | Key | Value |
+--------+-----------+----------+------------+-----+-------+
```

The key directory stores metadata required to locate the latest record without scanning the entire data set:

```text
Key -> File ID + Offset + Value Size + Timestamp
```

This means that reading a value generally consists of two steps:

1. Find the key metadata in memory.
2. Seek directly to the corresponding record in the data file.

Deleted keys are represented by tombstone records rather than by modifying or removing existing data.

## Hint Files

Scanning every data file on startup would become increasingly expensive as the database grows.

NeoCask therefore supports `.hncl` hint files containing the metadata necessary to reconstruct the key directory without reading every value from the corresponding data files.

This follows the same general idea used by Bitcask.

## Merge

Because the storage is append-only, updating a key leaves its previous value in the data files.

For example:

```text
PUT user = Alice
PUT user = Bob
PUT user = Charlie
```

The log contains all three records, while the key directory points only to the latest one.

The `Merge()` operation scans immutable files, keeps the latest valid record for each key, removes obsolete records and tombstones, and generates hint files for the resulting data.

This provides the compaction mechanism required by an append-only storage design.

## Recovery

NeoCask validates records using CRC32.

When opening the store, the tail of the last data file is inspected. If the file contains an incomplete or corrupted final record, NeoCask identifies the last valid offset and truncates the invalid tail.

This allows the store to recover from situations such as a process being interrupted while a record is being written.

## Why?

The main purpose of NeoCask was not to create a production-ready database.

It was an exercise in implementing a storage engine from first principles and exploring the trade-offs behind a log-structured key-value store:

* sequential writes vs. in-place updates;
* memory usage vs. disk access;
* startup recovery vs. persistent indexes;
* stale records vs. simple writes;
* append-only storage vs. compaction;
* data integrity vs. write overhead.

The design was implemented by studying the original Bitcask design documentation published by Riak.
It was interesting, a lot of fun to build, and gave me some headaches along the way.

## Status

NeoCask is an experimental project and should be considered a learning/research implementation rather than a production database.

The project intentionally keeps the storage engine relatively small so that its internal mechanics remain easy to inspect and understand.

It was actively developed during winter 2024-2025 and is not currently maintained. It ss still used under the hood in a couple of my other personal projects, but I haven't felt the need to develop it further.
