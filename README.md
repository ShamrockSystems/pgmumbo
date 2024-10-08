<div align="center">
    <img alt="pgmumbo logo" src="pgmumbo.svg" width=400>
</div>

#### In-dev checklist:

- [x] access method
  - [x] ambuild
    - [x] setup builder callback
    - [x] support index relation tablespaces
  - [x] ambuildempty
  - [x] aminsert
  - [x] ambulkdelete
  - [x] amvacuumcleanup
  - [x] amcanreturn
  - [x] amoptions
  - [x] amvalidate
  - [x] ambeginscan
  - [x] tear apart milli's `execute_search` into three pieces
    - [x] setup
    - [x] compute roaring bitmap
    - [ ] perform start-end paginated bucket sort
  - [x] ambrescan (setup and compute bitmap)
  - [x] amgetbitmap
    - [x] figure out how to convert roaringbitmap into a TIDBitmap ~~(roaring document IDs do not map easily to TIDs... do we really need to grab each document à la `milli::PrimaryKey`??)~~ Answer: yes, for now
  - [ ] amgettuple
  - [x] ammarkpos
  - [x] amrestrpos
  - [x] amendscan
- [ ] operator class
  - [ ] `anyelement ?= ("query")`
  - [ ] `anyelement ?= ("query", "filter expression")`
- [x] ~~PG XACT <-> LMDB RW XACT abort callback~~ (heed drops the LMDB txn properly with Drop impl, and we unwind)
- [ ] LMDB automatic memory map extension (free ratio check during VACUUM & LMDB RW XACT MMap Full failure)... inspect DB w/ `heed` before reopening?
- [ ] review MVCC semantics
- [ ] review meili reindexing semantics

```sql
CREATE INDEX ON foo USING pgmumbo ((foo)) INCLUDE (a, b, c) WITH (
  ms_searchable_fields='["b", "c"]',
  ms_stop_words='["whack"]',
);

SELECT * FROM foo WHERE foo @? pgmumbo_query("search term");
SELECT * FROM foo WHERE foo @? pgmumbo_filter("milli filter expression");
SELECT * FROM foo WHERE foo @? pgmumbo_query("search term", "milli filter expression");
```

This thing stores data in the `${PGDATA}/ext_pgmumbo/` (or `${TABLESPACE}/ext_pgmumbo/`) directory; that's where it opens the LMDB directories (in subdirs per database OID and index OID). I really can't figure out right now if there's a better way to store this stuff. LMDB just stores data on disk and I'm pretty sure we can't run milli on just postgres relations effectively.

The LMDB map needs to grow if a LMDB write transaction fails with an mmap full error. I think we can look up the current size of the mmap using `heed` itself so there shouldn't be a need to store mmap size independently.
