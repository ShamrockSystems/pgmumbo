<div align="center">
    <img alt="pgmumbo logo" src="pgmumbo.svg" width=400>
</div>

- [x] Build heap scanner
  - [x] Figure out heap relation structure
  - [x] Determine primary key
  - [x] Tuple isnull & values slide document former
  - [x] Setup scanning function
  - [x] Setup scanning callback
- [ ] access method
  - [x] ambuild
  - [x] ambuildempty
  - [x] aminsert
  - [ ] ambulkdelete
  - [ ] amvacuumcleanup
  - [ ] amoptions
  - [x] amvalidate
  - [ ] ambeginscan
  - [ ] ambrescan
- [ ] operator class
  - [ ] anyelement `?mq=` (query only)
  - [ ] anyelement `?mqf=` (query with filters)
- [ ] PG XACT <-> LMDB RW XACT abort callback
- [ ] LMDB memory map extension (VACUUM free ratio & LMDB RW XACT failure)

~~Create canonical naming system for milli indices per namespaced OID. Allow multiple postgres indices to act on a single milli index. Maybe it'd be best to implement some sort of reference counting system at the extension-level? That way we can have a "master" index and "slave" indices. Otherwise stuff like `aminsert` would just not work properly.~~

Honestly for simplicity let's not implement this for now. Nuking and reindexing is fine. No-one cares.

~~I think it'd be most wise to set up something like an executor manager where we build up a state of documents, and then at the end of the scan bulk apply all of that to the milli index in a single LMDB RW transaction.~~

Need to figure out whether I can keep the LMDB memory map open as soon as the index is initialized and whether I can just assume that that mmap continues to be open.

This thing stores data in the `${PGDATA}/ext_pgmumbo/` directory; that's where it tells milli to put the LMDB directories (in subdirs per database OID and heap OID). I really can't figure out right now if there's a better way to store this stuff. LMDB just stores data on disk and I'm pretty sure we can't run milli on just postgres relations effectively.

In the future it might be worth supporting a custom data directory in the vein of tablespaces, for example if there is want to store LMDB on a different filesystem for some reason.

The LMDB map needs to grow if a milli write transaction fails with an mmap full error. I think we can look up the current size of the mmap using `heed` itself so there shouldn't be a need to store mmap size independently.
