use std::{
    ffi::CStr,
    fs,
    io::Cursor,
    iter::{self, zip},
    mem,
    num::ParseIntError,
    path::Path,
    str::FromStr,
    string::ParseError,
};

use milli::{
    documents::{DocumentsBatchBuilder, DocumentsBatchReader},
    heed::EnvOpenOptions,
    update::{
        IndexDocuments as MilliIndexDocuments, IndexDocumentsConfig as MilliIndexDocumentsConfig,
        IndexDocumentsMethod as MilliIndexDocumentsMethod, IndexerConfig as MilliIndexerConfig,
        Settings as MilliSettings,
    },
    Index as MilliIndex,
};
use pg_sys::{palloc0, panic::ErrorReportable, FormData_pg_attribute, ItemPointerData, Oid};
use pgrx::{prelude::*, PgMemoryContexts, PgRelation};
use thiserror::Error;

pgrx::pg_module_magic!();

extension_sql_file!("operator_class.sql");

#[pg_guard]
pub unsafe extern "C" fn _PG_init() {
    info!("{PROGRAM_NAME} loaded");
}

#[pg_extern(sql = "
    CREATE OR REPLACE FUNCTION pgmumbo_am_handler(internal) RETURNS index_am_handler
        STRICT
        LANGUAGE c
        AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';
    CREATE ACCESS METHOD pgmumbo 
        TYPE INDEX
        HANDLER pgmumbo_am_handler;
")]
fn amhandler(_fcinfo: pg_sys::FunctionCallInfo) -> PgBox<pg_sys::IndexAmRoutine> {
    let mut amroutine =
        unsafe { PgBox::<pg_sys::IndexAmRoutine>::alloc_node(pg_sys::NodeTag::T_IndexAmRoutine) };

    amroutine.amstrategies = 4;
    amroutine.amsupport = 0;
    amroutine.amcanmulticol = true;
    amroutine.amsearcharray = true;

    amroutine.amkeytype = Oid::INVALID;

    amroutine.ambuild = Some(ambuild);
    amroutine.ambuildempty = Some(ambuildempty);
    amroutine.aminsert = Some(aminsert);
    amroutine.ambulkdelete = Some(ambulkdelete);
    amroutine.amvacuumcleanup = Some(amvacuumcleanup);
    amroutine.amcostestimate = Some(amcostestimate);
    amroutine.amoptions = Some(amoptions);
    amroutine.amvalidate = Some(amvalidate);
    amroutine.ambeginscan = Some(ambeginscan);
    amroutine.amrescan = Some(amrescan);
    // amroutine.amgettuple = Some(amgettuple);
    // amroutine.amgetbitmap = Some(ambitmapscan);
    amroutine.amendscan = Some(amendscan);

    amroutine.into_pg_boxed()
}

const PROGRAM_NAME: &str = "pgmumbo";
const PGDATA_BASE: &str = "ext_pgmumbo";

const INITIAL_LMDB_MMAP_SIZE: usize = 64 * 1024 * 1024 * 1024; // Initialize LMDB with 64 GB memory map

const TID_PRIMARY_KEY: &str = "@pgmumbo_tid";

#[pg_guard]
pub extern "C" fn ambuild(
    heap_relation: pg_sys::Relation,
    index_relation: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
) -> *mut pg_sys::IndexBuildResult {
    info!("pgmumbo ambuild...");

    let heap_relation = unsafe { PgRelation::from_pg(heap_relation) };
    let index_relation = unsafe { PgRelation::from_pg(index_relation) };

    assert!(index_relation.is_index());
    info!("pgmumbo ambuild: Preflight OK!");

    // TODO setup abort callback

    let index_path = Path::new(
        unsafe { CStr::from_ptr(pg_sys::DataDir) }
            .to_str()
            .unwrap_or_report(),
    )
    .join(PGDATA_BASE)
    .join(format!("{}", unsafe { pg_sys::MyDatabaseId }.as_u32()))
    .join(format!("{}", index_relation.oid().as_u32(),));
    let milli_config = MilliIndexerConfig::default();
    let mut lmdb_options = EnvOpenOptions::new();
    lmdb_options.map_size(INITIAL_LMDB_MMAP_SIZE);
    info!("pgmumbo ambuild: Building at: {index_path:?}");
    info!("pgmumbo ambuild: Memory Map: {INITIAL_LMDB_MMAP_SIZE} bytes");

    fs::create_dir_all(&index_path).unwrap_or_report();
    let milli_index = MilliIndex::new(lmdb_options, &index_path).unwrap_or_report();
    info!("pgmumbo ambuild: Opened OK!");

    let mut build_state = BuildState {
        owned_context: PgMemoryContexts::new(PROGRAM_NAME),
        batch_builder: DocumentsBatchBuilder::new(Vec::new()),
    };

    info!("pgmumbo ambuild: HeapScan...");

    unsafe {
        pg_sys::IndexBuildHeapScan(
            heap_relation.as_ptr(),
            index_relation.as_ptr(),
            index_info,
            Some(build_callback),
            &mut build_state,
        )
    }

    info!("pgmumbo ambuild: HeapScan OK!");

    info!("pgmumbo ambuild: LMDB wtxn...");
    let documents = build_state.batch_builder.into_inner().unwrap_or_report();
    let mut wtxn = milli_index.write_txn().unwrap_or_report();

    info!("pgmumbo ambuild: Settings...");
    let mut milli_settings = MilliSettings::new(&mut wtxn, &milli_index, &milli_config);
    milli_settings.set_primary_key(TID_PRIMARY_KEY.to_string());
    milli_settings.execute(|_| {}, || false).unwrap();

    let mut indexer = MilliIndexDocuments::new(
        &mut wtxn,
        &milli_index,
        &milli_config,
        MilliIndexDocumentsConfig {
            words_prefix_threshold: None,
            max_prefix_length: None,
            words_positions_level_group_size: None,
            words_positions_min_level_size: None,
            update_method: MilliIndexDocumentsMethod::ReplaceDocuments,
            autogenerate_docids: false,
        },
        |_| {},
        || false,
    )
    .unwrap_or_report();

    info!("pgmumbo ambuild: LMDB wtxn... Execute...");
    (indexer, _) = indexer
        .add_documents(DocumentsBatchReader::from_reader(Cursor::new(documents)).unwrap_or_report())
        .unwrap_or_report();
    let index_result = indexer.execute().unwrap_or_report();

    info!("pgmumbo ambuild: LMDB wtxn... COMMIT...");
    wtxn.commit().unwrap_or_report();

    info!(
        "pgmumbo ambuild: COMMIT OK! (added {}, total {})",
        index_result.indexed_documents, index_result.number_of_documents
    );

    let mut build_result = unsafe { PgBox::<pg_sys::IndexBuildResult>::alloc0() };
    build_result.heap_tuples = index_result.number_of_documents as f64;
    build_result.index_tuples = index_result.indexed_documents as f64;
    build_result.into_pg()
}

struct BuildState {
    owned_context: PgMemoryContexts,
    batch_builder: DocumentsBatchBuilder<Vec<u8>>,
}

#[pg_guard]
unsafe extern "C" fn build_callback(
    index: pg_sys::Relation,
    tid: pg_sys::ItemPointer,
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    _tuple_is_alive: bool,
    state: *mut std::os::raw::c_void,
) {
    let build_state = (state as *mut BuildState).as_mut().unwrap();
    let mut old_owned_context = build_state.owned_context.set_as_current();

    let desc = (*index).rd_att.as_ref().unwrap();
    let natts = desc.natts as usize;

    let isnull = std::slice::from_raw_parts(isnull, natts);
    let attrs = desc.attrs.as_slice(natts);
    let values = std::slice::from_raw_parts(values, natts);

    let document = form_document(TID(tid), isnull, attrs, values);

    info!("pgmumbo heapscan: {document:?}");

    build_state
        .batch_builder
        .append_json_object(&document)
        .unwrap_or_report();

    old_owned_context.set_as_current();
    build_state.owned_context.reset();
}

struct TID(*mut pg_sys::ItemPointerData);

impl ToString for TID {
    fn to_string(&self) -> String {
        format!(
            "{}-{}_{}",
            unsafe { *(self.0) }.ip_blkid.bi_hi,
            unsafe { *(self.0) }.ip_blkid.bi_lo,
            unsafe { *(self.0) }.ip_posid
        )
    }
}
impl FromStr for TID {
    type Err = PgmumboError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (blk_str, pos_str) = s.split_once("_").ok_or(PgmumboError::TIDSplit)?;
        let (blk_hi_str, blk_lo_str) = blk_str.split_once("-").ok_or(PgmumboError::TIDSplit)?;
        let item_ptr: *mut ItemPointerData =
            unsafe { palloc0(mem::size_of::<pg_sys::ItemPointerData>()).cast() };

        unsafe { *item_ptr }.ip_blkid.bi_hi =
            blk_hi_str.parse().map_err(PgmumboError::TIDParseInt)?;
        unsafe { *item_ptr }.ip_blkid.bi_lo =
            blk_lo_str.parse().map_err(PgmumboError::TIDParseInt)?;
        unsafe { *item_ptr }.ip_posid = pos_str.parse().map_err(PgmumboError::TIDParseInt)?;

        Ok(TID(item_ptr))
    }
}

fn form_document(
    tid: TID,
    isnull: &[bool],
    attrs: &[pg_sys::FormData_pg_attribute],
    values: &[pg_sys::Datum],
) -> serde_json::Map<String, serde_json::Value> {
    serde_json::Map::from_iter(
        // Managed primary key; 1:1 mapping with TID
        iter::once((
            TID_PRIMARY_KEY.to_string(),
            serde_json::Value::String(tid.to_string()),
        ))
        // Then, convert tuple attrs and values to KV pairs
        .chain(
            zip(isnull, zip(attrs, values)).filter_map(|(isnull, (attr, value))| {
                if *isnull {
                    return None;
                }
                let detoasted = unsafe { pg_sys::pg_detoast_datum(value.cast_mut_ptr()) };
                let document_key = unsafe { CStr::from_ptr(attr.attname.data.as_ptr()) }
                    .to_str()
                    .unwrap_or_report()
                    .to_string();
                let document_value = match attr.atttypid {
                    pg_sys::TEXTOID => serde_json::Value::String(
                        unsafe {
                            CStr::from_ptr(pg_sys::text_to_cstring(
                                detoasted.cast::<pg_sys::text>(),
                            ))
                        }
                        .to_str()
                        .unwrap_or_report()
                        .to_string(),
                    ),
                    _ => serde_json::Value::Null,
                };
                Some((document_key, document_value))
            }),
        ),
    )
}

#[pg_guard]
pub extern "C" fn ambuildempty(_index_relation: pg_sys::Relation) {
    info!("pgmumbo ambuildempty");
}

#[pg_guard]
pub unsafe extern "C" fn aminsert(
    index_relation: pg_sys::Relation,
    values: *mut pg_sys::Datum,
    _isnull: *mut bool,
    heap_tid: pg_sys::ItemPointer,
    _heap_relation: pg_sys::Relation,
    _check_unique: pg_sys::IndexUniqueCheck::Type,
    _index_unchanged: bool,
    _index_info: *mut pg_sys::IndexInfo,
) -> bool {
    info!("pgmumbo aminsert...");

    todo!()
}

#[pg_guard]
pub extern "C" fn ambulkdelete(
    info: *mut pg_sys::IndexVacuumInfo,
    stats: *mut pg_sys::IndexBulkDeleteResult,
    _callback: pg_sys::IndexBulkDeleteCallback,
    _callback_state: *mut ::std::os::raw::c_void,
) -> *mut pg_sys::IndexBulkDeleteResult {
    todo!()
}

#[pg_guard]
pub extern "C" fn amvacuumcleanup(
    info: *mut pg_sys::IndexVacuumInfo,
    stats: *mut pg_sys::IndexBulkDeleteResult,
) -> *mut pg_sys::IndexBulkDeleteResult {
    todo!()
}

#[pg_guard(immutable, parallel_safe)]
pub unsafe extern "C" fn amcostestimate(
    _root: *mut pg_sys::PlannerInfo,
    path: *mut pg_sys::IndexPath,
    _loop_count: f64,
    index_startup_cost: *mut pg_sys::Cost,
    index_total_cost: *mut pg_sys::Cost,
    index_selectivity: *mut pg_sys::Selectivity,
    index_correlation: *mut f64,
    index_pages: *mut f64,
) {
}

#[pg_guard]
pub unsafe extern "C" fn amoptions(
    reloptions: pg_sys::Datum,
    validate: bool,
) -> *mut pg_sys::bytea {
    std::ptr::null::<pg_sys::bytea>() as *mut pg_sys::bytea
}

#[pg_guard]
pub extern "C" fn amvalidate(_opclassoid: pg_sys::Oid) -> bool {
    true
}

#[pg_guard]
pub extern "C" fn ambeginscan(
    index_relation: pg_sys::Relation,
    nkeys: ::std::os::raw::c_int,
    norderbys: ::std::os::raw::c_int,
) -> pg_sys::IndexScanDesc {
    todo!()
}

#[pg_guard]
pub extern "C" fn amrescan(
    scan: pg_sys::IndexScanDesc,
    keys: pg_sys::ScanKey,
    nkeys: ::std::os::raw::c_int,
    _orderbys: pg_sys::ScanKey,
    _norderbys: ::std::os::raw::c_int,
) {
}

// #[pg_guard]
// pub extern "C" fn amgettuple(
//     scan: pg_sys::IndexScanDesc,
//     _direction: pg_sys::ScanDirection,
// ) -> bool {
//     todo!()
// }

// #[pg_guard]
// pub extern "C" fn ambitmapscan(scan: pg_sys::IndexScanDesc, tbm: *mut pg_sys::TIDBitmap) -> i64 {
//     todo!()
// }

#[pg_guard]
pub extern "C" fn amendscan(_scan: pg_sys::IndexScanDesc) {}

#[derive(Error, Debug)]
pub enum PgmumboError {
    #[error("Failed to split TID serialization, it could be malformed")]
    TIDSplit,
    #[error("Failed to parse TID integer, it could be malformed")]
    TIDParseInt(ParseIntError),
}
