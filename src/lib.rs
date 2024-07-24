use std::{
    borrow::Borrow,
    collections::{BTreeMap, BTreeSet, HashSet},
    convert::Infallible,
    ffi::CStr,
    fs,
    io::{self, Cursor},
    iter::{self, zip},
    mem,
    num::ParseIntError,
    option,
    os::raw,
    path::{Path, PathBuf},
    ptr, slice,
    str::FromStr,
    string::ParseError,
};

use lazy_static::lazy_static;
use milli::{
    documents::{DocumentsBatchBuilder, DocumentsBatchReader},
    heed::EnvOpenOptions,
    order_by_map::OrderByMap as MilliOrderByMap,
    update::{
        IndexDocuments as MilliIndexDocuments, IndexDocumentsConfig as MilliIndexDocumentsConfig,
        IndexDocumentsMethod as MilliIndexDocumentsMethod, IndexerConfig as MilliIndexerConfig,
        Settings as MilliSettings,
    },
    Criterion as MilliCriterion, Index as MilliIndex,
};
use pg_sys::{palloc0, panic::ErrorReportable, FormData_pg_attribute, ItemPointerData, Oid};
use pgrx::{prelude::*, PgMemoryContexts, PgRelation};
use serde::{Deserialize, Serialize};
use serde_json::Value;
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

    let index_path = lmdb_location(index_relation.rd_node).unwrap_or_report();
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

    let isnull = slice::from_raw_parts(isnull, natts);
    let attrs = desc.attrs.as_slice(natts);
    let values = slice::from_raw_parts(values, natts);

    let document = form_document(Tid(tid), isnull, attrs, values);

    info!("pgmumbo heapscan: {document:?}");

    build_state
        .batch_builder
        .append_json_object(&document)
        .unwrap_or_report();

    old_owned_context.set_as_current();
    build_state.owned_context.reset();
}

struct Tid(*mut pg_sys::ItemPointerData);

impl ToString for Tid {
    fn to_string(&self) -> String {
        format!(
            "{}-{}_{}",
            unsafe { *(self.0) }.ip_blkid.bi_hi,
            unsafe { *(self.0) }.ip_blkid.bi_lo,
            unsafe { *(self.0) }.ip_posid
        )
    }
}
impl FromStr for Tid {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (blk_str, pos_str) = s.split_once("_").ok_or(Error::TidSplit)?;
        let (blk_hi_str, blk_lo_str) = blk_str.split_once("-").ok_or(Error::TidSplit)?;
        let item_ptr: *mut ItemPointerData =
            unsafe { palloc0(mem::size_of::<pg_sys::ItemPointerData>()).cast() };

        unsafe { *item_ptr }.ip_blkid.bi_hi = blk_hi_str.parse().map_err(Error::TidParseInt)?;
        unsafe { *item_ptr }.ip_blkid.bi_lo = blk_lo_str.parse().map_err(Error::TidParseInt)?;
        unsafe { *item_ptr }.ip_posid = pos_str.parse().map_err(Error::TidParseInt)?;

        Ok(Tid(item_ptr))
    }
}

fn form_document(
    tid: Tid,
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

const PGDATA_TBLSPC: &str = "pg_tblspc";

fn spc_location(spc: Oid) -> Result<PathBuf, Error> {
    let my_database_spc = unsafe { pg_sys::MyDatabaseTableSpace };
    let mut spc = spc;

    let pgdata = Path::new(
        unsafe { CStr::from_ptr(pg_sys::DataDir) }
            .to_str()
            .unwrap_or_report(),
    );

    if spc == Oid::INVALID {
        spc = my_database_spc;
    }

    if spc == pg_sys::DEFAULTTABLESPACE_OID || spc == pg_sys::GLOBALTABLESPACE_OID {
        return Ok(pgdata.into());
    }

    pgdata
        .join(PGDATA_TBLSPC)
        .join(spc.as_u32().to_string())
        .canonicalize()
        .map_err(Error::SpaceResolve)
}

const PGDATA_EXT_BASE: &str = "ext_pgmumbo";

fn lmdb_location(node: pg_sys::RelFileNode) -> Result<PathBuf, Error> {
    Ok(spc_location(node.spcNode)?
        .join(PGDATA_EXT_BASE)
        .join(node.dbNode.as_u32().to_string())
        .join(node.relNode.as_u32().to_string()))
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

#[derive(Default, Serialize, Deserialize)]
struct PgmumboOptions {
    // Unmanaged MilliSettings fields
    m_searchable_fields: Option<Vec<String>>,
    m_displayed_fields: Option<Vec<String>>,
    m_filterable_fields: Option<HashSet<String>>,
    m_sortable_fields: Option<HashSet<String>>,
    m_criteria: Option<Vec<MilliCriterion>>,
    m_stop_words: Option<BTreeSet<String>>,
    m_non_separator_tokens: Option<BTreeSet<String>>,
    m_separator_tokens: Option<BTreeSet<String>>,
    m_dictionary: Option<BTreeSet<String>>,
    m_distinct_field: Option<String>,
    m_synonyms: Option<BTreeMap<String, Vec<String>>>,
    m_authorize_typos: Option<bool>,
    m_min_word_len_two_typos: Option<u8>,
    m_min_word_len_one_typo: Option<u8>,
    m_exact_words: Option<BTreeSet<String>>,
    m_exact_attributes: Option<HashSet<String>>,
    m_max_values_per_facet: Option<usize>,
    m_sort_facet_values_by: Option<MilliOrderByMap>,
    m_pagination_max_total_hits: Option<usize>,
    m_proximity_precision: Option<usize>,
    m_search_cutoff: Option<u64>,
    // Unmanaged MilliIndexDocumentsConfig fields
    m_doc_words_prefix_threshold: Option<u32>,
    m_doc_max_prefix_length: Option<u32>,
    m_doc_words_positions_level_group_size: Option<u32>,
    m_doc_words_positions_min_level_size: Option<u32>,
}

lazy_static! {
    static ref options_field_names: HashSet<String> =
        if let Value::Object(map) = serde_json::to_value(PgmumboOptions::default()).unwrap() {
            map.keys().cloned().collect()
        } else {
            panic!()
        };
}

#[pg_guard]
pub unsafe extern "C" fn amoptions(
    reloptions: pg_sys::Datum,
    validate: bool,
) -> *mut pg_sys::bytea {
    // pgmumbo encodes options using CBOR as opposed to StdRdOptions.
    // Consequently, build_reloptions isn't used.
    const TEXT_ELMTYPE: Oid = pg_sys::TEXTOID;
    const TEXT_ELMLEN: raw::c_int = -1;
    const TEXT_ELMBYVAL: bool = false;
    const TEXT_ELMALIGN: raw::c_char = pg_sys::TYPALIGN_INT as raw::c_char;

    let mut elemsp: *mut pg_sys::Datum = ptr::null_mut();
    let mut nullsp: *mut bool = ptr::null_mut();
    let nelemsp: *mut raw::c_int = ptr::null_mut();
    pg_sys::deconstruct_array(
        reloptions.cast_mut_ptr(),
        TEXT_ELMTYPE,
        TEXT_ELMLEN,
        TEXT_ELMBYVAL,
        TEXT_ELMALIGN,
        &mut elemsp,
        &mut nullsp,
        nelemsp,
    );

    let nelem = *nelemsp as usize;
    let elems = slice::from_raw_parts(elemsp, nelem);
    let nulls = slice::from_raw_parts(nullsp, nelem);

    let bytes = match encode_options(validate, elems, nulls) {
        Ok(bytes) => bytes,
        Err(error) => {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
                error.to_string()
            );
        }
    };

    let encoded_size = mem::size_of::<[::std::os::raw::c_char; 4usize]>() + bytes.len();
    let encoded = pg_sys::palloc0(encoded_size) as *mut pg_sys::bytea;

    (*encoded).vl_len_ = (encoded_size as i32).to_ne_bytes().map(|x| x as i8);
    ptr::copy_nonoverlapping(
        bytes.as_ptr(),
        (*encoded).vl_dat.as_mut_ptr() as *mut u8,
        bytes.len(),
    );

    encoded
}

fn encode_options(
    validate: bool,
    elems: &[pg_sys::Datum],
    nulls: &[bool],
) -> Result<Vec<u8>, Error> {
    let pairs = zip(nulls, elems)
        .filter_map(|(null, elem)| {
            if *null {
                return None;
            }
            let (k_str, v_str) =
                unsafe { CStr::from_ptr(pg_sys::text_to_cstring(elem.cast_mut_ptr())) }
                    .to_str()
                    .unwrap_or_report()
                    .split_once('=')
                    .unwrap();
            let k = k_str.to_string();
            Some(serde_json::from_str::<serde_json::Value>(v_str).map(|v| (k, v)))
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(Error::OptionsParseValue)?;
    let options_map = serde_json::Map::from_iter(pairs);

    if validate {
        let difference: Vec<_> = options_map
            .keys()
            .filter(|key| !options_field_names.contains(*key))
            .cloned()
            .collect();
        if !difference.is_empty() {
            return Err(Error::OptionsInvalidKeys(difference));
        }
    }

    let options: PgmumboOptions = serde_json::from_value(serde_json::Value::Object(options_map))
        .map_err(Error::OptionsParseValue)?;

    let mut bytes = Vec::new();
    ciborium::into_writer(&options, &mut bytes).map_err(Error::OptionsEncode)?;

    Ok(bytes)
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
pub enum Error {
    #[error("Failed to split TID serialization, it could be malformed")]
    TidSplit,
    #[error("Failed to parse TID integer, it could be malformed")]
    TidParseInt(ParseIntError),
    #[error("Failed to parse index option value from JSON")]
    OptionsParseValue(serde_json::Error),
    #[error("Failed to encode index options to CBOR")]
    OptionsEncode(ciborium::ser::Error<io::Error>),
    #[error("Invalid index options keys")]
    OptionsInvalidKeys(Vec<String>),
    #[error("Could not resolve index space OID")]
    SpaceResolve(io::Error),
}
