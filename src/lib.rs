#![allow(clippy::too_many_arguments)]

use std::{
    borrow::BorrowMut,
    collections::{BTreeMap, BTreeSet, HashSet},
    ffi::{self, CStr},
    fs,
    io::{self, Cursor},
    iter,
    num::{NonZeroU32, ParseIntError},
    os::raw,
    path::{Path, PathBuf},
    ptr, slice,
    sync::LazyLock,
};

use itertools::{izip, Itertools};
use milli::{
    documents::{documents_batch_reader_from_objects, DocumentsBatchBuilder, DocumentsBatchReader},
    get_ranking_rules_for_placeholder_search, get_ranking_rules_for_query_graph_search, heed,
    query_graph::QueryGraph,
    ranking_rules::{
        BoxRankingRule, PlaceholderQuery, RankingRule, RankingRuleOutput, RankingRuleQueryTrait,
    },
    resolve_query_terms, resolve_universe, DefaultSearchLogger,
};
use pg_sys::{object_access_hook_type, panic::ErrorReportable, ObjectAccessDrop};
use pgrx::{datum, memcx, prelude::*, vardata_4b, varsize_4b, PgMemoryContexts, PgRelation};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_nested_with::serde_nested;
use thiserror::Error;

#[macro_use]
extern crate ouroboros;

pgrx::pg_module_magic!();

thread_local! {
    static EXISTING_OBJECT_ACCESS_HOOK: object_access_hook_type = None;
}

// struct Cache {
//     milli_index: milli::Index,
// }

// // Very segfaulty rn.. wait till later
// macro_rules! get_cache {
//     ($index_relation:expr) => {{
//         if unsafe { *$index_relation }.rd_amcache.is_null() {
//             info!("{PROGRAM_NAME}: opening up a new index");
//             // Open up an index
//             let index_path = lmdb_location(unsafe { *$index_relation }.rd_node).unwrap_or_report();
//             let mut lmdb_options = heed::EnvOpenOptions::new();
//             lmdb_options.map_size(INITIAL_LMDB_MMAP_SIZE); // TODO discover map size if possible
//             let cache = Cache {
//                 milli_index: milli::Index::new(lmdb_options, index_path).unwrap_or_report(),
//             };
//             // let cache_ptr = PgMemoryContexts::For(unsafe { *$index_relation }.rd_indexcxt)
//             // .f_and_drop_on_delete(cache) as *mut ffi::c_void;
//             let cache_ptr = Box::leak(Box::new(cache)) as *mut Cache as *mut ffi::c_void;
//             unsafe { (*$index_relation).rd_amcache = cache_ptr };
//             info!(
//                 "{PROGRAM_NAME}: stored: {:?}",
//                 unsafe { *$index_relation }.rd_amcache
//             );
//         }
//         unsafe { PgBox::from_pg((*$index_relation).rd_amcache as *mut Cache) }
//     }};
// }

macro_rules! get_index {
    ($index_relation:expr) => {{
        let index_path = lmdb_location($index_relation.rd_node).unwrap_or_report();
        let mut lmdb_options = heed::EnvOpenOptions::new();
        lmdb_options.map_size(INITIAL_LMDB_MMAP_SIZE);
        milli::Index::new(lmdb_options, index_path).unwrap_or_report()
    }};
}

#[allow(non_snake_case)]
#[pg_guard]
pub extern "C" fn _PG_init() {
    info!("{PROGRAM_NAME} loaded");
    #[pg_guard]
    extern "C" fn pgmumbo_oat_hook(
        access: pg_sys::ObjectAccessType::Type,
        class_id: pg_sys::Oid,
        object_id: pg_sys::Oid,
        sub_id: ffi::c_int,
        arg: *mut ffi::c_void,
    ) {
        EXISTING_OBJECT_ACCESS_HOOK.with(|existing_hook| {
            if let Some(existing_hook) = existing_hook {
                unsafe { existing_hook(access, class_id, object_id, sub_id, arg) };
            }
        });

        if access == pg_sys::ObjectAccessType::OAT_DROP
            && (class_id == pg_sys::RelationRelationId || class_id == pg_sys::IndexRelationId)
            && sub_id == 0
        {
            let mut oat_hook_context = PgMemoryContexts::new(PROGRAM_NAME);
            let mut old_owned_context = unsafe { oat_hook_context.set_as_current() };

            let dropflags = unsafe { *(arg as *mut ObjectAccessDrop) }.dropflags as u32;

            let index_relation = unsafe { PgRelation::open(object_id) };
            if !index_relation.is_index() {
                return;
            }

            // Check if pgmumbo is the one servicing this index
            let mut managed = false;
            memcx::current_context(|memcx| {
                let amhandler_name = unsafe {
                    PgBox::<ffi::c_char>::from_rust(pg_sys::pstrdup(c"pgmumbo_amhandler".as_ptr()))
                };
                let mut amhandler_name_parts = ptr::null_mut();
                unsafe {
                    pg_sys::SplitIdentifierString(
                        amhandler_name.as_ptr(),
                        '.' as raw::c_char,
                        &mut amhandler_name_parts,
                    )
                };
                let amhandler_name_parts: pgrx::list::List<*mut ffi::c_void> =
                    unsafe { pgrx::list::List::downcast_ptr_in_memcx(amhandler_name_parts, memcx) }
                        .unwrap();

                let mut amhandler_name_list = pgrx::list::List::<*mut ffi::c_void>::default();

                amhandler_name_parts
                    .iter()
                    .map(|part| unsafe { pg_sys::makeString(pg_sys::pstrdup(part.cast())) })
                    .for_each(|part| {
                        amhandler_name_list.unstable_push_in_context(part.cast(), memcx);
                    });

                let argtypes = [pg_sys::INTERNALOID];
                let amhandler = unsafe {
                    pg_sys::LookupFuncName(
                        amhandler_name_list.as_mut_ptr(),
                        argtypes.len() as raw::c_int,
                        argtypes.as_ptr(),
                        false,
                    )
                };

                managed = amhandler == index_relation.rd_amhandler;
            });
            if !managed {
                return;
            }

            let index_path = lmdb_location(index_relation.rd_node).unwrap_or_report();

            if (dropflags & pg_sys::PERFORM_DELETION_QUIETLY) == 0 {
                info!("{PROGRAM_NAME}: Removing index files in {index_path:?}");
            }

            fs::remove_dir_all(&index_path).unwrap_or_report();

            unsafe {
                old_owned_context.set_as_current();
                oat_hook_context.reset();
            }
        }
    }
    EXISTING_OBJECT_ACCESS_HOOK.with(|mut existing_hook| {
        *existing_hook.borrow_mut() =
            unsafe { ptr::addr_of!(pg_sys::object_access_hook).as_ref().unwrap() };
    });
    unsafe { pg_sys::object_access_hook = Some(pgmumbo_oat_hook) };
}

// TODO Create a planner hook to modify any plan containing the operator to contain an index scan
// TODO Create an executor start hook to throw an error if the operator is being used without an index scan

extension_sql!(
    // Language=SQL
    "CREATE ACCESS METHOD pgmumbo 
        TYPE INDEX
        HANDLER pgmumbo_amhandler;",
    name = "index_access_method",
    requires = [amhandler],
);

// Not sure if pgrx allows for custom return types without overriding SQL.
// Postgres expects the amhandler to return index_am_handler
#[pg_extern(sql = "
    CREATE OR REPLACE FUNCTION pgmumbo_amhandler(internal)
        RETURNS index_am_handler
        STRICT
        LANGUAGE c
        AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';")]
fn amhandler(_fcinfo: pg_sys::FunctionCallInfo) -> PgBox<pg_sys::IndexAmRoutine> {
    let mut amroutine =
        unsafe { PgBox::<pg_sys::IndexAmRoutine>::alloc_node(pg_sys::NodeTag::T_IndexAmRoutine) };

    amroutine.amstrategies = 1; // Enumerated in operator classes
    amroutine.amsupport = 0;
    amroutine.amoptsprocnum = 0;
    amroutine.amcanorder = false; // The index cannot return TIDs in a B-tree compatible manner
    amroutine.amcanorderbyop = true; // Support ORDER BY clause on supported operators
    amroutine.amcanbackward = false; // It's not guaranteed the total number of hits for a given query is known
    amroutine.amcanunique = false; // It's not particularly useful to use milli to perform uniqueness checks on columns
    amroutine.amcanmulticol = true; // Support multiple index columns
    amroutine.amoptionalkey = true; // Support index scans omitting the first index column
    amroutine.amsearcharray = true;
    amroutine.amstorage = false;
    amroutine.amclusterable = false;
    amroutine.ampredlocks = false;
    amroutine.amcanparallel = false;
    amroutine.amcaninclude = false;
    amroutine.amusemaintenanceworkmem = false;
    amroutine.amparallelvacuumoptions = pg_sys::VACUUM_OPTION_NO_PARALLEL as u8;
    amroutine.amkeytype = pg_sys::Oid::INVALID;

    amroutine.ambuild = Some(ambuild);
    amroutine.ambuildempty = Some(ambuildempty);
    amroutine.aminsert = Some(aminsert);
    amroutine.ambulkdelete = Some(ambulkdelete);
    amroutine.amvacuumcleanup = Some(amvacuumcleanup);
    amroutine.amcanreturn = None;
    amroutine.amcostestimate = Some(amcostestimate);
    amroutine.amoptions = Some(amoptions);
    amroutine.amproperty = None;
    amroutine.ambuildphasename = None;
    amroutine.amvalidate = Some(amvalidate);
    amroutine.amadjustmembers = None;
    amroutine.ambeginscan = Some(ambeginscan);
    amroutine.amrescan = None; // Some(amrescan);
    amroutine.amgettuple = Some(amgettuple);
    amroutine.amgetbitmap = Some(amgetbitmap);
    amroutine.amendscan = Some(amendscan);
    amroutine.ammarkpos = None;
    amroutine.amrestrpos = None;

    amroutine.amestimateparallelscan = None;
    amroutine.aminitparallelscan = None;
    amroutine.amparallelrescan = None;

    amroutine.into_pg_boxed()
}

const PROGRAM_NAME: &str = "pgmumbo";

const INITIAL_LMDB_MMAP_SIZE: usize = 64 * 1024 * 1024 * 1024; // Initialize LMDB with 64 GB memory map

const TID_PRIMARY_KEY: &str = "@pgmumbo_tid";

// Called on CREATE INDEX.
// I'm pretty sure REINDEX just creates a new index relation with a different Oid,
// so Postgres should handle that for us. When the original index is DROP'ed though can we hook into that?
#[pg_guard]
pub extern "C" fn ambuild(
    heap_relation: pg_sys::Relation,
    index_relation: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
) -> *mut pg_sys::IndexBuildResult {
    info!("{PROGRAM_NAME} ambuild");
    let heap_relation = unsafe { PgRelation::from_pg(heap_relation) };
    let index_relation = unsafe { PgRelation::from_pg(index_relation) };

    assert!(index_relation.is_index());

    // Heed's Drop for LMDB RoTxn and RwTxn should call abort() on an open transaction.

    let options: Options = index_relation.rd_options.try_into().unwrap_or_report();

    let index_path = lmdb_location(index_relation.rd_node).unwrap_or_report();
    info!("Building a new index at: {index_path:?}");
    if index_path.exists() {
        warning!("Existing files in index location that should not be there, deleting them...");
        fs::remove_dir_all(&index_path).unwrap_or_report();
    }
    fs::create_dir_all(&index_path).unwrap_or_report();

    let milli_index = get_index!(index_relation);

    let mut build_state = BuildState {
        owned_context: PgMemoryContexts::new(PROGRAM_NAME),
        batch_builder: DocumentsBatchBuilder::new(Vec::new()),
    };

    unsafe {
        pg_sys::IndexBuildHeapScan(
            heap_relation.as_ptr(),
            index_relation.as_ptr(),
            index_info,
            Some(build_callback),
            &mut build_state,
        )
    }

    let documents = build_state.batch_builder.into_inner().unwrap_or_report();
    let mut wtxn = milli_index.write_txn().unwrap_or_report();

    let milli_idx_config: milli::update::IndexerConfig = (&options).into();
    let milli_idx_doc_config: milli::update::IndexDocumentsConfig = (&options).into();
    let mut milli_settings =
        milli::update::Settings::new(&mut wtxn, &milli_index, &milli_idx_config);
    milli_settings.set_primary_key(TID_PRIMARY_KEY.to_string());
    macro_rules! set_ms_option {
        ($option:ident, $method:ident) => {
            if let Some(x) = options.$option {
                milli_settings.$method(x);
            }
        };
    }
    set_ms_option!(ms_searchable_fields, set_searchable_fields);
    set_ms_option!(ms_displayed_fields, set_displayed_fields);
    set_ms_option!(ms_filterable_fields, set_filterable_fields);
    set_ms_option!(ms_sortable_fields, set_sortable_fields);
    set_ms_option!(ms_criteria, set_criteria);
    set_ms_option!(ms_stop_words, set_stop_words);
    set_ms_option!(ms_non_separator_tokens, set_non_separator_tokens);
    set_ms_option!(ms_separator_tokens, set_separator_tokens);
    set_ms_option!(ms_dictionary, set_dictionary);
    set_ms_option!(ms_distinct_field, set_distinct_field);
    set_ms_option!(ms_synonyms, set_synonyms);
    set_ms_option!(ms_authorize_typos, set_autorize_typos);
    set_ms_option!(ms_min_word_len_two_typos, set_min_word_len_two_typos);
    set_ms_option!(ms_min_word_len_one_typo, set_min_word_len_one_typo);
    set_ms_option!(ms_exact_words, set_exact_words);
    set_ms_option!(ms_exact_attributes, set_exact_attributes);
    set_ms_option!(ms_max_values_per_facet, set_max_values_per_facet);
    set_ms_option!(ms_sort_facet_values_by, set_sort_facet_values_by);
    set_ms_option!(ms_pagination_max_total_hits, set_pagination_max_total_hits);
    set_ms_option!(ms_proximity_precision, set_proximity_precision);
    set_ms_option!(ms_search_cutoff, set_search_cutoff);
    milli_settings.execute(|_| {}, || false).unwrap();

    let mut indexer = milli::update::IndexDocuments::new(
        &mut wtxn,
        &milli_index,
        &milli_idx_config,
        milli_idx_doc_config,
        |_| {},
        || false,
    )
    .unwrap_or_report();

    (indexer, _) = indexer
        .add_documents(DocumentsBatchReader::from_reader(Cursor::new(documents)).unwrap_or_report())
        .unwrap_or_report();
    let index_result = indexer.execute().unwrap_or_report();

    wtxn.commit().unwrap_or_report();

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
extern "C" fn build_callback(
    index: pg_sys::Relation,
    tid: pg_sys::ItemPointer,
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    _tuple_is_alive: bool,
    state: *mut raw::c_void,
) {
    let build_state = unsafe { (state as *mut BuildState).as_mut() }.unwrap();
    let mut old_owned_context = unsafe { build_state.owned_context.set_as_current() };

    let index_relation = unsafe { PgRelation::from_pg(index) };

    let desc = index_relation.tuple_desc();
    let natts = desc.len();

    let isnull = unsafe { slice::from_raw_parts(isnull, natts) };
    let values = unsafe { slice::from_raw_parts(values, natts) };
    let attrs = unsafe { desc.attrs.as_slice(natts) };

    let document = form_document(tid, isnull, values, attrs);

    info!("{PROGRAM_NAME} heapscan: {document:?}");

    build_state
        .batch_builder
        .append_json_object(&document)
        .unwrap_or_report();

    unsafe {
        old_owned_context.set_as_current();
        build_state.owned_context.reset();
    }
}

fn tid_to_name(ptr: pg_sys::ItemPointer) -> String {
    format!(
        "{}-{}_{}",
        unsafe { *ptr }.ip_blkid.bi_hi,
        unsafe { *ptr }.ip_blkid.bi_lo,
        unsafe { *ptr }.ip_posid
    )
}

fn name_to_tid(s: &str) -> Result<pg_sys::ItemPointerData, Error> {
    let (blk_str, pos_str) = s.split_once('_').ok_or(Error::TidSplit)?;
    let (blk_hi_str, blk_lo_str) = blk_str.split_once('-').ok_or(Error::TidSplit)?;

    Ok(pg_sys::ItemPointerData {
        ip_blkid: pg_sys::BlockIdData {
            bi_hi: blk_hi_str.parse().map_err(Error::TidParseInt)?,
            bi_lo: blk_lo_str.parse().map_err(Error::TidParseInt)?,
        },
        ip_posid: pos_str.parse().map_err(Error::TidParseInt)?,
    })
}

fn form_document(
    tid: pg_sys::ItemPointer,
    isnull: &[bool],
    values: &[pg_sys::Datum],
    attrs: &[pg_sys::FormData_pg_attribute],
) -> serde_json::Map<String, serde_json::Value> {
    serde_json::Map::from_iter(
        // Managed primary key; 1:1 mapping with TID
        iter::once((
            TID_PRIMARY_KEY.to_string(),
            serde_json::Value::String(tid_to_name(tid)),
        ))
        // Then, convert INCLUDE clause tuple attrs and values to KV pairs
        .chain(
            izip!(isnull, attrs, values).filter_map(|(isnull, attr, value)| {
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

fn spc_location(spc: pg_sys::Oid) -> Result<PathBuf, Error> {
    let my_database_spc = unsafe { pg_sys::MyDatabaseTableSpace };
    let mut spc = spc;

    let pgdata = Path::new(
        unsafe { CStr::from_ptr(pg_sys::DataDir) }
            .to_str()
            .unwrap_or_report(),
    );

    if spc == pg_sys::Oid::INVALID {
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
    info!("{PROGRAM_NAME} ambuildempty");
}

#[pg_guard]
pub extern "C" fn aminsert(
    index_relation: pg_sys::Relation,
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    heap_tid: pg_sys::ItemPointer,
    _heap_relation: pg_sys::Relation,
    _check_unique: pg_sys::IndexUniqueCheck::Type,
    _index_unchanged: bool,
    _index_info: *mut pg_sys::IndexInfo,
) -> bool {
    let mut insert_context = PgMemoryContexts::new(PROGRAM_NAME);
    let mut old_owned_context = unsafe { insert_context.set_as_current() };

    let index_relation = unsafe { PgRelation::from_pg(index_relation) };
    let options: Options = index_relation.rd_options.try_into().unwrap_or_report();

    let desc = unsafe { index_relation.rd_att.as_ref() }.unwrap();
    let natts = desc.natts as usize;

    let isnull = unsafe { slice::from_raw_parts(isnull, natts) };
    let values = unsafe { slice::from_raw_parts(values, natts) };
    let attrs = unsafe { desc.attrs.as_slice(natts) };

    let document = form_document(heap_tid, isnull, values, attrs);
    info!("{PROGRAM_NAME} aminsert: {document:?}");
    let document_reader = documents_batch_reader_from_objects(iter::once(document));

    let milli_index = get_index!(index_relation);

    // Insert the document into the index
    let mut wtxn = milli_index.write_txn().unwrap_or_report();
    let milli_idx_config: milli::update::IndexerConfig = (&options).into();
    let milli_idx_doc_config: milli::update::IndexDocumentsConfig = (&options).into();
    let indexer = milli::update::IndexDocuments::new(
        &mut wtxn,
        &milli_index,
        &milli_idx_config,
        milli_idx_doc_config,
        |_| {},
        || false,
    )
    .unwrap_or_report();
    let (indexer, user_error) = indexer.add_documents(document_reader).unwrap_or_report();
    user_error.unwrap_or_report();
    indexer.execute().unwrap_or_report();
    wtxn.commit().unwrap_or_report();

    unsafe {
        old_owned_context.set_as_current();
        insert_context.reset();
    }

    // pgmumbo does not perform uniqueness checks (yet)
    false
}

#[pg_guard]
pub extern "C" fn ambulkdelete(
    info: *mut pg_sys::IndexVacuumInfo,
    stats: *mut pg_sys::IndexBulkDeleteResult,
    callback: pg_sys::IndexBulkDeleteCallback,
    callback_state: *mut raw::c_void,
) -> *mut pg_sys::IndexBulkDeleteResult {
    let callback = callback.unwrap();
    let index_relation = unsafe { (*info).index };
    let index_relation = unsafe { PgRelation::from_pg(index_relation) };
    let options: Options = index_relation.rd_options.try_into().unwrap_or_report();

    let stats = if stats.is_null() {
        unsafe { PgBox::<pg_sys::IndexBulkDeleteResult>::alloc0() }.into_pg_boxed()
    } else {
        unsafe { PgBox::from_pg(stats) }
    };

    let milli_index = get_index!(index_relation);

    // Gather external document IDs for deletion
    let mut to_delete: Vec<String> = Vec::new();
    let mut wtxn = milli_index.write_txn().unwrap_or_report();
    let milli_index_fields = milli_index.fields_ids_map(&wtxn).unwrap_or_report();
    let milli_primary_key =
        milli::documents::PrimaryKey::new(TID_PRIMARY_KEY, &milli_index_fields).unwrap();
    for document in milli_index.all_documents(&wtxn).unwrap_or_report() {
        let (_, kv_reader) = document.unwrap_or_report();
        let pk_value = milli_primary_key
            .document_id(&kv_reader, &milli_index_fields)
            .unwrap()
            .map_err(|_| todo!())
            .unwrap();
        let tid = &mut name_to_tid(&pk_value).unwrap_or_report();
        let should_delete = unsafe { callback(tid, callback_state) };
        if should_delete {
            to_delete.push(pk_value);
        }
    }

    info!("pg ambulkdelete: deleting {} documents", to_delete.len());

    // Delete such documents
    let milli_idx_config: milli::update::IndexerConfig = (&options).into();
    let milli_idx_doc_config: milli::update::IndexDocumentsConfig = (&options).into();
    let indexer = milli::update::IndexDocuments::new(
        &mut wtxn,
        &milli_index,
        &milli_idx_config,
        milli_idx_doc_config,
        |_| (),
        || false,
    )
    .unwrap();
    let (indexer, user_error) = indexer.remove_documents(to_delete).unwrap_or_report();
    user_error.unwrap_or_report();
    indexer.execute().unwrap_or_report();
    wtxn.commit().unwrap_or_report();

    stats.into_pg()
}

#[pg_guard]
pub extern "C" fn amvacuumcleanup(
    _info: *mut pg_sys::IndexVacuumInfo,
    stats: *mut pg_sys::IndexBulkDeleteResult,
) -> *mut pg_sys::IndexBulkDeleteResult {
    info!("{PROGRAM_NAME} amvacuumcleanup");
    stats
}

#[allow(clippy::too_many_arguments)]
#[pg_guard(immutable, parallel_safe)]
pub extern "C" fn amcostestimate(
    _root: *mut pg_sys::PlannerInfo,
    _path: *mut pg_sys::IndexPath,
    _loop_count: f64,
    index_startup_cost: *mut pg_sys::Cost,
    index_total_cost: *mut pg_sys::Cost,
    index_selectivity: *mut pg_sys::Selectivity,
    index_correlation: *mut f64,
    index_pages: *mut f64,
) {
    info!("{PROGRAM_NAME} amcostestimate (phony)");
    // TODO this is kinda bad... i need to justify these numbers
    unsafe { *index_startup_cost = 0.0 };
    unsafe { *index_total_cost = -pg_sys::random_page_cost };
    unsafe { *index_selectivity = 0.5 };
    unsafe { *index_correlation = 1.0 };
    unsafe { *index_pages = 1.0 };
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "milli::CompressionType")]
enum MilliCompressionTypeDef {
    None,
    SnappyPre05,
    Zlib,
    Lz4,
    Zstd,
    Snappy,
}

#[serde_nested]
#[derive(Default, Serialize, Deserialize)]
struct Options {
    // Unmanaged milli::update::Settings fields
    ms_searchable_fields: Option<Vec<String>>,
    ms_displayed_fields: Option<Vec<String>>,
    ms_filterable_fields: Option<HashSet<String>>,
    ms_sortable_fields: Option<HashSet<String>>,
    ms_criteria: Option<Vec<milli::Criterion>>,
    ms_stop_words: Option<BTreeSet<String>>,
    ms_non_separator_tokens: Option<BTreeSet<String>>,
    ms_separator_tokens: Option<BTreeSet<String>>,
    ms_dictionary: Option<BTreeSet<String>>,
    ms_distinct_field: Option<String>,
    ms_synonyms: Option<BTreeMap<String, Vec<String>>>,
    ms_authorize_typos: Option<bool>,
    ms_min_word_len_two_typos: Option<u8>,
    ms_min_word_len_one_typo: Option<u8>,
    ms_exact_words: Option<BTreeSet<String>>,
    ms_exact_attributes: Option<HashSet<String>>,
    ms_max_values_per_facet: Option<usize>,
    ms_sort_facet_values_by: Option<milli::order_by_map::OrderByMap>,
    ms_pagination_max_total_hits: Option<usize>,
    ms_proximity_precision: Option<milli::proximity::ProximityPrecision>,
    ms_search_cutoff: Option<u64>,
    // Unmanaged milli::update::IndexerConfig fields
    mic_log_every_n: Option<usize>,
    mic_max_nb_chunks: Option<usize>,
    mic_documents_chunk_size: Option<usize>,
    mic_max_memory: Option<usize>,
    #[serde(default)]
    #[serde_nested(
        sub = "milli::CompressionType",
        serde(with = "MilliCompressionTypeDef")
    )]
    mic_chunk_compression_type: Option<milli::CompressionType>, // TODO: open up crate features flags for compression
    mic_chunk_compression_level: Option<u32>,
    mic_max_positions_per_attributes: Option<u32>,
    mic_skip_index_budget: Option<bool>,
    // Unmanaged milli::update::IndexDocumentsConfig fields
    midc_doc_words_prefix_threshold: Option<u32>,
    midc_doc_max_prefix_length: Option<usize>,
    midc_doc_words_positions_level_group_size: Option<NonZeroU32>,
    midc_doc_words_positions_min_level_size: Option<NonZeroU32>,
}

#[pg_guard]
pub extern "C" fn amoptions(reloptions: pg_sys::Datum, validate: bool) -> *mut pg_sys::bytea {
    info!("{PROGRAM_NAME} amoptions, validate {validate}");

    // pgmumbo encodes options using CBOR as opposed to StdRdOptions.
    // Consequently, build_reloptions isn't used.

    let array: datum::Array<&str> = unsafe {
        datum::Array::from_polymorphic_datum(reloptions, reloptions.is_null(), pg_sys::TEXTOID)
    }
    .unwrap();
    let elems = array.iter().flatten();

    let bytes = match encode_options(validate, elems) {
        Ok(bytes) => bytes,
        Err(error) => {
            ereport!(
                ERROR,
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
                error.to_string()
            );
        }
    };

    bytes.into_datum().unwrap().cast_mut_ptr()
}

static OPTIONS_FIELD_NAMES: LazyLock<HashSet<String>> = LazyLock::new(|| {
    if let Value::Object(map) = serde_json::to_value(Options::default()).unwrap() {
        map.keys().cloned().collect()
    } else {
        panic!()
    }
});

fn encode_options<'a>(
    validate: bool,
    elems: impl Iterator<Item = &'a str>,
) -> Result<Vec<u8>, Error> {
    let pairs = elems
        .map(|elem| {
            let (k_str, v_str) = elem.split_once('=').unwrap();
            let k = k_str.to_string();
            serde_json::from_str::<serde_json::Value>(v_str).map(|v| (k, v))
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(Error::OptionsParseValue)?;
    let options_map = serde_json::Map::from_iter(pairs);

    if validate {
        let difference: Vec<_> = options_map
            .keys()
            .filter(|key| !OPTIONS_FIELD_NAMES.contains(*key))
            .cloned()
            .collect();
        if !difference.is_empty() {
            return Err(Error::OptionsInvalidKeys(difference));
        }
    }

    let options: Options = serde_json::from_value(serde_json::Value::Object(options_map))
        .map_err(Error::OptionsParseValue)?;

    let mut bytes = Vec::new();
    ciborium::into_writer(&options, &mut bytes).map_err(Error::OptionsEncode)?;

    Ok(bytes)
}

impl TryFrom<*mut pg_sys::varlena> for Options {
    type Error = Error;

    fn try_from(options_data: *mut pg_sys::varlena) -> Result<Self, Self::Error> {
        if options_data.is_null() {
            return Ok(Self::default());
        }

        let options_data = unsafe {
            slice::from_raw_parts(
                vardata_4b(options_data) as *const u8,
                varsize_4b(options_data),
            )
        };

        ciborium::from_reader(options_data).map_err(Error::OptionsDecode)
    }
}

impl From<&Options> for milli::update::IndexerConfig {
    fn from(val: &Options) -> Self {
        milli::update::IndexerConfig {
            log_every_n: val.mic_log_every_n,
            max_nb_chunks: val.mic_max_nb_chunks,
            documents_chunk_size: val.mic_documents_chunk_size,
            max_memory: val.mic_max_memory,
            chunk_compression_type: val.mic_chunk_compression_type.unwrap_or_default(),
            chunk_compression_level: val.mic_chunk_compression_level,
            thread_pool: None,
            max_positions_per_attributes: val.mic_max_positions_per_attributes,
            skip_index_budget: val.mic_skip_index_budget.unwrap_or(false),
        }
    }
}

impl From<&Options> for milli::update::IndexDocumentsConfig {
    fn from(val: &Options) -> Self {
        milli::update::IndexDocumentsConfig {
            words_prefix_threshold: val.midc_doc_words_prefix_threshold,
            max_prefix_length: val.midc_doc_max_prefix_length,
            words_positions_level_group_size: val.midc_doc_words_positions_level_group_size,
            words_positions_min_level_size: val.midc_doc_words_positions_min_level_size,
            update_method: milli::update::IndexDocumentsMethod::ReplaceDocuments,
            autogenerate_docids: false,
        }
    }
}

#[pg_guard]
pub extern "C" fn amvalidate(_opclassoid: pg_sys::Oid) -> bool {
    info!("{PROGRAM_NAME} amvalidate");
    true
}

#[pg_extern(sql = "
    CREATE FUNCTION pgmumboquery_cmpfunc(text, PgmumboQuery)
        RETURNS boolean
        STRICT
        LANGUAGE c
        AS '@MODULE_PATHNAME@', '@FUNCTION_NAME@';")]
fn pgmumboquery_cmpfunc(
    _fcinfo: pg_sys::FunctionCallInfo,
    _anyelement: PgBox<&str>,
    _query: PgBox<PgmumboQuery>,
) -> bool {
    // It doesnt make sense to compare against things that aren't column attribute numbers
    error!("{PROGRAM_NAME} cmpfunc not valid on unindexed column");
}

#[pg_extern(immutable, parallel_safe)]
fn pgmumboquery_restrict(
    _planner_info: pgrx::Internal,
    _operator_oid: pg_sys::Oid,
    _args: pgrx::Internal,
    _var_relid: i32,
) -> f64 {
    0.0
}

extension_sql!(
    "CREATE OPERATOR pg_catalog.@? (
        FUNCTION = pgmumboquery_cmpfunc,
        LEFTARG  = text,
        RIGHTARG = pgmumboquery,
        RESTRICT = pgmumboquery_restrict
    );",
    name = "operator_pgmumboquery",
    requires = [pgmumboquery_cmpfunc, PgmumboQuery],
);

extension_sql!(
    "CREATE OPERATOR CLASS pgmumbo_ops_pgmumboquery
        DEFAULT FOR TYPE text
        USING pgmumbo AS
            OPERATOR 1 @?(text, pgmumboquery) FOR ORDER BY pgmumbo;",
    name = "operator_class_pgmumboquery",
    requires = ["index_access_method", "operator_pgmumboquery", PgmumboQuery],
);

enum MilliQuery<'ctx> {
    Graph(
        milli::query_graph::QueryGraph,
        Vec<BoxRankingRule<'ctx, QueryGraph>>,
    ),
    Placeholder(
        milli::ranking_rules::PlaceholderQuery,
        Vec<BoxRankingRule<'ctx, PlaceholderQuery>>,
    ),
}

#[self_referencing]
struct ScanOpaque {
    // Search context bootstrapping
    milli_index: milli::Index,
    #[borrows(milli_index)]
    #[covariant]
    lmdb_rtxn: heed::RoTxn<'this>,
    // Search context
    #[borrows(milli_index, lmdb_rtxn)]
    #[not_covariant]
    milli_context: milli::SearchContext<'this>,
    // Keep a universe for usage in amgetbitmap
    universe: RoaringBitmap,
    // Some useful state for scanning produced during universe contruction
    #[borrows(lmdb_rtxn)]
    #[not_covariant]
    milli_query: MilliQuery<'this>,
    terms_matching_strategy: milli::TermsMatchingStrategy,
    scoring_strategy: milli::score_details::ScoringStrategy,
    // Batch up stashed search results in a Vec
    batch_size: usize,
    search_stash: Vec<milli::DocumentId>,
    search_idx: Option<usize>,
    // Storage for ammarkpos and amrestrpos
    marked_stash: Vec<milli::DocumentId>,
    marked_idx: Option<usize>,
}

const DEFAULT_SCAN_BATCH_SIZE: usize = 64; // Picked arbitrarily for now

#[pg_guard]
pub extern "C" fn ambeginscan(
    index_relation: pg_sys::Relation,
    nkeys: raw::c_int,
    norderbys: raw::c_int,
) -> pg_sys::IndexScanDesc {
    info!("{PROGRAM_NAME} ambeginscan");
    let mut scan = unsafe {
        PgBox::<pg_sys::IndexScanDescData>::from_rust(pg_sys::RelationGetIndexScan(
            index_relation,
            nkeys,
            norderbys,
        ))
    };
    let index_relation = unsafe { PgRelation::from_pg(index_relation) };

    // Initialize opaque
    let opaque_builder = ScanOpaqueBuilder {
        milli_index: get_index!(index_relation),
        // Open an LMDB read transaction
        lmdb_rtxn_builder: |milli_index| milli_index.read_txn().unwrap_or_report(),
        // Open up a search context
        milli_context_builder: |milli_index, lmdb_rtxn| {
            milli::SearchContext::new(milli_index, lmdb_rtxn).unwrap_or_report()
        },
        // Empty universe, to be filled by amrescan
        universe: RoaringBitmap::new(),
        milli_query_builder: |_| MilliQuery::Placeholder(PlaceholderQuery, Vec::new()),
        terms_matching_strategy: milli::TermsMatchingStrategy::default(),
        scoring_strategy: milli::score_details::ScoringStrategy::default(),
        batch_size: DEFAULT_SCAN_BATCH_SIZE,
        search_stash: Vec::default(),
        search_idx: None,
        marked_stash: Vec::default(),
        marked_idx: None,
    };
    // Load opaque into the scan description, free during amendscan
    let opaque = opaque_builder.build();
    scan.opaque = Box::into_raw(Box::new(opaque)) as *mut ffi::c_void;
    scan.into_pg()
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "milli::TermsMatchingStrategy")]
enum MilliTermsMatchingStrategyDef {
    Last,
    All,
    Frequency,
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "milli::score_details::ScoringStrategy")]
enum MilliScoringStrategyDef {
    Skip,
    Detailed,
}

#[derive(Debug, Clone, Eq, PartialEq, Default, Serialize, Deserialize, PostgresType)]
struct PgmumboQuery {
    query: Option<String>,
    filter: Option<String>,
    #[serde(default, with = "MilliTermsMatchingStrategyDef")]
    terms_matching_strategy: milli::TermsMatchingStrategy,
    #[serde(default, with = "MilliScoringStrategyDef")]
    scoring_strategy: milli::score_details::ScoringStrategy,
}

#[pg_guard]
pub extern "C" fn amrescan(
    scan: pg_sys::IndexScanDesc,
    keys: pg_sys::ScanKey,
    nkeys: raw::c_int,
    orderbys: pg_sys::ScanKey,
    norderbys: raw::c_int,
) {
    info!("{PROGRAM_NAME} amrescan");
    let scan = unsafe { PgBox::from_pg(scan) };
    let mut opaque = unsafe { PgBox::from_pg(scan.opaque as *mut ScanOpaque) };
    let index_relation = unsafe { PgRelation::from_pg(scan.indexRelation) };
    let desc = unsafe { index_relation.rd_att.as_ref() }.unwrap();
    let natts = desc.natts as usize;
    let attrs = unsafe { desc.attrs.as_slice(natts) };

    // Need to restart scan with new values; limit universe and apply settings
    let keys = if keys.is_null() {
        &[]
    } else {
        unsafe { slice::from_raw_parts(keys, nkeys as usize) }
    };
    let orderbys = if orderbys.is_null() {
        &[]
    } else {
        unsafe { slice::from_raw_parts(orderbys, norderbys as usize) }
    };
    info!("{keys:?},{orderbys:?}");

    // Gather queries on columns
    let mut queries: Vec<(PgmumboQuery, HashSet<&str>)> = keys
        .iter()
        .chain(orderbys)
        .map(|scan_key| {
            let att_idx = (scan_key.sk_attno - 1) as usize;
            let att_name = unsafe { CStr::from_ptr(attrs[att_idx].attname.data.as_ptr()) }
                .to_str()
                .unwrap_or_report();
            let argument = unsafe {
                PgmumboQuery::from_datum(scan_key.sk_argument, scan_key.sk_argument.is_null())
            }
            .unwrap();
            (argument, att_name)
        })
        .chunk_by(|x| x.0.clone())
        .into_iter()
        .map(|(k, v)| (k, v.map(|(_, v)| v).collect::<HashSet<&str>>()))
        .collect();
    // Produce the final PgmumboQuery
    if queries.len() != 1 {
        error!("pgmumbo does not currently support dissimilar queries on multiple columns")
    }
    let (query, document_keys) = queries.pop().unwrap();
    let document_keys: Vec<String> = document_keys.into_iter().map(|x| x.to_string()).collect();

    opaque.with_mut(|opaque| {
        *opaque.universe = opaque.milli_index.documents_ids(opaque.lmdb_rtxn).unwrap();
        opaque
            .milli_context
            .attributes_to_search_on(&document_keys)
            .unwrap();
        let (query_terms, _) = resolve_query_terms(
            opaque.milli_context,
            query.query.as_deref(),
            opaque.universe,
            None,
            None,
        )
        .unwrap();
        if let Some(query_terms) = query_terms {
            let (graph, _) =
                QueryGraph::from_query(opaque.milli_context, &query_terms).unwrap_or_report();
            let ranking_rules = get_ranking_rules_for_query_graph_search(
                opaque.milli_context,
                &None, // TODO setup AscDesc
                milli::GeoSortStrategy::default(),
                query.terms_matching_strategy,
            )
            .unwrap_or_report();
            *opaque.universe &= resolve_universe(
                opaque.milli_context,
                opaque.universe,
                &graph,
                query.terms_matching_strategy,
                &mut milli::DefaultSearchLogger,
            )
            .unwrap_or_report();
            *opaque.milli_query = MilliQuery::Graph(graph, ranking_rules);
        } else {
            let ranking_rules = get_ranking_rules_for_placeholder_search(
                opaque.milli_context,
                &None,
                milli::GeoSortStrategy::default(),
            )
            .unwrap_or_report();
            *opaque.milli_query = MilliQuery::Placeholder(PlaceholderQuery, ranking_rules);
        }
        *opaque.terms_matching_strategy = query.terms_matching_strategy;
        *opaque.scoring_strategy = query.scoring_strategy;
    });
}

#[pg_guard]
pub extern "C" fn amgettuple(
    scan: pg_sys::IndexScanDesc,
    direction: pg_sys::ScanDirection::Type,
) -> bool {
    info!("{PROGRAM_NAME} amgettuple");
    let mut scan = unsafe { PgBox::from_pg(scan) };
    let mut opaque = unsafe { PgBox::from_pg(scan.opaque as *mut ScanOpaque) };

    let documents_remain = false;

    // TODO: check specifics of lifetime aliasing issue

    // opaque.with_mut(|opaque| {
    //     // Set up TID lookup tools
    //     let milli_index_fields = opaque
    //         .milli_index
    //         .fields_ids_map(opaque.lmdb_rtxn)
    //         .unwrap_or_report();
    //     let milli_primary_key =
    //         milli::documents::PrimaryKey::new(TID_PRIMARY_KEY, &milli_index_fields).unwrap();
    //     macro_rules! get_tid {
    //         ($document_id:expr) => {{
    //             let (_, document) = opaque
    //                 .milli_index
    //                 .documents(opaque.lmdb_rtxn, iter::once($document_id))
    //                 .unwrap()
    //                 .into_iter()
    //                 .next()
    //                 .unwrap();
    //             let pk_value = milli_primary_key
    //                 .document_id(&document, &milli_index_fields)
    //                 .unwrap_or_report()
    //                 .map_err(|_| todo!())
    //                 .unwrap();
    //             name_to_tid(&pk_value).unwrap_or_report()
    //         }};
    //     }

    //     // Check if we have a stash already...
    //     if let Some(search_idx) = opaque.search_idx {
    //         match direction {
    //             pg_sys::ScanDirection::ForwardScanDirection => {}
    //             pg_sys::ScanDirection::NoMovementScanDirection => {
    //                 // We know that the stash is already initialized, so let's just grab it
    //                 let stash_idx = *search_idx % *opaque.batch_size;
    //                 let document_id = opaque.search_stash[stash_idx];
    //                 scan.xs_heaptid = get_tid!(document_id);
    //             }
    //             _ => error!("Unsupported scan direction: {direction}"),
    //         };
    //     } else {
    //         // if not, then we'll have to initialize the stash
    //         let bucket_result = match opaque.milli_query {
    //             MilliQuery::Graph(query, ranking_rules) => {
    //                 milli::search::new::bucket_sort::bucket_sort(
    //                     opaque.milli_context,
    //                     ranking_rules,
    //                     query,
    //                     None,
    //                     opaque.universe,
    //                     0,
    //                     0,
    //                     *opaque.scoring_strategy,
    //                     &mut DefaultSearchLogger,
    //                     milli::TimeBudget::max(),
    //                     None,
    //                 )
    //             }
    //             MilliQuery::Placeholder(query, ranking_rules) => {
    //                 milli::search::new::bucket_sort::bucket_sort(
    //                     opaque.milli_context,
    //                     ranking_rules,
    //                     query,
    //                     None,
    //                     opaque.universe,
    //                     0,
    //                     0,
    //                     *opaque.scoring_strategy,
    //                     &mut DefaultSearchLogger,
    //                     milli::TimeBudget::max(),
    //                     None,
    //                 )
    //             }
    //         }
    //         .unwrap_or_report();
    //         *opaque.search_stash = bucket_result.docids;
    //     }
    // });

    documents_remain
}

#[pg_guard]
pub extern "C" fn amgetbitmap(scan: pg_sys::IndexScanDesc, tbm: *mut pg_sys::TIDBitmap) -> i64 {
    info!("{PROGRAM_NAME} amgetbitmap");
    let tbm = unsafe { PgBox::from_pg(tbm) };
    let scan = unsafe { PgBox::from_pg(scan) };
    let mut opaque = unsafe { PgBox::from_pg(scan.opaque as *mut ScanOpaque) };

    // Convert the universe into a Vec of TIDs
    let mut tids = Vec::new();
    opaque.with_mut(|opaque| {
        let universe = opaque.universe.clone();
        let rtxn = opaque.lmdb_rtxn;
        let milli_index = opaque.milli_index;
        let milli_index_fields = milli_index.fields_ids_map(rtxn).unwrap_or_report();
        let milli_primary_key =
            milli::documents::PrimaryKey::new(TID_PRIMARY_KEY, &milli_index_fields).unwrap();
        for (_, kv_reader) in milli_index
            .documents(rtxn, universe.into_iter())
            .unwrap_or_report()
        {
            let pk_value = milli_primary_key
                .document_id(&kv_reader, &milli_index_fields)
                .unwrap_or_report()
                .map_err(|_| todo!())
                .unwrap();
            let tid = name_to_tid(&pk_value).unwrap_or_report();
            tids.push(tid);
        }
    });

    // Add the TID Vec to a TIDBitmap
    unsafe {
        pg_sys::tbm_add_tuples(
            tbm.into_pg(),
            tids.as_mut_ptr(),
            tids.len() as raw::c_int,
            false,
        );
    }

    tids.len() as i64
}

#[pg_guard]
pub extern "C" fn amendscan(scan: pg_sys::IndexScanDesc) {
    info!("{PROGRAM_NAME} amendscan");
    let scan = unsafe { PgBox::from_pg(scan) };

    // Release opaque resources
    let opaque = unsafe { PgBox::from_pg(scan.opaque as *mut ScanOpaque) };
    unsafe { ptr::read(opaque.borrow_lmdb_rtxn()) }
        .commit()
        .unwrap_or_report();
    drop(opaque);
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Failed to split TID serialization, it could be malformed")]
    TidSplit,
    #[error("Failed to parse TID integer, it could be malformed: {0}")]
    TidParseInt(ParseIntError),
    #[error("Failed to parse index option value from JSON: {0}")]
    OptionsParseValue(serde_json::Error),
    #[error("Failed to encode index options to CBOR: {0}")]
    OptionsEncode(ciborium::ser::Error<io::Error>),
    #[error("Failed to decode index options from CBOR: {0}")]
    OptionsDecode(ciborium::de::Error<io::Error>),
    #[error("Invalid index options keys: {0:?}")]
    OptionsInvalidKeys(Vec<String>),
    #[error("Could not resolve index space OID: {0}")]
    SpaceResolve(io::Error),
}
