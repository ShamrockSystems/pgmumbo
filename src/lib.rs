use std::{
    ffi::{CStr, CString, OsStr, OsString},
    fmt::format,
    mem::size_of,
    path::Path,
    str::FromStr,
};

use pg_sys::panic::ErrorReportable;
use pgrx::{prelude::*, set_varsize_4b, PgRelation};
use serde::{Deserialize, Serialize};

pgrx::pg_module_magic!();

#[pg_extern(sql = "
    CREATE OR REPLACE FUNCTION pgmumbo_am_handler(internal)
        RETURNS index_am_handler
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

    amroutine.amkeytype = pg_sys::InvalidOid;

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

const VERSION_MAJOR: u32 = 0;
const VERSION_MINOR: u32 = 0;
const VERSION_PATCH: u32 = 0;

const PGDATA_BASE: &'static str = "pgmumbo";

const INITIAL_LMDB_MMAP_SIZE: usize = 64 * 1024 * 1024 * 1024; // Initialize LMDB with 64 GB memory map

#[derive(Copy, Clone, PostgresType, Serialize, Deserialize)]
#[pgvarlena_inoutfuncs]
struct MumboIndexOptions {
    version_major: u32,
    version_minor: u32,
    version_patch: u32,
}

impl PgVarlenaInOutFuncs for MumboIndexOptions {
    fn input(input: &core::ffi::CStr) -> PgVarlena<Self>
    where
        Self: Copy + Sized,
    {
        let deserialized: MumboIndexOptions =
            serde_json::from_str(input.to_str().unwrap_or_report()).unwrap_or_report();

        let mut result = PgVarlena::<MumboIndexOptions>::new();
        result.version_major = deserialized.version_major;
        result.version_minor = deserialized.version_minor;
        result.version_patch = deserialized.version_patch;
        result
    }

    fn output(&self, buffer: &mut pgrx::StringInfo) {
        buffer.push_str(serde_json::to_string(self).unwrap_or_report().as_str())
    }
}

#[pg_guard]
pub extern "C" fn ambuild(
    heap_relation: pg_sys::Relation,
    index_relation: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
) -> *mut pg_sys::IndexBuildResult {
    info!("pgmumbo ambuild");

    let heap_relation = unsafe { PgRelation::from_pg(heap_relation) };
    let index_relation = unsafe { PgRelation::from_pg(index_relation) };

    assert!(index_relation.is_index());

    let index_options = if index_relation.rd_options.is_null() {
        let options = unsafe { PgBox::<MumboIndexOptions>::alloc0() };
        unsafe {
            set_varsize_4b(
                options.as_ptr().cast(),
                size_of::<MumboIndexOptions>() as i32,
            );
        }
        options.into_pg_boxed()
    } else {
        unsafe { PgBox::from_pg(index_relation.rd_options as *mut MumboIndexOptions) }
    };

    let index_oid = index_relation.oid();
    let path = Path::new(
        unsafe { CStr::from_ptr(pg_sys::DataDir) }
            .to_str()
            .unwrap_or_report(),
    )
    .join(PGDATA_BASE)
    .join(format!("{:x}", unsafe { pg_sys::MyDatabaseId }.as_u32()));

    todo!()
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
    info!("pgmumbo aminsert");

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
    todo!()
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
