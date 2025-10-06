use std::ffi::CStr;
use std::os::raw::{c_char, c_int};
use std::ptr;
use std::slice;
use std::sync::Arc;
use tokio::runtime::Runtime;
use once_cell::sync::Lazy;
use tonbo::{DB, DbOption, Projection};
use tonbo::record::{DynRecord, DynSchema, DynamicField, Value, AsValue};
use tonbo::arrow::datatypes::DataType;
use tonbo::executor::tokio::TokioExecutor;
use fusio::path::Path as FusioPath;

// Global Tokio runtime for async operations
static RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    Runtime::new().expect("Failed to create Tokio runtime")
});

// Create a dynamic schema for key-value storage
fn kv_schema() -> DynSchema {
    let fields = vec![
        DynamicField::new("key".to_string(), DataType::Utf8, false),
        DynamicField::new("value".to_string(), DataType::Binary, false),
        DynamicField::new("timestamp".to_string(), DataType::Int64, false),
    ];
    DynSchema::new(&fields, 0) // primary key index = 0 (key column)
}

// Opaque handle for database
pub struct TonboDb {
    db: Arc<DB<DynRecord, TokioExecutor>>,
}

// Opaque handle for iterator
pub struct TonboIter {
    // We'll implement this when scan is needed
    _placeholder: (),
}

/// Open a Tonbo database at the given path
///
/// Returns NULL on failure
#[no_mangle]
pub extern "C" fn tonbo_db_open(path: *const c_char) -> *mut TonboDb {
    if path.is_null() {
        return ptr::null_mut();
    }

    let path_str = unsafe {
        match CStr::from_ptr(path).to_str() {
            Ok(s) => s,
            Err(_) => return ptr::null_mut(),
        }
    };

    RUNTIME.block_on(async {
        // Create database path
        let fusio_path = match FusioPath::from_filesystem_path(path_str) {
            Ok(p) => p,
            Err(_) => return ptr::null_mut(),
        };

        // Create dynamic schema
        let schema = kv_schema();

        // Create database options
        let options = DbOption::new(fusio_path, &schema);

        // Open database
        match DB::new(options, TokioExecutor::default(), schema).await {
            Ok(db) => {
                let tonbo_db = Box::new(TonboDb {
                    db: Arc::new(db),
                });
                Box::into_raw(tonbo_db)
            }
            Err(_) => ptr::null_mut(),
        }
    })
}

/// Get a value by key
///
/// Returns:
///   0 on success (value_out and value_len_out are set)
///   -1 if key not found
///   -2 on error
#[no_mangle]
pub extern "C" fn tonbo_db_get(
    db: *mut TonboDb,
    key: *const c_char,
    key_len: usize,
    value_out: *mut *mut u8,
    value_len_out: *mut usize,
) -> c_int {
    if db.is_null() || key.is_null() || value_out.is_null() || value_len_out.is_null() {
        return -2;
    }

    let tonbo_db = unsafe { &*db };

    let key_bytes = unsafe { slice::from_raw_parts(key as *const u8, key_len) };
    let key_str = match std::str::from_utf8(key_bytes) {
        Ok(s) => s.to_string(),
        Err(_) => return -2,
    };

    RUNTIME.block_on(async {
        let txn = tonbo_db.db.transaction().await;

        // Create key as Value
        let key_value = Value::String(key_str.into());

        // Get the entry and extract data immediately before dropping txn
        let result = txn.get(&key_value, Projection::All).await;

        match result {
            Ok(Some(entry)) => {
                // Extract value from dynamic record (index 1 is value column)
                let record_ref = entry.get();

                // Access value from ValueRef (columns is Vec<ValueRef>)
                // Copy the bytes immediately since record_ref is borrowed
                let value_bytes = record_ref.columns[1].as_bytes().to_vec();
                let value_len = value_bytes.len();

                // Allocate memory for the value
                let value_ptr = unsafe {
                    let layout = std::alloc::Layout::from_size_align(value_len, 1).unwrap();
                    let ptr = std::alloc::alloc(layout);
                    if ptr.is_null() {
                        return -2;
                    }
                    ptr::copy_nonoverlapping(value_bytes.as_ptr(), ptr, value_len);
                    ptr
                };

                unsafe {
                    *value_out = value_ptr;
                    *value_len_out = value_len;
                }
                0
            }
            Ok(None) => -1,
            Err(_) => -2,
        }
    })
}

/// Insert a key-value pair
///
/// Returns:
///   0 on success
///   -1 on error
#[no_mangle]
pub extern "C" fn tonbo_db_insert(
    db: *mut TonboDb,
    key: *const c_char,
    key_len: usize,
    value: *const u8,
    value_len: usize,
) -> c_int {
    if db.is_null() || key.is_null() || value.is_null() {
        return -1;
    }

    let tonbo_db = unsafe { &*db };

    let key_bytes = unsafe { slice::from_raw_parts(key as *const u8, key_len) };
    let key_str = match std::str::from_utf8(key_bytes) {
        Ok(s) => s.to_string(),
        Err(_) => return -1,
    };

    let value_bytes = unsafe { slice::from_raw_parts(value, value_len) }.to_vec();

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64;

    RUNTIME.block_on(async {
        // Create dynamic record with values (no Option wrapper)
        let values = vec![
            Value::String(key_str.into()),
            Value::Binary(value_bytes),
            Value::Int64(timestamp),
        ];
        let record = DynRecord::new(values, 0); // primary_index = 0 (key column)

        match tonbo_db.db.insert(record).await {
            Ok(_) => 0,
            Err(_) => -1,
        }
    })
}

/// Delete a key
///
/// Returns:
///   0 on success
///   -1 if key not found
///   -2 on error
#[no_mangle]
pub extern "C" fn tonbo_db_delete(
    db: *mut TonboDb,
    key: *const c_char,
    key_len: usize,
) -> c_int {
    if db.is_null() || key.is_null() {
        return -2;
    }

    let tonbo_db = unsafe { &*db };

    let key_bytes = unsafe { slice::from_raw_parts(key as *const u8, key_len) };
    let key_str = match std::str::from_utf8(key_bytes) {
        Ok(s) => s.to_string(),
        Err(_) => return -2,
    };

    RUNTIME.block_on(async {
        // Create key as Value (pass by value, not reference)
        let key_value = Value::String(key_str.into());

        match tonbo_db.db.remove(key_value).await {
            Ok(_) => 0,
            Err(_) => -1,
        }
    })
}

/// Close the database
#[no_mangle]
pub extern "C" fn tonbo_db_close(db: *mut TonboDb) {
    if !db.is_null() {
        unsafe {
            let _ = Box::from_raw(db);
        }
    }
}

/// Free bytes allocated by tonbo_db_get
#[no_mangle]
pub extern "C" fn tonbo_free_bytes(ptr: *mut u8, len: usize) {
    if !ptr.is_null() && len > 0 {
        unsafe {
            let layout = std::alloc::Layout::from_size_align(len, 1).unwrap();
            std::alloc::dealloc(ptr, layout);
        }
    }
}

// Scan operations (placeholder for future implementation)
#[no_mangle]
pub extern "C" fn tonbo_db_scan(
    _db: *mut TonboDb,
    _start_key: *const c_char,
    _end_key: *const c_char,
    _limit: c_int,
) -> *mut TonboIter {
    // TODO: Implement scan when needed
    ptr::null_mut()
}

#[no_mangle]
pub extern "C" fn tonbo_iter_next(
    _iter: *mut TonboIter,
    _key_out: *mut *mut c_char,
    _value_out: *mut *mut u8,
    _value_len_out: *mut usize,
) -> c_int {
    // TODO: Implement when scan is implemented
    -1
}

#[no_mangle]
pub extern "C" fn tonbo_iter_free(_iter: *mut TonboIter) {
    // TODO: Implement when scan is implemented
}
