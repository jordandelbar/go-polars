use crate::conversions::*;
use crate::{set_last_error};
use polars::prelude::*;
use std::cell::RefCell;
use std::ffi::{c_int, CStr, CString};
use std::fs::File;
use std::os::raw::c_char;
use std::ptr;
use std::rc::Rc;
use serde_json::json;
use serde_json::Map;
use polars::frame::row::Row;

#[repr(C)]
pub struct CIterator {
    inner: *mut Rc<RefCell<RowIterator>>,
}

#[repr(C)]
pub struct BytesResult {
    ptr: *const u8,
    len: usize,
}

struct RowIterator{
    df: Rc<RefCell<DataFrame>>,
    current: usize,
    total_rows: usize,
}

#[no_mangle]
pub extern "C" fn read_csv(path: *const c_char) -> *mut CDataFrame {
    let c_str = unsafe { CStr::from_ptr(path) };
    let path_str = match c_str.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_last_error("Invalid UTF-8 path");
            return ptr::null_mut();
        }
    };

    match CsvReadOptions::default()
        .try_into_reader_with_file_path(Some(path_str.into()))
        .and_then(|reader| reader.finish())
    {
        Ok(df) => polars_df_to_c_df(df),
        Err(e) => {
            set_last_error(&format!("Failed to read CSV: {}", e));
            return ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn read_parquet(path: *const c_char) -> *mut CDataFrame {
    unsafe {
        let c_str = CStr::from_ptr(path);
        let path_str = match c_str.to_str() {
            Ok(s) => s,
            Err(_) => {
                set_last_error("Invalid UTF-8 path");
                return ptr::null_mut();
            }
        };

        let file = match File::open(path_str) {
            Ok(f) => f,
            Err(e) => {
                set_last_error(&format!("Failed to open file: {}", e));
                return ptr::null_mut();
            }
        };

        let parquet_reader = ParquetReader::new(file);
        match parquet_reader.finish() {
            Ok(df) => polars_df_to_c_df(df),
            Err(e) => {
                set_last_error(&format!("Failed to read Parquet: {}", e));
                return ptr::null_mut()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn free_dataframe(df: *mut CDataFrame) {
    unsafe {
        if df.is_null() {
            return;
        }
        let c_df = Box::from_raw(df);
        if !c_df.inner.is_null() {
                drop(Box::from_raw(c_df.inner as *mut Rc<RefCell<DataFrame>>));
                drop(c_df);
        }
    }
}

#[no_mangle]
pub extern "C" fn print_dataframe(df_ptr: *mut CDataFrame) -> *const c_char {
    unsafe {
        match c_df_to_polars_df(df_ptr) {
            Ok(rc_df) => {
                let df = rc_df.borrow();
                let df_str = format!("{}", df);
                CString::new(df_str).unwrap().into_raw()
            }
            Err(e) => {
                set_last_error(&format!("Print DataFrame error: {}", e));
                ptr::null()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn dataframe_width(df: *const CDataFrame) -> usize {
    unsafe {
        match c_df_to_polars_df_ref(df) {
            Ok(rc_df) => rc_df.borrow().width(),
            Err(_) => 0,
        }
    }
}

#[no_mangle]
pub extern "C" fn dataframe_height(df: *const CDataFrame) -> usize {
    unsafe {
        match c_df_to_polars_df_ref(df) {
            Ok(rc_df) => rc_df.borrow().height(),
            Err(_) => 0,
        }
    }
}

#[no_mangle]
pub extern "C" fn columns(df_ptr: *mut CDataFrame) -> *const c_char {
    unsafe {
        let df_result = c_df_to_polars_df(df_ptr);
        match df_result {
            Ok(rc_df) => {
                let df = rc_df.borrow_mut();
                let col_names: Vec<String> = df
                    .get_column_names()
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect();
                let joined_names = col_names.join(",");
                CString::new(joined_names).unwrap().into_raw()
            }
            Err(e) => {
                set_last_error(&format!("Columns error: {}", e));
                ptr::null()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn dataframe_column_name(df: *const CDataFrame, index: usize) -> *const c_char {
    unsafe {
        match c_df_to_polars_df_ref(df) {
            Ok(rc_df) => {
                let df = rc_df.borrow_mut();
                let names = df.get_column_names();
                if index < names.len() {
                    CString::new(names[index].as_str()).unwrap().into_raw()
                } else {
                    set_last_error("Index out of bounds for column names");
                    ptr::null()
                }
            }
            Err(e) => {
                set_last_error(&format!("Error getting column name: {}", e));
                ptr::null()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn filter(df_ptr: *mut CDataFrame, expr_ptr: *mut CExpr) -> *mut CDataFrame {
    unsafe {
        match (c_df_to_polars_df(df_ptr), c_expr_to_expr(expr_ptr)) {
            (Ok(rc_df), Ok(expr)) => {
                let df = rc_df.borrow_mut();
                match df.clone().lazy().filter(expr.clone()).collect() {
                    Ok(filtered_df) => polars_df_to_c_df(filtered_df),
                    Err(e) => {
                        set_last_error(&format!("Filter error: {}", e));
                        ptr::null_mut()
                    }
                }
            }
            _ => {
                set_last_error("Error converting DataFrame or expression");
                ptr::null_mut()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn head(df_ptr: *mut CDataFrame, n: usize) -> *mut CDataFrame {
    unsafe {
        let df_result = c_df_to_polars_df(df_ptr);
        match df_result {
            Ok(rc_df) => {
                let df = rc_df.borrow_mut();
                let head_df = df.head(Some(n));
                return polars_df_to_c_df(head_df);
            }
            Err(e) => {
                set_last_error(&format!("Error getting head: {}", e));
                return ptr::null_mut()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn write_csv(df_ptr: *mut CDataFrame, file_path: *const c_char) -> *const c_char {
    unsafe {
        match c_df_to_polars_df(df_ptr) {
            Ok(rc_df) => {

                let path_str = match CStr::from_ptr(file_path).to_str() {
                    Ok(s) => s,
                    Err(_) => {
                        set_last_error("Invalid UTF-8 file path");
                        return ptr::null();
                    }
                };

                let df = rc_df.borrow_mut();
                let mut df_clone = df.clone();

                match File::create(path_str) {
                    Ok(mut file) => match CsvWriter::new(&mut file).finish(&mut df_clone) {
                        Ok(_) => CString::new("CSV written successfully").unwrap().into_raw(),
                        Err(e) => {
                            set_last_error(&format!("Error writing CSV: {}", e));
                            CString::new(format!("Error writing CSV: {}", e))
                                .unwrap()
                                .into_raw()
                        }
                    },
                    Err(e) => {
                        set_last_error(&format!("Error creating file: {}", e));
                        CString::new(format!("Error creating file: {}", e))
                            .unwrap()
                            .into_raw()
                    }
                }
            }
            Err(e) => {
                set_last_error(&format!("Error in write_csv: {}", e));
                CString::new(format!("Error in write_csv: {}", e))
                    .unwrap()
                    .into_raw()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn write_parquet(
    df_ptr: *mut CDataFrame,
    file_path: *const c_char,
) -> *const c_char {
    unsafe {

        let path_str = match CStr::from_ptr(file_path).to_str() {
            Ok(s) => s,
            Err(_) => {
                set_last_error("Invalid UTF-8 path");
                return ptr::null();
            }
        };

        match c_df_to_polars_df(df_ptr) {
            Ok(rc_df) => {
                let df = rc_df.borrow_mut();
                let file = match File::create(path_str) {
                    Ok(f) => f,
                    Err(e) => {
                        set_last_error(&format!("Failed to create file: {}", e));
                        return ptr::null_mut();
                    }
                };

                let writer = ParquetWriter::new(file);

                match writer.finish(&mut df.clone()) {
                    Ok(_) => CString::new("Parquet written successfully")
                        .unwrap()
                        .into_raw(),
                    Err(e) => {
                        set_last_error(&format!("Failed to write Parquet: {}", e));
                        CString::new(format!("Failed to write Parquet: {}", e))
                            .unwrap()
                            .into_raw()
                    }
                }
            }
            Err(e) => {
                set_last_error(&format!("Error getting DataFrame: {}", e));
                return ptr::null_mut();
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn with_columns(
    df_ptr: *mut CDataFrame,
    exprs_ptr: *mut *mut CExpr,
    exprs_len: c_int,
) -> *mut CDataFrame {
    unsafe {
        match c_df_to_polars_df(df_ptr) {
            Ok(rc_df) => {

                let exprs_slice = std::slice::from_raw_parts(exprs_ptr, exprs_len as usize);
                let mut exprs: Vec<Expr> = Vec::with_capacity(exprs_len as usize);

                for &expr_ptr in exprs_slice {
                    match c_expr_to_expr(expr_ptr) {
                        Ok(expr) => exprs.push(expr.clone()),
                        Err(e) => {
                            set_last_error(&format!("Error converting expr: {}", e));
                            return ptr::null_mut();
                        }
                    }
                }

                let df = rc_df.borrow();
                let mut lazy_df = df.clone().lazy().with_columns(&exprs);
                for expr in exprs {
                    lazy_df = lazy_df.with_column(expr);
                }

                match lazy_df.collect() {
                    Ok(new_df) => polars_df_to_c_df(new_df),
                    Err(e) => {
                        set_last_error(&format!("Error with_columns: {}", e));
                        ptr::null_mut()
                    }
                }
            }
            Err(e) => {
                set_last_error(&format!("Error in with_columns: {}", e));
                ptr::null_mut()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn select_columns(
    df_ptr: *mut CDataFrame,
    exprs_ptr: *mut *mut CExpr,
    exprs_len: c_int,
) -> *mut CDataFrame {
    unsafe {
        match c_df_to_polars_df(df_ptr) {
            Ok(rc_df) => {
                let exprs_slice = std::slice::from_raw_parts(exprs_ptr, exprs_len as usize);
                let mut exprs: Vec<Expr> = Vec::with_capacity(exprs_len as usize);

                for &expr_ptr in exprs_slice {
                    match c_expr_to_expr(expr_ptr) {
                        Ok(expr) => exprs.push(expr),
                        Err(e) => {
                            set_last_error(&format!("Error converting expr: {}", e));
                            return ptr::null_mut();
                        }
                    }
                }

                let df = rc_df.borrow();
                let lazy_df = df.clone().lazy();
                let selected_lazy_df = lazy_df.select(exprs);

                match selected_lazy_df.collect() {
                    Ok(selected_df) => polars_df_to_c_df(selected_df),
                    Err(e) => {
                        set_last_error(&format!("Error in select: {}", e));
                        return ptr::null_mut();
                    }
                }
            }
            Err(e) => {
                set_last_error(&format!("Error getting DataFrame: {}", e));
                return ptr::null_mut();
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn dataframe_iter(df: *mut CDataFrame)-> *mut CIterator{
    unsafe {
        match c_df_to_polars_df(df) {
            Ok(rc_df) => {
                let df = rc_df.borrow();
                let total_rows = df.height();

                let iter = Rc::new(RefCell::new(RowIterator {
                    df: Rc::clone(&rc_df),
                    current: 0,
                    total_rows,
                }));

                Box::into_raw(Box::new(CIterator{
                    inner: Rc::into_raw(iter) as *mut _,
                }))
            }
            Err(e) => {
                set_last_error(&format!("Error creating iterator: {}", e));
                ptr::null_mut()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn iter_next(iter: *mut CIterator) -> *const c_char {
    unsafe {
        if iter.is_null() {
            set_last_error("Iterator is null");
            return ptr::null();
        }

        let c_iter = &*iter;
        let rc_iter = Rc::from_raw(c_iter.inner as *const RefCell<RowIterator>);
        let mut iter_inner = rc_iter.borrow_mut();

        if iter_inner.current >= iter_inner.total_rows {
            return ptr::null();
        }

        let row_str = {
            let df = iter_inner.df.borrow();

            match df.get_row(iter_inner.current) {
                Ok(row) => format!("{:?}", row),
                Err(e) => {
                    set_last_error(&format!("Error getting row: {}", e));
                    return ptr::null();
                }
            }
        };

        iter_inner.current += 1;
        match CString::new(row_str) {
            Ok(c_str) => c_str.into_raw(),
            Err(_) => {
                set_last_error("Failed to convert row to C string");
                ptr::null()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn free_iterator(iter: *mut CIterator) {
    unsafe {
        if iter.is_null() {
            return;
        }
        let c_iter = Box::from_raw(iter);
        if !c_iter.inner.is_null() {
            drop(Rc::from_raw(c_iter.inner as *const RefCell<RowIterator>));
        }
    }
}

#[no_mangle]
pub extern "C" fn iter_next_json(iter: *mut CIterator) -> *const c_char {
    unsafe {
        if iter.is_null() {
            set_last_error("Iterator is null");
            return ptr::null();
        }

        let c_iter = &*iter;
        let rc_iter = Rc::from_raw(c_iter.inner as *const RefCell<RowIterator>);
        let mut iter_inner = rc_iter.borrow_mut();

        if iter_inner.current >= iter_inner.total_rows {
            return ptr::null();
        }

        let json_str = {
            let df = iter_inner.df.borrow();

            match df.get_row(iter_inner.current) {
                Ok(row) => serialize_row_to_json(&row, &df),
                Err(e) => {
                    set_last_error(&format!("Error getting row: {}", e));
                    return ptr::null();
                }
            }
        };

        iter_inner.current += 1;
        match CString::new(json_str) {
            Ok(c_str) => c_str.into_raw(),
            Err(_) => {
                set_last_error("Failed to convert row to JSON string");
                ptr::null()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn free_bytes(ptr: *mut u8, len: usize) {
    unsafe {
        if ptr.is_null() {
            return;
        }
        let _ = Vec::from_raw_parts(ptr, len, len);
    }
}

fn serialize_row_to_json(row: &Row, df: &DataFrame) -> String {
    let mut json_data = Map::new();
    let column_names = df.get_column_names();

    for (i, value) in row.0.iter().enumerate() {
        let key = column_names.get(i).map(|s| s.to_string()).unwrap_or_else(|| format!("col_{}", i));
        match value {
            AnyValue::Null => {
                json_data.insert(key, json!(null));
            }
            AnyValue::Boolean(v) => {
                json_data.insert(key, json!(*v));
            }
            AnyValue::Int8(v) => {
                json_data.insert(key, json!(*v));
            }
            AnyValue::Int16(v) => {
                json_data.insert(key, json!(*v));
            }
            AnyValue::Int32(v) => {
                json_data.insert(key, json!(*v));
            }
            AnyValue::Int64(v) => {
                json_data.insert(key, json!(*v));
            }
            AnyValue::UInt8(v) => {
                json_data.insert(key, json!(*v));
            }
            AnyValue::UInt16(v) => {
                json_data.insert(key, json!(*v));
            }
            AnyValue::UInt32(v) => {
                json_data.insert(key, json!(*v));
            }
            AnyValue::UInt64(v) => {
                json_data.insert(key, json!(*v));
            }
            AnyValue::Float32(v) => {
                json_data.insert(key, json!(*v));
            }
            AnyValue::Float64(v) => {
                json_data.insert(key, json!(*v));
            }
            AnyValue::String(s) => {
                json_data.insert(key, json!(s));
            }
            AnyValue::Date(v) => {
                json_data.insert(key, json!(v));
            }
            AnyValue::Datetime(v, _, _) => {
                json_data.insert(key, json!(v));
            }
            AnyValue::Duration(v, _) => {
                json_data.insert(key, json!(v));
            }
            AnyValue::Time(v) => {
                json_data.insert(key, json!(v));
            }
            _ => {
                // Handle unsupported types
                json_data.insert(key, json!("unsupported_type"));
            }
        }
    }

    serde_json::to_string(&json_data).unwrap_or_else(|_| "{}".to_string())

}