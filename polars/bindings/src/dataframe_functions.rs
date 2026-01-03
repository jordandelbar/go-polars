use crate::conversions::*;
use crate::set_last_error;
use polars::io::json::{JsonFormat, JsonReader, JsonWriter};
use polars::lazy::frame::{LazyCsvReader, LazyJsonLineReader};
use polars::prelude::*;
use std::cell::RefCell;
use std::ffi::{c_int, CStr, CString};
use std::fs::File;
use std::os::raw::c_char;
use std::ptr;
use std::rc::Rc;
use std::sync::Arc;

fn dtype_to_c_tag(dtype: &DataType) -> i32 {
    match dtype {
        DataType::Boolean => 0,
        DataType::Int32 => 1,
        DataType::Int64 => 2,
        DataType::Float32 => 3,
        DataType::Float64 => 4,
        DataType::String => 5,
        DataType::Date => 6,
        DataType::Datetime(TimeUnit::Milliseconds, _) => 7,
        _ => -1,
    }
}

fn dtype_from_c_tag(tag: i32) -> Option<DataType> {
    match tag {
        0 => Some(DataType::Boolean),
        1 => Some(DataType::Int32),
        2 => Some(DataType::Int64),
        3 => Some(DataType::Float32),
        4 => Some(DataType::Float64),
        5 => Some(DataType::String),
        6 => Some(DataType::Date),
        7 => Some(DataType::Datetime(TimeUnit::Milliseconds, None)),
        _ => None,
    }
}

#[no_mangle]
pub extern "C" fn dataframe_column_dtype(df: *const CDataFrame, index: usize) -> i32 {
    unsafe {
        match c_df_to_polars_df_ref(df) {
            Ok(rc_df) => {
                let df_ref = rc_df.borrow();
                if index >= df_ref.width() {
                    set_last_error("column index out of bounds");
                    return -1;
                }
                dtype_to_c_tag(&df_ref.get_columns()[index].dtype())
            }
            Err(e) => {
                set_last_error(&format!("schema error: {}", e));
                -1
            }
        }
    }
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
            return ptr::null_mut();
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
                return ptr::null_mut();
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn read_json(path: *const c_char) -> *mut CDataFrame {
    let c_str = unsafe { CStr::from_ptr(path) };
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

    match JsonReader::new(file)
        .with_json_format(JsonFormat::JsonLines)
        .finish()
    {
        Ok(df) => polars_df_to_c_df(df),
        Err(e) => {
            set_last_error(&format!("Failed to read JSON: {}", e));
            ptr::null_mut()
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
pub extern "C" fn drop_columns(df_ptr: *mut CDataFrame, columns_ptr: *const c_char) -> *mut CDataFrame {
    if df_ptr.is_null() {
        set_last_error("DataFrame pointer is null");
        return ptr::null_mut();
    }

    let cols_str = unsafe {
        match CStr::from_ptr(columns_ptr).to_str() {
            Ok(s) => s,
            Err(_) => {
                set_last_error("Invalid UTF-8 in columns");
                return ptr::null_mut();
            }
        }
    };

    let columns: Vec<String> = cols_str
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();

    if columns.is_empty() {
        set_last_error("No columns provided to drop");
        return ptr::null_mut();
    }

    let df_rc = match unsafe { c_df_to_polars_df(df_ptr) } {
        Ok(rc) => rc,
        Err(e) => {
            set_last_error(&format!("Failed to get DataFrame: {}", e));
            return ptr::null_mut();
        }
    };

    let out_df = match df_rc.try_borrow() {
        Ok(df) => df.clone().drop_many(columns),
        Err(_) => {
            set_last_error("Failed to borrow DataFrame for drop");
            return ptr::null_mut();
        }
    };

    polars_df_to_c_df(out_df)
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
                return ptr::null_mut();
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
pub extern "C" fn write_json(df_ptr: *mut CDataFrame, file_path: *const c_char) -> *const c_char {
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
                let mut df_clone = df.clone();

                let file = match File::create(path_str) {
                    Ok(f) => f,
                    Err(e) => {
                        set_last_error(&format!("Error creating file: {}", e));
                        return CString::new(format!("Error creating file: {}", e))
                            .unwrap()
                            .into_raw();
                    }
                };

                let mut writer = JsonWriter::new(file).with_json_format(JsonFormat::JsonLines);

                match writer.finish(&mut df_clone) {
                    Ok(_) => CString::new("JSON written successfully").unwrap().into_raw(),
                    Err(e) => {
                        set_last_error(&format!("Error writing JSON: {}", e));
                        CString::new(format!("Error writing JSON: {}", e))
                            .unwrap()
                            .into_raw()
                    }
                }
            }
            Err(e) => {
                set_last_error(&format!("Error in write_json: {}", e));
                CString::new(format!("Error in write_json: {}", e))
                    .unwrap()
                    .into_raw()
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
pub extern "C" fn sort_by_columns(
    df_ptr: *mut CDataFrame,
    columns: *const c_char,
    descending: *const c_char,
) -> *mut CDataFrame {
    unsafe {
        match c_df_to_polars_df(df_ptr) {
            Ok(rc_df) => {
                let columns_str = match CStr::from_ptr(columns).to_str() {
                    Ok(s) => s,
                    Err(_) => {
                        set_last_error("Invalid UTF-8 columns string");
                        return ptr::null_mut();
                    }
                };

                let descending_str = match CStr::from_ptr(descending).to_str() {
                    Ok(s) => s,
                    Err(_) => {
                        set_last_error("Invalid UTF-8 descending string");
                        return ptr::null_mut();
                    }
                };

                let column_names: Vec<&str> = if columns_str.is_empty() {
                    vec![]
                } else {
                    columns_str.split(',').collect()
                };

                let descending_flags: Vec<&str> = if descending_str.is_empty() {
                    vec![]
                } else {
                    descending_str.split(',').collect()
                };

                if column_names.len() != descending_flags.len() {
                    set_last_error("Columns and descending arrays must have the same length");
                    return ptr::null_mut();
                }

                let df = rc_df.borrow();

                if column_names.is_empty() {
                    // No columns to sort by, return original DataFrame
                    return polars_df_to_c_df(df.clone());
                }

                let mut sort_exprs: Vec<Expr> = Vec::new();
                let mut sort_options: Vec<bool> = Vec::new();

                for (i, col_name) in column_names.iter().enumerate() {
                    let desc = descending_flags[i] == "true";
                    sort_exprs.push(col(*col_name));
                    sort_options.push(desc);
                }

                match df
                    .clone()
                    .lazy()
                    .sort_by_exprs(
                        &sort_exprs,
                        SortMultipleOptions::default().with_order_descending_multi(sort_options),
                    )
                    .collect()
                {
                    Ok(sorted_df) => polars_df_to_c_df(sorted_df),
                    Err(e) => {
                        set_last_error(&format!("Sort error: {}", e));
                        ptr::null_mut()
                    }
                }
            }
            Err(e) => {
                set_last_error(&format!("Error getting DataFrame: {}", e));
                ptr::null_mut()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn sort_by_exprs(
    df_ptr: *mut CDataFrame,
    exprs_ptr: *mut *mut CExpr,
    exprs_len: c_int,
    descending: *const c_char,
) -> *mut CDataFrame {
    unsafe {
        match c_df_to_polars_df(df_ptr) {
            Ok(rc_df) => {
                let descending_str = match CStr::from_ptr(descending).to_str() {
                    Ok(s) => s,
                    Err(_) => {
                        set_last_error("Invalid UTF-8 descending string");
                        return ptr::null_mut();
                    }
                };

                let descending_flags: Vec<&str> = if descending_str.is_empty() {
                    vec![]
                } else {
                    descending_str.split(',').collect()
                };

                if descending_flags.len() != exprs_len as usize {
                    set_last_error("Expressions and descending arrays must have the same length");
                    return ptr::null_mut();
                }

                let exprs_slice = std::slice::from_raw_parts(exprs_ptr, exprs_len as usize);
                let mut sort_exprs: Vec<Expr> = Vec::with_capacity(exprs_len as usize);
                let mut desc_bools: Vec<bool> = Vec::with_capacity(exprs_len as usize);

                for (i, &expr_ptr) in exprs_slice.iter().enumerate() {
                    match c_expr_to_expr(expr_ptr) {
                        Ok(expr) => {
                            sort_exprs.push(expr);
                            desc_bools.push(descending_flags[i] == "true");
                        }
                        Err(e) => {
                            set_last_error(&format!("Error converting expr: {}", e));
                            return ptr::null_mut();
                        }
                    }
                }

                let df = rc_df.borrow();
                match df
                    .clone()
                    .lazy()
                    .sort_by_exprs(
                        &sort_exprs,
                        SortMultipleOptions::default().with_order_descending_multi(desc_bools),
                    )
                    .collect()
                {
                    Ok(sorted_df) => polars_df_to_c_df(sorted_df),
                    Err(e) => {
                        set_last_error(&format!("Sort by expressions error: {}", e));
                        ptr::null_mut()
                    }
                }
            }
            Err(e) => {
                set_last_error(&format!("Error getting DataFrame: {}", e));
                ptr::null_mut()
            }
        }
    }
}

// DataFrame creation functions

fn row_is_valid(validity_ptr: *const u8, idx: usize) -> bool {
    if validity_ptr.is_null() {
        return true;
    }
    unsafe { *validity_ptr.add(idx) != 0 }
}

// Column type enum for mixed DataFrame creation
#[repr(C)]
pub enum CColumnType {
    String = 0,
    Int64 = 1,
    Float64 = 2,
    Bool = 3,
    Date = 4,
    DatetimeMs = 5,
    ListString = 6,
    ListInt64 = 7,
    ListFloat64 = 8,
    ListBool = 9,
    ListDatetimeMs = 10,
}

#[repr(C)]
pub struct CColumnSpec {
    name: *const c_char,
    column_type: CColumnType,
    data: *const std::ffi::c_void,
    length: c_int,
    sub_lengths: *const c_int, // per-row length for list columns
    validity: *const u8,       // optional per-row validity bitmap (1=valid,0=null)
}

#[no_mangle]
pub extern "C" fn create_dataframe_mixed(
    column_specs: *const CColumnSpec,
    column_count: c_int,
) -> *mut CDataFrame {
    unsafe {
        if column_specs.is_null() || column_count <= 0 {
            set_last_error("Invalid parameters for DataFrame creation");
            return ptr::null_mut();
        }

        let mut series_vec = Vec::new();

        for i in 0..column_count {
            let spec = &*column_specs.add(i as usize);

            if spec.name.is_null() || spec.length < 0 {
                set_last_error("Invalid column specification");
                return ptr::null_mut();
            }

            // Allow null data only if length is 0 (empty column)
            if spec.data.is_null() && spec.length > 0 {
                set_last_error("Invalid column specification");
                return ptr::null_mut();
            }

            let name_cstr = CStr::from_ptr(spec.name);
            let name = match name_cstr.to_str() {
                Ok(s) => s,
                Err(_) => {
                    set_last_error("Invalid UTF-8 column name");
                    return ptr::null_mut();
                }
            };

            let series = match spec.column_type {
                CColumnType::String => {
                    if spec.length == 0 {
                        let empty_values: Vec<Option<String>> = Vec::new();
                        Series::new(name.into(), empty_values)
                    } else {
                        let string_ptrs = spec.data as *const *const c_char;
                        let mut values: Vec<Option<String>> = Vec::with_capacity(spec.length as usize);
                        for j in 0..spec.length {
                            if !row_is_valid(spec.validity, j as usize) {
                                values.push(None);
                                continue;
                            }
                            let str_ptr = *string_ptrs.add(j as usize);
                            if str_ptr.is_null() {
                                values.push(None);
                            } else {
                                let str_cstr = CStr::from_ptr(str_ptr);
                                match str_cstr.to_str() {
                                    Ok(s) => values.push(Some(s.to_string())),
                                    Err(_) => {
                                        set_last_error("Invalid UTF-8 string value");
                                        return ptr::null_mut();
                                    }
                                }
                            }
                        }
                        Series::new(name.into(), values)
                    }
                }
                CColumnType::Int64 => {
                    if spec.length == 0 {
                        let empty_values: Vec<Option<i64>> = Vec::new();
                        Series::new(name.into(), empty_values)
                    } else {
                        let int_data = spec.data as *const i64;
                        let mut values: Vec<Option<i64>> = Vec::with_capacity(spec.length as usize);
                        for j in 0..spec.length {
                            if !row_is_valid(spec.validity, j as usize) {
                                values.push(None);
                            } else {
                                let val = *int_data.add(j as usize);
                                values.push(Some(val));
                            }
                        }
                        Series::new(name.into(), values)
                    }
                }
                CColumnType::Float64 => {
                    if spec.length == 0 {
                        let empty_values: Vec<Option<f64>> = Vec::new();
                        Series::new(name.into(), empty_values)
                    } else {
                        let float_data = spec.data as *const f64;
                        let mut values: Vec<Option<f64>> = Vec::with_capacity(spec.length as usize);
                        for j in 0..spec.length {
                            if !row_is_valid(spec.validity, j as usize) {
                                values.push(None);
                            } else {
                                let val = *float_data.add(j as usize);
                                values.push(Some(val));
                            }
                        }
                        Series::new(name.into(), values)
                    }
                }
                CColumnType::Bool => {
                    if spec.length == 0 {
                        let empty_values: Vec<Option<bool>> = Vec::new();
                        Series::new(name.into(), empty_values)
                    } else {
                        let bool_data = spec.data as *const u8;
                        let mut values: Vec<Option<bool>> = Vec::with_capacity(spec.length as usize);
                        for j in 0..spec.length {
                            if !row_is_valid(spec.validity, j as usize) {
                                values.push(None);
                            } else {
                                let bool_val = *bool_data.add(j as usize) != 0;
                                values.push(Some(bool_val));
                            }
                        }
                        Series::new(name.into(), values)
                    }
                }
                CColumnType::Date => {
                    if spec.length == 0 {
                        let empty_values: Vec<Option<i32>> = Vec::new();
                        Series::new(name.into(), empty_values)
                    } else {
                        let date_data = spec.data as *const i32;
                        let mut values: Vec<Option<i32>> = Vec::with_capacity(spec.length as usize);
                        for j in 0..spec.length {
                            if !row_is_valid(spec.validity, j as usize) {
                                values.push(None);
                            } else {
                                let val = *date_data.add(j as usize);
                                values.push(Some(val));
                            }
                        }
                        let series = Series::new(name.into(), values);
                        match series.cast(&DataType::Date) {
                            Ok(s) => s,
                            Err(e) => {
                                set_last_error(&format!("Failed to cast date column: {}", e));
                                return ptr::null_mut();
                            }
                        }
                    }
                }
                CColumnType::DatetimeMs => {
                    if spec.length == 0 {
                        let empty_values: Vec<Option<i64>> = Vec::new();
                        Series::new(name.into(), empty_values)
                    } else {
                        let dt_data = spec.data as *const i64;
                        let mut values: Vec<Option<i64>> = Vec::with_capacity(spec.length as usize);
                        for j in 0..spec.length {
                            if !row_is_valid(spec.validity, j as usize) {
                                values.push(None);
                            } else {
                                let val = *dt_data.add(j as usize);
                                values.push(Some(val));
                            }
                        }
                        let series = Series::new(name.into(), values);
                        match series.cast(&DataType::Datetime(TimeUnit::Milliseconds, None)) {
                            Ok(s) => s,
                            Err(e) => {
                                set_last_error(&format!("Failed to cast datetime column: {}", e));
                                return ptr::null_mut();
                            }
                        }
                    }
                }
                CColumnType::ListString => {
                    if spec.length == 0 {
                        let mut lc = ListChunked::from_iter(std::iter::empty::<Series>());
                        lc.rename(name.into());
                        lc.into_series()
                    } else {
                        if spec.sub_lengths.is_null() {
                            set_last_error("Missing sub_lengths for ListString");
                            return ptr::null_mut();
                        }

                        let row_ptrs = spec.data as *const *const *const c_char;
                        let lens_ptr = spec.sub_lengths;

                        let mut rows: Vec<Option<Series>> = Vec::with_capacity(spec.length as usize);
                        for i in 0..spec.length {
                            if !row_is_valid(spec.validity, i as usize) {
                                rows.push(None);
                                continue;
                            }

                            let len = *lens_ptr.add(i as usize);
                            if len == 0 {
                                rows.push(Some(Series::new("".into(), Vec::<String>::new())));
                                continue;
                            }
                            let row_ptr = *row_ptrs.add(i as usize);
                            if row_ptr.is_null() {
                                rows.push(Some(Series::new("".into(), Vec::<String>::new())));
                                continue;
                            }

                            let row_slice = std::slice::from_raw_parts(row_ptr, len as usize);
                            let mut row_vec = Vec::with_capacity(len as usize);
                            for &cstr_ptr in row_slice {
                                if cstr_ptr.is_null() {
                                    row_vec.push(String::new());
                                } else {
                                    match CStr::from_ptr(cstr_ptr).to_str() {
                                        Ok(s) => row_vec.push(s.to_string()),
                                        Err(_) => {
                                            set_last_error("Invalid UTF-8 string value in list");
                                            return ptr::null_mut();
                                        }
                                    }
                                }
                            }
                            rows.push(Some(Series::new("".into(), row_vec)));
                        }

                        let mut lc = ListChunked::from_iter(rows);
                        lc.rename(name.into());
                        lc.into_series()
                    }
                }
                CColumnType::ListInt64 => {
                    if spec.length == 0 {
                        let mut lc = ListChunked::from_iter(std::iter::empty::<Series>());
                        lc.rename(name.into());
                        lc.into_series()
                    } else {
                        if spec.sub_lengths.is_null() {
                            set_last_error("Missing sub_lengths for ListInt64");
                            return ptr::null_mut();
                        }

                        let row_ptrs = spec.data as *const *const i64;
                        let lens_ptr = spec.sub_lengths;

                        let mut rows: Vec<Option<Series>> = Vec::with_capacity(spec.length as usize);
                        for i in 0..spec.length {
                            if !row_is_valid(spec.validity, i as usize) {
                                rows.push(None);
                                continue;
                            }

                            let len = *lens_ptr.add(i as usize);
                            if len == 0 {
                                rows.push(Some(Series::new("".into(), Vec::<i64>::new())));
                                continue;
                            }
                            let row_ptr = *row_ptrs.add(i as usize);
                            if row_ptr.is_null() {
                                rows.push(Some(Series::new("".into(), Vec::<i64>::new())));
                                continue;
                            }

                            let row_slice = std::slice::from_raw_parts(row_ptr, len as usize);
                            rows.push(Some(Series::new("".into(), row_slice)));
                        }

                        let mut lc = ListChunked::from_iter(rows);
                        lc.rename(name.into());
                        lc.into_series()
                    }
                }
                CColumnType::ListFloat64 => {
                    if spec.length == 0 {
                        let mut lc = ListChunked::from_iter(std::iter::empty::<Series>());
                        lc.rename(name.into());
                        lc.into_series()
                    } else {
                        if spec.sub_lengths.is_null() {
                            set_last_error("Missing sub_lengths for ListFloat64");
                            return ptr::null_mut();
                        }

                        let row_ptrs = spec.data as *const *const f64;
                        let lens_ptr = spec.sub_lengths;

                        let mut rows: Vec<Option<Series>> = Vec::with_capacity(spec.length as usize);
                        for i in 0..spec.length {
                            if !row_is_valid(spec.validity, i as usize) {
                                rows.push(None);
                                continue;
                            }

                            let len = *lens_ptr.add(i as usize);
                            if len == 0 {
                                rows.push(Some(Series::new("".into(), Vec::<f64>::new())));
                                continue;
                            }
                            let row_ptr = *row_ptrs.add(i as usize);
                            if row_ptr.is_null() {
                                rows.push(Some(Series::new("".into(), Vec::<f64>::new())));
                                continue;
                            }

                            let row_slice = std::slice::from_raw_parts(row_ptr, len as usize);
                            rows.push(Some(Series::new("".into(), row_slice)));
                        }

                        let mut lc = ListChunked::from_iter(rows);
                        lc.rename(name.into());
                        lc.into_series()
                    }
                }
                CColumnType::ListBool => {
                    if spec.length == 0 {
                        let mut lc = ListChunked::from_iter(std::iter::empty::<Series>());
                        lc.rename(name.into());
                        lc.into_series()
                    } else {
                        if spec.sub_lengths.is_null() {
                            set_last_error("Missing sub_lengths for ListBool");
                            return ptr::null_mut();
                        }

                        let row_ptrs = spec.data as *const *const u8;
                        let lens_ptr = spec.sub_lengths;

                        let mut rows: Vec<Option<Series>> = Vec::with_capacity(spec.length as usize);
                        for i in 0..spec.length {
                            if !row_is_valid(spec.validity, i as usize) {
                                rows.push(None);
                                continue;
                            }

                            let len = *lens_ptr.add(i as usize);
                            if len == 0 {
                                rows.push(Some(Series::new("".into(), Vec::<bool>::new())));
                                continue;
                            }
                            let row_ptr = *row_ptrs.add(i as usize);
                            if row_ptr.is_null() {
                                rows.push(Some(Series::new("".into(), Vec::<bool>::new())));
                                continue;
                            }

                            let row_slice = std::slice::from_raw_parts(row_ptr, len as usize);
                            let mut row_vec = Vec::with_capacity(len as usize);
                            for &b in row_slice {
                                row_vec.push(b != 0);
                            }
                            rows.push(Some(Series::new("".into(), row_vec)));
                        }

                        let mut lc = ListChunked::from_iter(rows);
                        lc.rename(name.into());
                        lc.into_series()
                    }
                }
                CColumnType::ListDatetimeMs => {
                    if spec.length == 0 {
                        let mut lc = ListChunked::from_iter(std::iter::empty::<Series>());
                        lc.rename(name.into());
                        lc.into_series()
                    } else {
                        if spec.sub_lengths.is_null() {
                            set_last_error("Missing sub_lengths for ListDatetimeMs");
                            return ptr::null_mut();
                        }

                        let row_ptrs = spec.data as *const *const i64;
                        let lens_ptr = spec.sub_lengths;

                        let mut rows: Vec<Option<Series>> = Vec::with_capacity(spec.length as usize);
                        for i in 0..spec.length {
                            if !row_is_valid(spec.validity, i as usize) {
                                rows.push(None);
                                continue;
                            }

                            let len = *lens_ptr.add(i as usize);
                            if len == 0 {
                                rows.push(Some(Series::new("".into(), Vec::<i64>::new())));
                                continue;
                            }
                            let row_ptr = *row_ptrs.add(i as usize);
                            if row_ptr.is_null() {
                                rows.push(Some(Series::new("".into(), Vec::<i64>::new())));
                                continue;
                            }

                            let row_slice = std::slice::from_raw_parts(row_ptr, len as usize);
                            rows.push(Some(Series::new("".into(), row_slice)));
                        }

                        let mut lc = ListChunked::from_iter(rows);
                        lc.rename(name.into());
                        let series = lc.into_series();

                        let target_dtype = DataType::List(Box::new(DataType::Datetime(TimeUnit::Milliseconds, None)));
                        match series.cast(&target_dtype) {
                            Ok(s) => s,
                            Err(e) => {
                                set_last_error(&format!("Failed to cast list datetime column: {}", e));
                                return ptr::null_mut();
                            }
                        }
                    }
                }
            };

            series_vec.push(series.into());
        }

        match DataFrame::new(series_vec) {
            Ok(df) => polars_df_to_c_df(df),
            Err(e) => {
                set_last_error(&format!("Error creating DataFrame: {}", e));
                ptr::null_mut()
            }
        }
    }
}

// Join type enum
#[repr(C)]
pub enum CJoinType {
    Inner = 0,
    Left = 1,
    Right = 2,
    Outer = 3,
}

impl From<CJoinType> for JoinType {
    fn from(join_type: CJoinType) -> Self {
        match join_type {
            CJoinType::Inner => JoinType::Inner,
            CJoinType::Left => JoinType::Left,
            CJoinType::Right => JoinType::Right,
            CJoinType::Outer => JoinType::Full,
        }
    }
}

#[no_mangle]
pub extern "C" fn join_dataframes(
    left_df: *mut CDataFrame,
    right_df: *mut CDataFrame,
    left_on: *const c_char,
    right_on: *const c_char,
    join_type: CJoinType,
) -> *mut CDataFrame {
    if left_df.is_null() || right_df.is_null() {
        set_last_error("DataFrame pointers cannot be null");
        return ptr::null_mut();
    }

    let left_on_str = unsafe {
        match CStr::from_ptr(left_on).to_str() {
            Ok(s) => s,
            Err(_) => {
                set_last_error("Invalid UTF-8 in left_on column name");
                return ptr::null_mut();
            }
        }
    };

    let right_on_str = unsafe {
        match CStr::from_ptr(right_on).to_str() {
            Ok(s) => s,
            Err(_) => {
                set_last_error("Invalid UTF-8 in right_on column name");
                return ptr::null_mut();
            }
        }
    };

    let left_df_rc = match unsafe { c_df_to_polars_df_ref(left_df) } {
        Ok(rc) => rc,
        Err(e) => {
            set_last_error(&format!("Failed to get left DataFrame: {}", e));
            return ptr::null_mut();
        }
    };

    let left_df_guard = match left_df_rc.try_borrow() {
        Ok(guard) => guard,
        Err(_) => {
            set_last_error("Failed to borrow left DataFrame");
            return ptr::null_mut();
        }
    };

    let right_df_rc = match unsafe { c_df_to_polars_df_ref(right_df) } {
        Ok(rc) => rc,
        Err(e) => {
            set_last_error(&format!("Failed to get right DataFrame: {}", e));
            return ptr::null_mut();
        }
    };

    let right_df_guard = match right_df_rc.try_borrow() {
        Ok(guard) => guard,
        Err(_) => {
            set_last_error("Failed to borrow right DataFrame");
            return ptr::null_mut();
        }
    };

    let join_result = left_df_guard
        .clone()
        .lazy()
        .join(
            right_df_guard.clone().lazy(),
            [col(left_on_str)],
            [col(right_on_str)],
            JoinArgs::new(join_type.into())
                .with_suffix(Some("_right".into()))
                .with_coalesce(JoinCoalesce::default()),
        )
        .collect();

    match join_result {
        Ok(result_df) => polars_df_to_c_df(result_df),
        Err(e) => {
            set_last_error(&format!("Join operation failed: {}", e));
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn join_dataframes_multiple_keys(
    left_df: *mut CDataFrame,
    right_df: *mut CDataFrame,
    left_on: *const c_char,
    right_on: *const c_char,
    join_type: CJoinType,
) -> *mut CDataFrame {
    if left_df.is_null() || right_df.is_null() {
        set_last_error("DataFrame pointers cannot be null");
        return ptr::null_mut();
    }

    let left_on_str = unsafe {
        match CStr::from_ptr(left_on).to_str() {
            Ok(s) => s,
            Err(_) => {
                set_last_error("Invalid UTF-8 in left_on column names");
                return ptr::null_mut();
            }
        }
    };

    let right_on_str = unsafe {
        match CStr::from_ptr(right_on).to_str() {
            Ok(s) => s,
            Err(_) => {
                set_last_error("Invalid UTF-8 in right_on column names");
                return ptr::null_mut();
            }
        }
    };

    // Parse comma-separated column names
    let left_cols: Vec<&str> = left_on_str.split(',').map(|s| s.trim()).collect();
    let right_cols: Vec<&str> = right_on_str.split(',').map(|s| s.trim()).collect();

    if left_cols.len() != right_cols.len() {
        set_last_error("Number of left_on and right_on columns must match");
        return ptr::null_mut();
    }

    let left_df_rc = match unsafe { c_df_to_polars_df_ref(left_df) } {
        Ok(rc) => rc,
        Err(e) => {
            set_last_error(&format!("Failed to get left DataFrame: {}", e));
            return ptr::null_mut();
        }
    };

    let left_df_guard = match left_df_rc.try_borrow() {
        Ok(guard) => guard,
        Err(_) => {
            set_last_error("Failed to borrow left DataFrame");
            return ptr::null_mut();
        }
    };

    let right_df_rc = match unsafe { c_df_to_polars_df_ref(right_df) } {
        Ok(rc) => rc,
        Err(e) => {
            set_last_error(&format!("Failed to get right DataFrame: {}", e));
            return ptr::null_mut();
        }
    };

    let right_df_guard = match right_df_rc.try_borrow() {
        Ok(guard) => guard,
        Err(_) => {
            set_last_error("Failed to borrow right DataFrame");
            return ptr::null_mut();
        }
    };

    let left_exprs: Vec<Expr> = left_cols.iter().map(|&col_name| col(col_name)).collect();
    let right_exprs: Vec<Expr> = right_cols.iter().map(|&col_name| col(col_name)).collect();

    let join_result = left_df_guard
        .clone()
        .lazy()
        .join(
            right_df_guard.clone().lazy(),
            left_exprs,
            right_exprs,
            JoinArgs::new(join_type.into())
                .with_suffix(Some("_right".into()))
                .with_coalesce(JoinCoalesce::default()),
        )
        .collect();

    match join_result {
        Ok(result_df) => polars_df_to_c_df(result_df),
        Err(e) => {
            set_last_error(&format!("Join operation failed: {}", e));
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn concat_df_vertical(
    dfs_ptr: *const *mut CDataFrame,
    dfs_len: c_int,
    rechunk: bool,
) -> *mut CDataFrame {
    if dfs_ptr.is_null() || dfs_len <= 0 {
        set_last_error("No DataFrames provided for vertical concat");
        return ptr::null_mut();
    }

    let dfs_slice = unsafe { std::slice::from_raw_parts(dfs_ptr, dfs_len as usize) };

    let mut dfs = Vec::with_capacity(dfs_slice.len());
    for &df_ptr in dfs_slice {
        if df_ptr.is_null() {
            set_last_error("Encountered null DataFrame pointer in concat");
            return ptr::null_mut();
        }
        match unsafe { c_df_to_polars_df_ref(df_ptr) } {
            Ok(rc_df) => {
                if let Ok(guard) = rc_df.try_borrow() {
                    dfs.push(guard.clone());
                } else {
                    set_last_error("Failed to borrow DataFrame for concat");
                    return ptr::null_mut();
                }
            }
            Err(e) => {
                set_last_error(&format!("Failed to convert DataFrame: {}", e));
                return ptr::null_mut();
            }
        }
    }

    if dfs.is_empty() {
        set_last_error("No DataFrames provided for vertical concat");
        return ptr::null_mut();
    }

    let mut acc = dfs[0].clone();
    for df in dfs.iter().skip(1) {
        if let Err(e) = acc.vstack_mut(df) {
            set_last_error(&format!("Concat vertical error: {}", e));
            return ptr::null_mut();
        }
    }

    if rechunk {
        acc.as_single_chunk();
    }

    polars_df_to_c_df(acc)
}

#[no_mangle]
pub extern "C" fn concat_df_horizontal(
    dfs_ptr: *const *mut CDataFrame,
    dfs_len: c_int,
    rechunk: bool,
) -> *mut CDataFrame {
    if dfs_ptr.is_null() || dfs_len <= 0 {
        set_last_error("No DataFrames provided for horizontal concat");
        return ptr::null_mut();
    }

    let dfs_slice = unsafe { std::slice::from_raw_parts(dfs_ptr, dfs_len as usize) };

    let mut dfs = Vec::with_capacity(dfs_slice.len());
    for &df_ptr in dfs_slice {
        if df_ptr.is_null() {
            set_last_error("Encountered null DataFrame pointer in concat");
            return ptr::null_mut();
        }
        match unsafe { c_df_to_polars_df_ref(df_ptr) } {
            Ok(rc_df) => {
                if let Ok(guard) = rc_df.try_borrow() {
                    dfs.push(guard.clone());
                } else {
                    set_last_error("Failed to borrow DataFrame for concat");
                    return ptr::null_mut();
                }
            }
            Err(e) => {
                set_last_error(&format!("Failed to convert DataFrame: {}", e));
                return ptr::null_mut();
            }
        }
    }

    if dfs.is_empty() {
        set_last_error("No DataFrames provided for horizontal concat");
        return ptr::null_mut();
    }

    let mut acc = dfs[0].clone();
    for df in dfs.iter().skip(1) {
        let cols: Vec<Column> = df.get_columns().to_vec();
        if let Err(e) = acc.hstack_mut(&cols) {
            set_last_error(&format!("Concat horizontal error: {}", e));
            return ptr::null_mut();
        }
    }

    if rechunk {
        acc.as_single_chunk();
    }

    polars_df_to_c_df(acc)
}

#[no_mangle]
pub extern "C" fn scan_csv(path_ptr: *const c_char) -> *mut CLazyFrame {
    if path_ptr.is_null() {
        set_last_error("Path pointer is null");
        return ptr::null_mut();
    }

    let path = unsafe {
        match CStr::from_ptr(path_ptr).to_str() {
            Ok(p) => p,
            Err(_) => {
                set_last_error("Invalid UTF-8 in path");
                return ptr::null_mut();
            }
        }
    };

    match LazyCsvReader::new(path).finish() {
        Ok(lf) => lazyframe_to_c_lazyframe(lf),
        Err(e) => {
            set_last_error(&format!("scan_csv error: {}", e));
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn scan_csv_with_schema(
    path_ptr: *const c_char,
    override_names_ptr: *const *const c_char,
    override_dtypes_ptr: *const i32,
    override_len: c_int,
    schema_names_ptr: *const *const c_char,
    schema_dtypes_ptr: *const i32,
    schema_len: c_int,
    infer_schema_length: i64,
    ignore_errors: bool,
    truncate_ragged_lines: bool,
    has_header: bool,
) -> *mut CLazyFrame {
    if path_ptr.is_null() {
        set_last_error("Path pointer is null");
        return ptr::null_mut();
    }

    let path = unsafe {
        match CStr::from_ptr(path_ptr).to_str() {
            Ok(p) => p,
            Err(_) => {
                set_last_error("Invalid UTF-8 in path");
                return ptr::null_mut();
            }
        }
    };

    let mut reader = LazyCsvReader::new(path).with_has_header(has_header);

    // Overrides
    if override_len < 0 {
        set_last_error("Override length cannot be negative");
        return ptr::null_mut();
    }
    let override_len = override_len as usize;
    if override_len > 0 {
        if override_names_ptr.is_null() || override_dtypes_ptr.is_null() {
            set_last_error("Override column names or dtypes pointer is null");
            return ptr::null_mut();
        }

        let names_slice = unsafe { std::slice::from_raw_parts(override_names_ptr, override_len) };
        let dtype_slice = unsafe { std::slice::from_raw_parts(override_dtypes_ptr, override_len) };

        let mut schema = Schema::with_capacity(override_len);

        for i in 0..override_len {
            let name_ptr = names_slice[i];
            if name_ptr.is_null() {
                set_last_error("Null column name pointer provided (override)");
                return ptr::null_mut();
            }

            let name = unsafe {
                match CStr::from_ptr(name_ptr).to_str() {
                    Ok(s) => s,
                    Err(_) => {
                        set_last_error("Invalid UTF-8 in column name (override)");
                        return ptr::null_mut();
                    }
                }
            };

            let dtype_tag = dtype_slice[i];
            let dtype = match dtype_from_c_tag(dtype_tag) {
                Some(dt) => dt,
                None => {
                    set_last_error("Invalid dtype tag provided (override)");
                    return ptr::null_mut();
                }
            };

            schema.with_column(name.to_string().into(), dtype);
        }

        reader = reader.with_schema(Some(Arc::new(schema)));
    }

    // Full schema
    if schema_len < 0 {
        set_last_error("Schema length cannot be negative");
        return ptr::null_mut();
    }
    let schema_len = schema_len as usize;
    if schema_len > 0 {
        if schema_names_ptr.is_null() || schema_dtypes_ptr.is_null() {
            set_last_error("Schema column names or dtypes pointer is null");
            return ptr::null_mut();
        }

        let names_slice = unsafe { std::slice::from_raw_parts(schema_names_ptr, schema_len) };
        let dtype_slice = unsafe { std::slice::from_raw_parts(schema_dtypes_ptr, schema_len) };

        let mut schema = Schema::with_capacity(schema_len);

        for i in 0..schema_len {
            let name_ptr = names_slice[i];
            if name_ptr.is_null() {
                set_last_error("Null column name pointer provided (schema)");
                return ptr::null_mut();
            }

            let name = unsafe {
                match CStr::from_ptr(name_ptr).to_str() {
                    Ok(s) => s,
                    Err(_) => {
                        set_last_error("Invalid UTF-8 in column name (schema)");
                        return ptr::null_mut();
                    }
                }
            };

            let dtype_tag = dtype_slice[i];
            let dtype = match dtype_from_c_tag(dtype_tag) {
                Some(dt) => dt,
                None => {
                    set_last_error("Invalid dtype tag provided (schema)");
                    return ptr::null_mut();
                }
            };

            schema.with_column(name.to_string().into(), dtype);
        }

        reader = reader.with_schema(Some(Arc::new(schema)));
    }

    if infer_schema_length >= 0 {
        reader = reader.with_infer_schema_length(Some(infer_schema_length as usize));
    }

    if ignore_errors {
        reader = reader.with_ignore_errors(true);
    }

    if truncate_ragged_lines {
        reader = reader.with_truncate_ragged_lines(true);
    }

    match reader.finish() {
        Ok(lf) => lazyframe_to_c_lazyframe(lf),
        Err(e) => {
            set_last_error(&format!("scan_csv_with_schema error: {}", e));
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn scan_parquet(path_ptr: *const c_char) -> *mut CLazyFrame {
    if path_ptr.is_null() {
        set_last_error("Path pointer is null");
        return ptr::null_mut();
    }

    let path = unsafe {
        match CStr::from_ptr(path_ptr).to_str() {
            Ok(p) => p,
            Err(_) => {
                set_last_error("Invalid UTF-8 in path");
                return ptr::null_mut();
            }
        }
    };

    match LazyFrame::scan_parquet(path, ScanArgsParquet::default()) {
        Ok(lf) => lazyframe_to_c_lazyframe(lf),
        Err(e) => {
            set_last_error(&format!("scan_parquet error: {}", e));
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn scan_ndjson(path_ptr: *const c_char) -> *mut CLazyFrame {
    if path_ptr.is_null() {
        set_last_error("Path pointer is null");
        return ptr::null_mut();
    }

    let path = unsafe {
        match CStr::from_ptr(path_ptr).to_str() {
            Ok(p) => p,
            Err(_) => {
                set_last_error("Invalid UTF-8 in path");
                return ptr::null_mut();
            }
        }
    };

    match LazyJsonLineReader::new(path).finish() {
        Ok(lf) => lazyframe_to_c_lazyframe(lf),
        Err(e) => {
            set_last_error(&format!("scan_ndjson error: {}", e));
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn dataframe_lazy(df_ptr: *mut CDataFrame) -> *mut CLazyFrame {
    if df_ptr.is_null() {
        set_last_error("DataFrame pointer is null");
        return ptr::null_mut();
    }

    let df_rc = match unsafe { c_df_to_polars_df(df_ptr) } {
        Ok(rc) => rc,
        Err(e) => {
            set_last_error(&format!("Failed to convert DataFrame: {}", e));
            return ptr::null_mut();
        }
    };

    let lf = {
        match df_rc.try_borrow() {
            Ok(df) => df.clone().lazy(),
            Err(_) => {
                set_last_error("Failed to borrow DataFrame for lazy conversion");
                return ptr::null_mut();
            }
        }
    };

    lazyframe_to_c_lazyframe(lf)
}

#[no_mangle]
pub extern "C" fn lazy_collect(lf_ptr: *mut CLazyFrame, streaming: bool) -> *mut CDataFrame {
    if lf_ptr.is_null() {
        set_last_error("LazyFrame pointer is null");
        return ptr::null_mut();
    }

    let lf_clone = match unsafe { c_lazyframe_to_lazyframe(lf_ptr) } {
        Ok(lf) => lf,
        Err(e) => {
            set_last_error(&e);
            return ptr::null_mut();
        }
    };

    let result = lf_clone.with_streaming(streaming).collect();

    match result {
        Ok(df) => polars_df_to_c_df(df),
        Err(e) => {
            set_last_error(&format!("Lazy collect error: {}", e));
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn free_lazyframe(lf_ptr: *mut CLazyFrame) {
    unsafe {
        if lf_ptr.is_null() {
            return;
        }
        let c_lf = Box::from_raw(lf_ptr);
        if !c_lf.inner.is_null() {
            drop(Box::from_raw(c_lf.inner as *mut LazyFrame));
        }
        // c_lf dropped here
    }
}
