use crate::conversions::*;
use crate::set_last_error;
use polars::prelude::*;
use std::cell::RefCell;
use std::ffi::{c_int, CStr, CString};
use std::fs::File;
use std::os::raw::c_char;
use std::ptr;
use std::rc::Rc;

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

// Column type enum for mixed DataFrame creation
#[repr(C)]
pub enum CColumnType {
    String = 0,
    Int64 = 1,
    Float64 = 2,
    Bool = 3,
}

#[repr(C)]
pub struct CColumnSpec {
    name: *const c_char,
    column_type: CColumnType,
    data: *const std::ffi::c_void,
    length: c_int,
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
                    let mut values = Vec::new();
                    if spec.length == 0 {
                        // Empty column
                        let empty_values: Vec<Option<String>> = Vec::new();
                        Series::new(name.into(), empty_values)
                    } else {
                        let string_ptrs = spec.data as *const *const c_char;
                        for j in 0..spec.length {
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
                        let empty_values: Vec<i64> = Vec::new();
                        Series::new(name.into(), empty_values)
                    } else {
                        let int_data = spec.data as *const i64;
                        let values: Vec<i64> =
                            std::slice::from_raw_parts(int_data, spec.length as usize).to_vec();
                        Series::new(name.into(), values)
                    }
                }
                CColumnType::Float64 => {
                    if spec.length == 0 {
                        let empty_values: Vec<f64> = Vec::new();
                        Series::new(name.into(), empty_values)
                    } else {
                        let float_data = spec.data as *const f64;
                        let values: Vec<f64> =
                            std::slice::from_raw_parts(float_data, spec.length as usize).to_vec();
                        Series::new(name.into(), values)
                    }
                }
                CColumnType::Bool => {
                    if spec.length == 0 {
                        let empty_values: Vec<bool> = Vec::new();
                        Series::new(name.into(), empty_values)
                    } else {
                        let bool_data = spec.data as *const u8;
                        let mut values = Vec::new();
                        for j in 0..spec.length {
                            let bool_val = *bool_data.add(j as usize) != 0;
                            values.push(bool_val);
                        }
                        Series::new(name.into(), values)
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
