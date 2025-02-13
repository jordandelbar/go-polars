use crate::conversions::*;
use crate::{set_last_error, LAST_ERROR};
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
            *LAST_ERROR.lock().unwrap() = Some("Invalid UTF-8 path".to_string());
            return ptr::null_mut();
        }
    };

    match CsvReadOptions::default()
        .try_into_reader_with_file_path(Some(path_str.into()))
        .and_then(|reader| reader.finish())
    {
        Ok(df) => polars_df_to_c_df(df),
        Err(e) => {
            *LAST_ERROR.lock().unwrap() = Some(format!("Failed to read CSV: {}", e));
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
        drop(Box::from_raw(c_df.inner as *mut Rc<RefCell<DataFrame>>));
        drop(c_df);
    }
}

#[no_mangle]
pub extern "C" fn print_dataframe(df_ptr: *mut CDataFrame) -> *const c_char {
    unsafe {
        let df_result = c_df_to_polars_df(df_ptr);
        match df_result {
            Ok(rc_df) => {
                let df = rc_df.borrow();
                let df_str = format!("{}", df);
                CString::new(df_str).unwrap().into_raw()
            }
            Err(e) => {
                *LAST_ERROR.lock().unwrap() = Some(format!("Print DataFrame error: {}", e));
                ptr::null()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn dataframe_width(df: *const CDataFrame) -> usize {
    unsafe {
        match c_df_to_polars_df_ref(df) {
            Ok(rc_df) => {
                let df = rc_df.borrow();
                df.width()
            }
            Err(_) => 0,
        }
    }
}

#[no_mangle]
pub extern "C" fn dataframe_height(df: *const CDataFrame) -> usize {
    unsafe {
        match c_df_to_polars_df_ref(df) {
            Ok(rc_df) => {
                let df = rc_df.borrow();
                df.height()
            }
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
                *LAST_ERROR.lock().unwrap() = Some(format!("Columns error: {}", e));
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
                    let name = names[index];
                    match CString::new(name.as_str()) {
                        Ok(c_string) => c_string.into_raw(),
                        Err(e) => {
                            *LAST_ERROR.lock().unwrap() =
                                Some(format!("Error converting column name: {}", e));
                            ptr::null()
                        }
                    }
                } else {
                    ptr::null()
                }
            }
            Err(e) => {
                *LAST_ERROR.lock().unwrap() = Some(format!("Error getting column name: {}", e));
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
                let filtered_df = match df.clone().lazy().filter(expr.clone()).collect() {
                    Ok(df) => df,
                    Err(_) => return std::ptr::null_mut(),
                };

                polars_df_to_c_df(filtered_df)
            }
            _ => ptr::null_mut(),
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
                *LAST_ERROR.lock().unwrap() = Some(format!("Error getting head: {}", e));
                ptr::null_mut()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn write_csv(df_ptr: *mut CDataFrame, file_path: *const c_char) -> *const c_char {
    unsafe {
        let df_result = c_df_to_polars_df(df_ptr);
        match df_result {
            Ok(rc_df) => {
                let path_str = CStr::from_ptr(file_path).to_str().unwrap();
                let df = rc_df.borrow_mut();
                let mut df_clone = df.clone();

                match File::create(path_str) {
                    Ok(mut file) => {
                        let mut writer = CsvWriter::new(&mut file);
                        match writer.finish(&mut df_clone) {
                            Ok(_) => CString::new("CSV written successfully").unwrap().into_raw(),
                            Err(e) => {
                                *LAST_ERROR.lock().unwrap() =
                                    Some(format!("Error writing CSV: {}", e));
                                CString::new(format!("Error writing CSV: {}", e))
                                    .unwrap()
                                    .into_raw()
                            }
                        }
                    }
                    Err(e) => {
                        *LAST_ERROR.lock().unwrap() = Some(format!("Error creating file: {}", e));
                        CString::new(format!("Error creating file: {}", e))
                            .unwrap()
                            .into_raw()
                    }
                }
            }
            Err(e) => {
                *LAST_ERROR.lock().unwrap() = Some(format!("Error in write_csv: {}", e));
                CString::new(format!("Error in write_csv: {}", e))
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
                let mut exprs: Vec<Expr> = Vec::new();
                let exprs_slice = std::slice::from_raw_parts(exprs_ptr, exprs_len as usize);

                for &expr_ptr in exprs_slice {
                    match c_expr_to_expr(expr_ptr) {
                        Ok(expr) => exprs.push(expr.clone()),
                        Err(e) => {
                            *LAST_ERROR.lock().unwrap() =
                                Some(format!("Error converting expr: {}", e));
                            return ptr::null_mut();
                        }
                    }
                }

                let df = rc_df.borrow();
                let mut lazy_df = df.clone().lazy();
                for expr in exprs {
                    lazy_df = lazy_df.with_column(expr);
                }

                let new_df = match lazy_df.collect() {
                    Ok(df) => df,
                    Err(e) => {
                        *LAST_ERROR.lock().unwrap() = Some(format!("Error with_columns: {}", e));
                        return ptr::null_mut();
                    }
                };
                polars_df_to_c_df(new_df)
            }
            Err(e) => {
                *LAST_ERROR.lock().unwrap() = Some(format!("Error in with_columns: {}", e));
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
