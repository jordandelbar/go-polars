use polars::prelude::*;
use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::fs::File;
use std::os::raw::{c_char, c_int};
use std::ptr;
use std::rc::Rc;
use std::sync::Mutex;

lazy_static::lazy_static! {
    static ref LAST_ERROR: Mutex<Option<String>> = Mutex::new(None);
}

fn set_last_error(err: &str) {
    *LAST_ERROR.lock().unwrap() = Some(err.to_string());
}

#[no_mangle]
fn get_last_error_message() -> *const c_char {
    let error = LAST_ERROR.lock().unwrap();
    match &*error {
        Some(msg) => CString::new(msg.clone()).unwrap().into_raw(),
        None => ptr::null(),
    }
}

mod conversions {
    use super::*;

    #[repr(C)]
    pub struct CDataFrame {
        pub inner: *mut std::ffi::c_void,
    }

    #[repr(C)]
    pub struct CExpr {
        pub inner: *mut std::ffi::c_void,
    }

    #[repr(C)]
    pub struct CGroupBy {
        pub inner: *mut std::ffi::c_void,
    }

    pub fn polars_df_to_c_df(df: DataFrame) -> *mut CDataFrame {
        let rc_df = Rc::new(RefCell::new(df));
        let boxed_df = Box::new(rc_df);
        let inner = Box::into_raw(boxed_df) as *mut std::ffi::c_void;
        let c_df = CDataFrame { inner };
        Box::into_raw(Box::new(c_df))
    }

    pub unsafe fn c_df_to_polars_df(
        c_df: *mut CDataFrame,
    ) -> Result<Rc<RefCell<DataFrame>>, String> {
        if c_df.is_null() || (*c_df).inner.is_null() {
            return Err("CDataFrame or inner pointer is null".to_string());
        }
        let rc_df_ptr = (*c_df).inner as *mut Rc<RefCell<DataFrame>>;
        Ok(Rc::clone(&*rc_df_ptr))
    }

    pub unsafe fn c_df_to_polars_df_ref(
        c_df: *const CDataFrame,
    ) -> Result<Rc<RefCell<DataFrame>>, String> {
        if c_df.is_null() || (*c_df).inner.is_null() {
            return Err("CDataFrame or inner pointer is null".to_string());
        }
        let rc_df_ptr = (*c_df).inner as *mut Rc<RefCell<DataFrame>>;
        Ok(Rc::clone(&*rc_df_ptr))
    }

    pub fn expr_to_c_expr(expr: Expr) -> *mut CExpr {
        let boxed_expr = Box::new(expr);
        let ptr = Box::into_raw(boxed_expr) as *mut std::ffi::c_void;
        let c_expr = CExpr { inner: ptr };
        Box::into_raw(Box::new(c_expr))
    }

    pub unsafe fn c_expr_to_expr(c_expr: *mut CExpr) -> Result<Expr, String> {
        if c_expr.is_null() || (*c_expr).inner.is_null() {
            return Err("CExpr or inner pointer is null".to_string());
        }
        let c_expr_struct = Box::from_raw(c_expr);
        let expr_ptr = c_expr_struct.inner as *mut Expr;
        let expr = *Box::from_raw(expr_ptr);
        Ok(expr)
    }

    pub fn groupby_to_c_groupby(gb: LazyGroupBy) -> *mut CGroupBy {
        let boxed_gb = Box::new(gb);
        let inner = Box::into_raw(boxed_gb) as *mut std::ffi::c_void;
        let c_gb = CGroupBy { inner };
        Box::into_raw(Box::new(c_gb))
    }

    #[allow(dead_code)]
    pub unsafe fn c_groupby_to_groupby(c_gb: *mut CGroupBy) -> Result<GroupBy<'static>, String> {
        if c_gb.is_null() || (*c_gb).inner.is_null() {
            return Err("CGroupBy or inner pointer is null".to_string());
        }
        let c_gb_struct = Box::from_raw(c_gb);
        let gb_ptr = c_gb_struct.inner as *mut GroupBy;
        let gb = *Box::from_raw(gb_ptr);
        Ok(gb)
    }
}

use conversions::*;

mod dataframe_functions {
    use super::*;
    use polars::prelude::{CsvReadOptions, CsvReader, CsvWriter};

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
    pub extern "C" fn write_csv(
        df_ptr: *mut CDataFrame,
        file_path: *const c_char,
    ) -> *const c_char {
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
                                Ok(_) => {
                                    CString::new("CSV written successfully").unwrap().into_raw()
                                }
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
                            *LAST_ERROR.lock().unwrap() =
                                Some(format!("Error creating file: {}", e));
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
                            *LAST_ERROR.lock().unwrap() =
                                Some(format!("Error with_columns: {}", e));
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
}

use dataframe_functions::*;

mod expr_functions {
    use super::*;

    #[no_mangle]
    pub extern "C" fn col(name: *const c_char) -> *mut CExpr {
        let name_str = unsafe { CStr::from_ptr(name).to_str().unwrap_or_default() };
        let expr = polars::prelude::col(name_str);
        expr_to_c_expr(expr)
    }

    #[no_mangle]
    pub extern "C" fn col_gt(expr_ptr: *mut CExpr, value: i64) -> *mut CExpr {
        unsafe {
            let expr_result = c_expr_to_expr(expr_ptr);
            match expr_result {
                Ok(expr) => {
                    let new_expr = expr.clone().gt(lit(value));
                    expr_to_c_expr(new_expr)
                }
                Err(_) => ptr::null_mut(),
            }
        }
    }

    #[no_mangle]
    pub extern "C" fn free_expr(expr: *mut CExpr) {
        unsafe {
            if expr.is_null() {
                return;
            }

            let _expr = Box::from_raw(expr);
        }
    }

    #[no_mangle]
    pub extern "C" fn expr_alias(c_expr: *mut CExpr, alias: *const c_char) -> *mut CExpr {
        unsafe {
            let expr = match c_expr_to_expr(c_expr) {
                Ok(expr) => expr,
                Err(e) => {
                    *LAST_ERROR.lock().unwrap() = Some(e);
                    return std::ptr::null_mut();
                }
            };

            let alias_str = CStr::from_ptr(alias).to_str().unwrap_or_default();
            let aliased_expr = expr.alias(alias_str);
            expr_to_c_expr(aliased_expr)
        }
    }

    #[no_mangle]
    pub extern "C" fn lit_int64(val: i64) -> *mut CExpr {
        expr_to_c_expr(lit(val))
    }

    #[no_mangle]
    pub extern "C" fn lit_int32(val: i32) -> *mut CExpr {
        expr_to_c_expr(lit(val))
    }

    #[no_mangle]
    pub extern "C" fn lit_float64(val: f64) -> *mut CExpr {
        expr_to_c_expr(lit(val))
    }

    #[no_mangle]
    pub extern "C" fn lit_float32(val: f32) -> *mut CExpr {
        expr_to_c_expr(lit(val))
    }

    #[no_mangle]
    pub extern "C" fn lit_string(val: *const c_char) -> *mut CExpr {
        let val_str = unsafe { CStr::from_ptr(val).to_str().unwrap_or_default() };
        expr_to_c_expr(lit(val_str))
    }

    #[no_mangle]
    pub extern "C" fn lit_bool(val: u8) -> *mut CExpr {
        expr_to_c_expr(lit(val != 0))
    }
}

use expr_functions::*;

mod groupby_functions {
    use super::*;

    #[no_mangle]
    pub extern "C" fn group_by(
        df_ptr: *mut CDataFrame,
        columns_ptr: *const c_char,
    ) -> *mut CGroupBy {
        unsafe {
            match c_df_to_polars_df(df_ptr) {
                Ok(rc_df) => {
                    let df = rc_df.borrow();
                    let columns_str = CStr::from_ptr(columns_ptr).to_str().unwrap();
                    let columns: Vec<&str> = columns_str.split(',').collect();

                    let lazy_df = df.clone().lazy();

                    let lazy_group_by = lazy_df.group_by(columns); // Get LazyGroupBy

                    groupby_to_c_groupby(lazy_group_by) // Pass LazyGroupBy directly
                }
                Err(e) => {
                    *LAST_ERROR.lock().unwrap() = Some(format!("Group by error: {}", e));
                    ptr::null_mut()
                }
            }
        }
    }

    #[no_mangle]
    pub extern "C" fn free_groupby(groupby: *mut CGroupBy) {
        unsafe {
            if groupby.is_null() {
                return;
            }
            let _groupby = Box::from_raw(groupby);
        }
    }
}

use groupby_functions::*;
