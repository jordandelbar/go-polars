use polars::prelude::*;
use std::ffi::{CStr, CString};
use std::fs::File;
use std::os::raw::c_char;
use std::ptr;
use std::sync::Mutex;

lazy_static::lazy_static! {
    static ref LAST_ERROR: Mutex<Option<String>> = Mutex::new(None);
}

#[repr(C)]
pub struct CDataFrame {
    pub handle: *mut DataFrame,
}

#[repr(C)]
pub struct CExpr {
    pub handle: *mut Expr,
}

#[repr(C)]
pub struct CGroupBy<'a> {
    pub handle: *mut GroupBy<'a>,
}

// --- Conversion Functions ---

fn polars_df_to_c_df(df: DataFrame) -> *mut CDataFrame {
    let boxed_df = Box::new(df);
    let handle = Box::into_raw(boxed_df);
    let c_df = CDataFrame { handle };
    Box::into_raw(Box::new(c_df))
}

unsafe fn c_df_to_polars_df(c_df: *mut CDataFrame) -> Result<&'static DataFrame, &'static str> {
    if c_df.is_null() {
        return Err("DataFrame pointer is null");
    }
    Ok(&*(*c_df).handle)
}

unsafe fn c_df_to_polars_df_mut(
    c_df: *mut CDataFrame,
) -> Result<&'static mut DataFrame, &'static str> {
    if c_df.is_null() {
        return Err("DataFrame pointer is null");
    }
    Ok(&mut *(*c_df).handle)
}

fn expr_to_c_expr(expr: Expr) -> *mut CExpr {
    let boxed_expr = Box::new(expr);
    let handle = Box::into_raw(boxed_expr);
    let c_expr = CExpr { handle };
    Box::into_raw(Box::new(c_expr))
}

unsafe fn c_expr_to_expr(c_expr: *mut CExpr) -> Result<&'static Expr, &'static str> {
    if c_expr.is_null() {
        return Err("Expr pointer is null");
    }
    Ok(&*(*c_expr).handle)
}

fn groupby_to_c_groupby(gb: GroupBy) -> *mut CGroupBy {
    let boxed_gb = Box::new(gb);
    let handle = Box::into_raw(boxed_gb);
    let c_gb = CGroupBy { handle };
    Box::into_raw(Box::new(c_gb))
}

#[allow(dead_code)]
unsafe fn c_groupby_to_groupby(c_gb: *mut CGroupBy) -> Result<&'static GroupBy, &'static str> {
    if c_gb.is_null() {
        return Err("GroupBy pointer is null");
    }
    Ok(&*(*c_gb).handle)
}

// --- Exported Functions ---

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
        let _df = Box::from_raw(c_df.handle);
    }
}

#[no_mangle]
pub extern "C" fn dataframe_width(df: *const CDataFrame) -> usize {
    unsafe {
        match c_df_to_polars_df(df as *mut CDataFrame) {
            Ok(df) => df.width(),
            Err(_) => 0,
        }
    }
}

#[no_mangle]
pub extern "C" fn dataframe_height(df: *const CDataFrame) -> usize {
    unsafe {
        match c_df_to_polars_df(df as *mut CDataFrame) {
            Ok(df) => df.height(),
            Err(_) => 0,
        }
    }
}

#[no_mangle]
pub extern "C" fn dataframe_column_name(df: *const CDataFrame, index: usize) -> *const c_char {
    unsafe {
        match c_df_to_polars_df(df as *mut CDataFrame) {
            Ok(df) => {
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
        let df_result = c_df_to_polars_df_mut(df_ptr);
        let expr_result = c_expr_to_expr(expr_ptr);

        match (df_result, expr_result) {
            (Ok(df), Ok(expr)) => {
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
pub extern "C" fn group_by(
    df_ptr: *mut CDataFrame,
    columns_ptr: *const c_char,
) -> *mut CGroupBy<'static> {
    unsafe {
        let df_result = c_df_to_polars_df_mut(df_ptr);

        match df_result {
            Ok(df) => {
                let columns_str = CStr::from_ptr(columns_ptr).to_str().unwrap();
                let columns: Vec<&str> = columns_str.split(',').collect();
                let groupby = df.group_by(columns).unwrap();
                groupby_to_c_groupby(groupby)
            }
            Err(e) => {
                *LAST_ERROR.lock().unwrap() = Some(format!("Group by error: {}", e));
                ptr::null_mut()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn columns(df_ptr: *mut CDataFrame) -> *const c_char {
    unsafe {
        let df_result = c_df_to_polars_df(df_ptr);
        match df_result {
            Ok(df) => {
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
pub extern "C" fn print_dataframe(df_ptr: *mut CDataFrame) -> *const c_char {
    unsafe {
        let df_result = c_df_to_polars_df(df_ptr);
        match df_result {
            Ok(df) => {
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
pub extern "C" fn get_last_error() -> *const c_char {
    let error = LAST_ERROR.lock().unwrap();
    match &*error {
        Some(msg) => CString::new(msg.clone()).unwrap().into_raw(),
        None => ptr::null(),
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
pub extern "C" fn free_groupby(groupby: *mut CGroupBy) {
    unsafe {
        if groupby.is_null() {
            return;
        }
        let _groupby = Box::from_raw(groupby);
    }
}

#[no_mangle]
pub extern "C" fn head(df_ptr: *mut CDataFrame, n: usize) -> *const c_char {
    unsafe {
        let df_result = c_df_to_polars_df(df_ptr);
        match df_result {
            Ok(df) => {
                let head_df = df.head(Some(n));
                let df_str = format!("{:?}", head_df);
                match CString::new(df_str) {
                    Ok(c_string) => c_string.into_raw(),
                    Err(e) => {
                        *LAST_ERROR.lock().unwrap() =
                            Some(format!("Error converting head to string: {}", e));
                        ptr::null()
                    }
                }
            }
            Err(e) => {
                *LAST_ERROR.lock().unwrap() = Some(format!("Error getting head: {}", e));
                ptr::null()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn write_csv(df_ptr: *mut CDataFrame, file_path: *const c_char) -> *const c_char {
    unsafe {
        let df_result = c_df_to_polars_df_mut(df_ptr);
        match df_result {
            Ok(df) => {
                let path_str = CStr::from_ptr(file_path).to_str().unwrap();
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
