use crate::conversions::*;
use crate::LAST_ERROR;
use polars::prelude::*;
use std::ffi::{c_char, CStr};
use std::ptr;

#[no_mangle]
pub extern "C" fn group_by(df_ptr: *mut CDataFrame, columns_ptr: *const c_char) -> *mut CGroupBy {
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

#[no_mangle]
pub extern "C" fn groupby_agg(
    groupby_ptr: *mut CGroupBy,
    exprs_ptr: *mut *mut CExpr,
    exprs_len: i32,
) -> *mut CDataFrame {
    unsafe {
        if groupby_ptr.is_null() || (*groupby_ptr).inner.is_null() {
            *LAST_ERROR.lock().unwrap() = Some("GroupBy pointer is null".to_string());
            return ptr::null_mut();
        }

        let gb_ptr = (*groupby_ptr).inner as *mut LazyGroupBy;
        let lazy_groupby = Box::from_raw(gb_ptr);

        let mut exprs = Vec::new();
        for i in 0..exprs_len {
            let expr_ptr = *exprs_ptr.add(i as usize);
            match c_expr_to_expr(expr_ptr) {
                Ok(expr) => exprs.push(expr),
                Err(e) => {
                    *LAST_ERROR.lock().unwrap() =
                        Some(format!("Error converting expression: {}", e));
                    return ptr::null_mut();
                }
            }
        }

        match lazy_groupby.agg(exprs).collect() {
            Ok(df) => polars_df_to_c_df(df),
            Err(e) => {
                *LAST_ERROR.lock().unwrap() = Some(format!("Aggregation error: {}", e));
                ptr::null_mut()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn groupby_sum(
    groupby_ptr: *mut CGroupBy,
    column_ptr: *const c_char,
) -> *mut CDataFrame {
    unsafe {
        if groupby_ptr.is_null() || (*groupby_ptr).inner.is_null() {
            *LAST_ERROR.lock().unwrap() = Some("GroupBy pointer is null".to_string());
            return ptr::null_mut();
        }

        let gb_ptr = (*groupby_ptr).inner as *mut LazyGroupBy;
        let lazy_groupby = Box::from_raw(gb_ptr);

        let column_str = CStr::from_ptr(column_ptr).to_str().unwrap();

        match lazy_groupby.agg([col(column_str).sum()]).collect() {
            Ok(df) => polars_df_to_c_df(df),
            Err(e) => {
                *LAST_ERROR.lock().unwrap() = Some(format!("Sum error: {}", e));
                ptr::null_mut()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn groupby_mean(
    groupby_ptr: *mut CGroupBy,
    column_ptr: *const c_char,
) -> *mut CDataFrame {
    unsafe {
        if groupby_ptr.is_null() || (*groupby_ptr).inner.is_null() {
            *LAST_ERROR.lock().unwrap() = Some("GroupBy pointer is null".to_string());
            return ptr::null_mut();
        }

        let gb_ptr = (*groupby_ptr).inner as *mut LazyGroupBy;
        let lazy_groupby = Box::from_raw(gb_ptr);

        let column_str = CStr::from_ptr(column_ptr).to_str().unwrap();

        match lazy_groupby.agg([col(column_str).mean()]).collect() {
            Ok(df) => polars_df_to_c_df(df),
            Err(e) => {
                *LAST_ERROR.lock().unwrap() = Some(format!("Mean error: {}", e));
                ptr::null_mut()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn groupby_count(groupby_ptr: *mut CGroupBy) -> *mut CDataFrame {
    unsafe {
        if groupby_ptr.is_null() || (*groupby_ptr).inner.is_null() {
            *LAST_ERROR.lock().unwrap() = Some("GroupBy pointer is null".to_string());
            return ptr::null_mut();
        }

        let gb_ptr = (*groupby_ptr).inner as *mut LazyGroupBy;
        let lazy_groupby = Box::from_raw(gb_ptr);

        match lazy_groupby.agg([len().alias("count")]).collect() {
            Ok(df) => polars_df_to_c_df(df),
            Err(e) => {
                *LAST_ERROR.lock().unwrap() = Some(format!("Count error: {}", e));
                ptr::null_mut()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn groupby_min(
    groupby_ptr: *mut CGroupBy,
    column_ptr: *const c_char,
) -> *mut CDataFrame {
    unsafe {
        if groupby_ptr.is_null() || (*groupby_ptr).inner.is_null() {
            *LAST_ERROR.lock().unwrap() = Some("GroupBy pointer is null".to_string());
            return ptr::null_mut();
        }

        let gb_ptr = (*groupby_ptr).inner as *mut LazyGroupBy;
        let lazy_groupby = Box::from_raw(gb_ptr);

        let column_str = CStr::from_ptr(column_ptr).to_str().unwrap();

        match lazy_groupby.agg([col(column_str).min()]).collect() {
            Ok(df) => polars_df_to_c_df(df),
            Err(e) => {
                *LAST_ERROR.lock().unwrap() = Some(format!("Min error: {}", e));
                ptr::null_mut()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn groupby_max(
    groupby_ptr: *mut CGroupBy,
    column_ptr: *const c_char,
) -> *mut CDataFrame {
    unsafe {
        if groupby_ptr.is_null() || (*groupby_ptr).inner.is_null() {
            *LAST_ERROR.lock().unwrap() = Some("GroupBy pointer is null".to_string());
            return ptr::null_mut();
        }

        let gb_ptr = (*groupby_ptr).inner as *mut LazyGroupBy;
        let lazy_groupby = Box::from_raw(gb_ptr);

        let column_str = CStr::from_ptr(column_ptr).to_str().unwrap();

        match lazy_groupby.agg([col(column_str).max()]).collect() {
            Ok(df) => polars_df_to_c_df(df),
            Err(e) => {
                *LAST_ERROR.lock().unwrap() = Some(format!("Max error: {}", e));
                ptr::null_mut()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn groupby_std(
    groupby_ptr: *mut CGroupBy,
    column_ptr: *const c_char,
) -> *mut CDataFrame {
    unsafe {
        if groupby_ptr.is_null() || (*groupby_ptr).inner.is_null() {
            *LAST_ERROR.lock().unwrap() = Some("GroupBy pointer is null".to_string());
            return ptr::null_mut();
        }

        let gb_ptr = (*groupby_ptr).inner as *mut LazyGroupBy;
        let lazy_groupby = Box::from_raw(gb_ptr);

        let column_str = CStr::from_ptr(column_ptr).to_str().unwrap();

        match lazy_groupby.agg([col(column_str).std(1)]).collect() {
            Ok(df) => polars_df_to_c_df(df),
            Err(e) => {
                *LAST_ERROR.lock().unwrap() = Some(format!("Std error: {}", e));
                ptr::null_mut()
            }
        }
    }
}
