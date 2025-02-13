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
