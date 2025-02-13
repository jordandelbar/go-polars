mod conversions;
mod dataframe_functions;
mod expr_functions;
mod groupby_functions;

use std::ffi::{c_char, CString};
use std::ptr;
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

pub use conversions::*;
pub use dataframe_functions::*;
pub use expr_functions::*;
pub use groupby_functions::*;
