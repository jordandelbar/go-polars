use crate::conversions::*;
use crate::set_last_error;
use crate::LAST_ERROR;
use polars::lazy::dsl::when;
use polars::prelude::*;
use std::ffi::{c_char, CStr};
use std::ptr;

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
            Ok(expr) => expr_to_c_expr(expr.clone().gt(lit(value))),
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_and(left_expr: *mut CExpr, right_expr: *mut CExpr) -> *mut CExpr {
    unsafe {
        match (c_expr_to_expr(left_expr), c_expr_to_expr(right_expr)) {
            (Ok(left), Ok(right)) => expr_to_c_expr(left.clone().and(right.clone())),
            _ => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_or(left_expr: *mut CExpr, right_expr: *mut CExpr) -> *mut CExpr {
    unsafe {
        match (c_expr_to_expr(left_expr), c_expr_to_expr(right_expr)) {
            (Ok(left), Ok(right)) => expr_to_c_expr(left.clone().or(right.clone())),
            _ => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_not(expr_ptr: *mut CExpr) -> *mut CExpr {
    unsafe {
        match c_expr_to_expr(expr_ptr) {
            Ok(expr) => expr_to_c_expr(expr.clone().not()),
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_median(expr_ptr: *mut CExpr) -> *mut CExpr {
    unsafe {
        match c_expr_to_expr(expr_ptr) {
            Ok(expr) => expr_to_c_expr(expr.clone().median()),
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_is_null(expr_ptr: *mut CExpr) -> *mut CExpr {
    unsafe {
        match c_expr_to_expr(expr_ptr) {
            Ok(expr) => expr_to_c_expr(expr.clone().is_null()),
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_is_not_null(expr_ptr: *mut CExpr) -> *mut CExpr {
    unsafe {
        match c_expr_to_expr(expr_ptr) {
            Ok(expr) => expr_to_c_expr(expr.clone().is_not_null()),
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_fill_null_int64(expr_ptr: *mut CExpr, value: i64) -> *mut CExpr {
    unsafe {
        match c_expr_to_expr(expr_ptr) {
            Ok(expr) => expr_to_c_expr(expr.clone().fill_null(lit(value))),
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_fill_null_f64(expr_ptr: *mut CExpr, value: f64) -> *mut CExpr {
    unsafe {
        match c_expr_to_expr(expr_ptr) {
            Ok(expr) => expr_to_c_expr(expr.clone().fill_null(lit(value))),
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_fill_null_str(expr_ptr: *mut CExpr, value: *const c_char) -> *mut CExpr {
    unsafe {
        if value.is_null() {
            return ptr::null_mut();
        }
        let val_str = CStr::from_ptr(value).to_str().unwrap_or_default();
        match c_expr_to_expr(expr_ptr) {
            Ok(expr) => expr_to_c_expr(expr.clone().fill_null(lit(val_str))),
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_fill_null_bool(expr_ptr: *mut CExpr, value: u8) -> *mut CExpr {
    unsafe {
        match c_expr_to_expr(expr_ptr) {
            Ok(expr) => expr_to_c_expr(expr.clone().fill_null(lit(value != 0))),
            Err(_) => ptr::null_mut(),
        }
    }
}
// ========== String namespace functions ==========

#[no_mangle]
pub extern "C" fn expr_str_strip_chars(expr_ptr: *mut CExpr, chars: *const c_char) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        let matches = if chars.is_null() {
            lit(NULL)
        } else {
            let chars_str = CStr::from_ptr(chars).to_str().unwrap_or_default();
            lit(chars_str)
        };
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().str().strip_chars(matches);
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_str_to_lowercase(expr_ptr: *mut CExpr) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().str().to_lowercase();
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_str_to_uppercase(expr_ptr: *mut CExpr) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().str().to_uppercase();
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_str_contains(expr_ptr: *mut CExpr, pattern: *const c_char, literal: u8) -> *mut CExpr {
    unsafe {
        if pattern.is_null() {
            set_last_error("pattern cannot be null");
            return ptr::null_mut();
        }
        let expr_result = c_expr_to_expr(expr_ptr);
        let pat = CStr::from_ptr(pattern).to_str().unwrap_or_default();
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().str().contains(lit(pat), literal != 0);
                expr_to_c_expr(new_expr)
            }
            Err(e) => {
                set_last_error(&e);
                ptr::null_mut()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_str_extract(expr_ptr: *mut CExpr, pattern: *const c_char, group_index: u32) -> *mut CExpr {
    unsafe {
        if pattern.is_null() {
            set_last_error("pattern cannot be null");
            return ptr::null_mut();
        }
        let expr_result = c_expr_to_expr(expr_ptr);
        let pat = CStr::from_ptr(pattern).to_str().unwrap_or_default();
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().str().extract(lit(pat), group_index as usize);
                expr_to_c_expr(new_expr)
            }
            Err(e) => {
                set_last_error(&e);
                ptr::null_mut()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_str_starts_with(expr_ptr: *mut CExpr, prefix: *const c_char) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        let pre = if prefix.is_null() { "" } else { CStr::from_ptr(prefix).to_str().unwrap_or_default() };
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().str().starts_with(lit(pre));
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_str_ends_with(expr_ptr: *mut CExpr, suffix: *const c_char) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        let suf = if suffix.is_null() { "" } else { CStr::from_ptr(suffix).to_str().unwrap_or_default() };
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().str().ends_with(lit(suf));
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_str_len_chars(expr_ptr: *mut CExpr) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().str().len_chars();
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_str_replace(expr_ptr: *mut CExpr, pattern: *const c_char, value: *const c_char, literal: u8) -> *mut CExpr {
    unsafe {
        if pattern.is_null() || value.is_null() {
            set_last_error("pattern and value cannot be null");
            return ptr::null_mut();
        }
        let expr_result = c_expr_to_expr(expr_ptr);
        let pattern_str = CStr::from_ptr(pattern).to_str().unwrap_or_default();
        let value_str = CStr::from_ptr(value).to_str().unwrap_or_default();
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().str().replace(lit(pattern_str), lit(value_str), literal != 0);
                expr_to_c_expr(new_expr)
            }
            Err(e) => {
                set_last_error(&e);
                ptr::null_mut()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_str_replace_all(expr_ptr: *mut CExpr, pattern: *const c_char, value: *const c_char, literal: u8) -> *mut CExpr {
    unsafe {
        if pattern.is_null() || value.is_null() {
            set_last_error("pattern and value cannot be null");
            return ptr::null_mut();
        }
        let expr_result = c_expr_to_expr(expr_ptr);
        let pattern_str = CStr::from_ptr(pattern).to_str().unwrap_or_default();
        let value_str = CStr::from_ptr(value).to_str().unwrap_or_default();
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().str().replace_all(lit(pattern_str), lit(value_str), literal != 0);
                expr_to_c_expr(new_expr)
            }
            Err(e) => {
                set_last_error(&e);
                ptr::null_mut()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_str_slice(expr_ptr: *mut CExpr, offset: i64, length: i64) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        match expr_result {
            Ok(expr) => {
                let len_expr = if length < 0 { lit(NULL) } else { lit(length as u64) };
                let new_expr = expr.clone().str().slice(lit(offset), len_expr);
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_str_split(expr_ptr: *mut CExpr, by: *const c_char) -> *mut CExpr {
    unsafe {
        if by.is_null() {
            set_last_error("split delimiter cannot be null");
            return ptr::null_mut();
        }
        let by_str = CStr::from_ptr(by).to_str().unwrap_or_default();
        match c_expr_to_expr(expr_ptr) {
            Ok(expr) => expr_to_c_expr(expr.clone().str().split(lit(by_str))),
            Err(e) => {
                set_last_error(&e);
                ptr::null_mut()
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_str_split_inclusive(expr_ptr: *mut CExpr, by: *const c_char) -> *mut CExpr {
    unsafe {
        if by.is_null() {
            set_last_error("split delimiter cannot be null");
            return ptr::null_mut();
        }
        let by_str = CStr::from_ptr(by).to_str().unwrap_or_default();
        match c_expr_to_expr(expr_ptr) {
            Ok(expr) => expr_to_c_expr(expr.clone().str().split_inclusive(lit(by_str))),
            Err(e) => {
                set_last_error(&e);
                ptr::null_mut()
            }
        }
    }
}

// ========== List namespace functions ==========

#[no_mangle]
pub extern "C" fn expr_list_len(expr_ptr: *mut CExpr) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().list().len();
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_list_first(expr_ptr: *mut CExpr) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().list().first();
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_list_last(expr_ptr: *mut CExpr) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().list().last();
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_list_get(expr_ptr: *mut CExpr, index: i64) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().list().get(lit(index), false);
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_list_join(expr_ptr: *mut CExpr, separator: *const c_char) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        let sep_str = if separator.is_null() { "" } else { CStr::from_ptr(separator).to_str().unwrap_or_default() };
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().list().join(lit(sep_str), false);
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_list_sum(expr_ptr: *mut CExpr) -> *mut CExpr {
    unsafe {
        match c_expr_to_expr(expr_ptr) {
            Ok(expr) => expr_to_c_expr(expr.clone().list().sum()),
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_list_max(expr_ptr: *mut CExpr) -> *mut CExpr {
    unsafe {
        match c_expr_to_expr(expr_ptr) {
            Ok(expr) => expr_to_c_expr(expr.clone().list().max()),
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_list_min(expr_ptr: *mut CExpr) -> *mut CExpr {
    unsafe {
        match c_expr_to_expr(expr_ptr) {
            Ok(expr) => expr_to_c_expr(expr.clone().list().min()),
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_list_mean(expr_ptr: *mut CExpr) -> *mut CExpr {
    unsafe {
        match c_expr_to_expr(expr_ptr) {
            Ok(expr) => expr_to_c_expr(expr.clone().list().mean()),
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_list_sort(expr_ptr: *mut CExpr, descending: u8, nulls_last: u8) -> *mut CExpr {
    unsafe {
        let mut options = SortOptions::default();
        options.descending = descending != 0;
        options.nulls_last = nulls_last != 0;
        match c_expr_to_expr(expr_ptr) {
            Ok(expr) => expr_to_c_expr(expr.clone().list().sort(options)),
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_list_reverse(expr_ptr: *mut CExpr) -> *mut CExpr {
    unsafe {
        match c_expr_to_expr(expr_ptr) {
            Ok(expr) => expr_to_c_expr(expr.clone().list().reverse()),
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_list_unique(expr_ptr: *mut CExpr, maintain_order: u8) -> *mut CExpr {
    unsafe {
        match c_expr_to_expr(expr_ptr) {
            Ok(expr) => {
                let new_expr = if maintain_order != 0 {
                    expr.clone().list().unique_stable()
                } else {
                    expr.clone().list().unique()
                };
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_list_head(expr_ptr: *mut CExpr, length: i64) -> *mut CExpr {
    unsafe {
        match c_expr_to_expr(expr_ptr) {
            Ok(expr) => expr_to_c_expr(expr.clone().list().head(lit(length))),
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_list_tail(expr_ptr: *mut CExpr, length: i64) -> *mut CExpr {
    unsafe {
        match c_expr_to_expr(expr_ptr) {
            Ok(expr) => expr_to_c_expr(expr.clone().list().tail(lit(length))),
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_list_slice(expr_ptr: *mut CExpr, offset: i64, length: i64) -> *mut CExpr {
    unsafe {
        match c_expr_to_expr(expr_ptr) {
            Ok(expr) => expr_to_c_expr(expr.clone().list().slice(lit(offset), lit(length))),
            Err(_) => ptr::null_mut(),
        }
    }
}

// ========== is_in functions ==========

#[no_mangle]
pub extern "C" fn expr_is_in_str(expr_ptr: *mut CExpr, values: *const *const c_char, len: i32) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        if values.is_null() || len <= 0 {
            set_last_error("is_in requires at least one value");
            return ptr::null_mut();
        }
        let slice = std::slice::from_raw_parts(values, len as usize);
        let strings: Vec<String> = slice
            .iter()
            .map(|&s| if s.is_null() { String::new() } else { CStr::from_ptr(s).to_str().unwrap_or_default().to_string() })
            .collect();
        let series = Series::new("is_in_values".into(), strings);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().is_in(lit(series));
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_is_in_int64(expr_ptr: *mut CExpr, values: *const i64, len: i32) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        if values.is_null() || len <= 0 {
            set_last_error("is_in requires at least one value");
            return ptr::null_mut();
        }
        let slice = std::slice::from_raw_parts(values, len as usize);
        let series = Series::new("is_in_values".into(), slice);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().is_in(lit(series));
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_is_in_f64(expr_ptr: *mut CExpr, values: *const f64, len: i32) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        if values.is_null() || len <= 0 {
            set_last_error("is_in requires at least one value");
            return ptr::null_mut();
        }
        let slice = std::slice::from_raw_parts(values, len as usize);
        let series = Series::new("is_in_values".into(), slice);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().is_in(lit(series));
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

        // Free both the inner Expr and the outer CExpr wrapper.
        let boxed = Box::from_raw(expr);
        let inner_ptr = boxed.inner as *mut Expr;
        if !inner_ptr.is_null() {
            drop(Box::from_raw(inner_ptr));
        }
        drop(boxed);
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

#[no_mangle]
pub extern "C" fn expr_sum(expr_ptr: *mut CExpr) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().sum();
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_mean(expr_ptr: *mut CExpr) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().mean();
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn col_lt(expr_ptr: *mut CExpr, value: i64) -> *mut CExpr {
    unsafe {
        match c_expr_to_expr(expr_ptr) {
            Ok(expr) => expr_to_c_expr(expr.clone().lt(lit(value))),
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn col_eq(expr_ptr: *mut CExpr, value: i64) -> *mut CExpr {
    unsafe {
        match c_expr_to_expr(expr_ptr) {
            Ok(expr) => expr_to_c_expr(expr.clone().eq(lit(value))),
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn col_ne(expr_ptr: *mut CExpr, value: i64) -> *mut CExpr {
    unsafe {
        match c_expr_to_expr(expr_ptr) {
            Ok(expr) => expr_to_c_expr(expr.clone().neq(lit(value))),
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn col_ge(expr_ptr: *mut CExpr, value: i64) -> *mut CExpr {
    unsafe {
        match c_expr_to_expr(expr_ptr) {
            Ok(expr) => expr_to_c_expr(expr.clone().gt_eq(lit(value))),
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn col_le(expr_ptr: *mut CExpr, value: i64) -> *mut CExpr {
    unsafe {
        match c_expr_to_expr(expr_ptr) {
            Ok(expr) => expr_to_c_expr(expr.clone().lt_eq(lit(value))),
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn col_eq_str(expr_ptr: *mut CExpr, value: *const c_char) -> *mut CExpr {
    unsafe {
        let val_str = if value.is_null() { "" } else { CStr::from_ptr(value).to_str().unwrap_or_default() };
        match c_expr_to_expr(expr_ptr) {
            Ok(expr) => expr_to_c_expr(expr.clone().eq(lit(val_str))),
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn col_ne_str(expr_ptr: *mut CExpr, value: *const c_char) -> *mut CExpr {
    unsafe {
        let val_str = if value.is_null() { "" } else { CStr::from_ptr(value).to_str().unwrap_or_default() };
        match c_expr_to_expr(expr_ptr) {
            Ok(expr) => expr_to_c_expr(expr.clone().neq(lit(val_str))),
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_add(left_expr: *mut CExpr, right_expr: *mut CExpr) -> *mut CExpr {
    unsafe {
        match (c_expr_to_expr(left_expr), c_expr_to_expr(right_expr)) {
            (Ok(left), Ok(right)) => expr_to_c_expr(left + right),
            _ => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_sub(left_expr: *mut CExpr, right_expr: *mut CExpr) -> *mut CExpr {
    unsafe {
        match (c_expr_to_expr(left_expr), c_expr_to_expr(right_expr)) {
            (Ok(left), Ok(right)) => expr_to_c_expr(left - right),
            _ => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_mul(left_expr: *mut CExpr, right_expr: *mut CExpr) -> *mut CExpr {
    unsafe {
        match (c_expr_to_expr(left_expr), c_expr_to_expr(right_expr)) {
            (Ok(left), Ok(right)) => expr_to_c_expr(left * right),
            _ => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_div(left_expr: *mut CExpr, right_expr: *mut CExpr) -> *mut CExpr {
    unsafe {
        match (c_expr_to_expr(left_expr), c_expr_to_expr(right_expr)) {
            (Ok(left), Ok(right)) => expr_to_c_expr(left / right),
            _ => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_add_value(expr_ptr: *mut CExpr, value: f64) -> *mut CExpr {
    unsafe {
        match c_expr_to_expr(expr_ptr) {
            Ok(expr) => expr_to_c_expr(expr + lit(value)),
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_sub_value(expr_ptr: *mut CExpr, value: f64) -> *mut CExpr {
    unsafe {
        match c_expr_to_expr(expr_ptr) {
            Ok(expr) => expr_to_c_expr(expr - lit(value)),
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_mul_value(expr_ptr: *mut CExpr, value: f64) -> *mut CExpr {
    unsafe {
        match c_expr_to_expr(expr_ptr) {
            Ok(expr) => expr_to_c_expr(expr * lit(value)),
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_div_value(expr_ptr: *mut CExpr, value: f64) -> *mut CExpr {
    unsafe {
        match c_expr_to_expr(expr_ptr) {
            Ok(expr) => expr_to_c_expr(expr / lit(value)),
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_min(expr_ptr: *mut CExpr) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().min();
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_max(expr_ptr: *mut CExpr) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().max();
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_std(expr_ptr: *mut CExpr) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().std(1);
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_var(expr_ptr: *mut CExpr) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().var(1);
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_quantile(expr_ptr: *mut CExpr, quantile: f64) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr
                    .clone()
                    .quantile(lit(quantile), QuantileMethod::Nearest);
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_n_unique(expr_ptr: *mut CExpr) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().n_unique();
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_approx_n_unique(expr_ptr: *mut CExpr) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().approx_n_unique();
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_first(expr_ptr: *mut CExpr) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().first();
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_last(expr_ptr: *mut CExpr) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().last();
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_product(expr_ptr: *mut CExpr) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().product();
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_count() -> *mut CExpr {
    expr_to_c_expr(len().alias("count"))
}

#[no_mangle]
pub extern "C" fn col_gt_f64(expr_ptr: *mut CExpr, value: f64) -> *mut CExpr {
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
pub extern "C" fn col_lt_f64(expr_ptr: *mut CExpr, value: f64) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().lt(lit(value));
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn col_eq_f64(expr_ptr: *mut CExpr, value: f64) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().eq(lit(value));
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn col_ne_f64(expr_ptr: *mut CExpr, value: f64) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().neq(lit(value));
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn col_ge_f64(expr_ptr: *mut CExpr, value: f64) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().gt_eq(lit(value));
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn col_le_f64(expr_ptr: *mut CExpr, value: f64) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().lt_eq(lit(value));
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_when_then_otherwise(
    conditions: *const *mut CExpr,
    results: *const *mut CExpr,
    len: i32,
    otherwise_ptr: *mut CExpr,
) -> *mut CExpr {
    if len <= 0 || conditions.is_null() || results.is_null() || otherwise_ptr.is_null() {
        set_last_error("When/Otherwise requires at least one branch and non-null expressions");
        return ptr::null_mut();
    }

    let len = len as usize;

    let cond_slice = unsafe { std::slice::from_raw_parts(conditions, len) };
    let result_slice = unsafe { std::slice::from_raw_parts(results, len) };

    let mut current_expr = match unsafe { c_expr_to_expr(otherwise_ptr) } {
        Ok(expr) => expr,
        Err(e) => {
            set_last_error(&e);
            return ptr::null_mut();
        }
    };

    for idx in (0..len).rev() {
        let condition_expr = match unsafe { c_expr_to_expr(cond_slice[idx]) } {
            Ok(expr) => expr,
            Err(e) => {
                set_last_error(&e);
                return ptr::null_mut();
            }
        };

        let then_expr = match unsafe { c_expr_to_expr(result_slice[idx]) } {
            Ok(expr) => expr,
            Err(e) => {
                set_last_error(&e);
                return ptr::null_mut();
            }
        };

        current_expr = when(condition_expr).then(then_expr).otherwise(current_expr);
    }

    expr_to_c_expr(current_expr)
}

fn c_int_to_dtype(tag: i32) -> Option<DataType> {
    match tag {
        0 => Some(DataType::Boolean),
        1 => Some(DataType::Int32),
        2 => Some(DataType::Int64),
        3 => Some(DataType::Float32),
        4 => Some(DataType::Float64),
        5 => Some(DataType::String),
        _ => None,
    }
}

#[no_mangle]
pub extern "C" fn expr_cast(expr_ptr: *mut CExpr, dtype: i32) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        match (expr_result, c_int_to_dtype(dtype)) {
            (Ok(expr), Some(dt)) => {
                let new_expr = expr.cast(dt);
                expr_to_c_expr(new_expr)
            }
            (Err(e), _) => {
                *LAST_ERROR.lock().unwrap() = Some(e);
                ptr::null_mut()
            }
            (_, None) => {
                *LAST_ERROR.lock().unwrap() = Some("Unsupported data type".into());
                ptr::null_mut()
            }
        }
    }
}
