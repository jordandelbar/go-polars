use crate::conversions::*;
use crate::LAST_ERROR;
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
            Ok(expr) => {
                let new_expr = expr.clone().gt(lit(value));
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn col_lt(expr_ptr: *mut CExpr, value: i64) -> *mut CExpr {
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
pub extern "C" fn col_eq(expr_ptr: *mut CExpr, value: i64) -> *mut CExpr {
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
pub extern "C" fn col_ne(expr_ptr: *mut CExpr, value: i64) -> *mut CExpr {
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
pub extern "C" fn col_ge(expr_ptr: *mut CExpr, value: i64) -> *mut CExpr {
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
pub extern "C" fn col_le(expr_ptr: *mut CExpr, value: i64) -> *mut CExpr {
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
pub extern "C" fn expr_add(left_expr: *mut CExpr, right_expr: *mut CExpr) -> *mut CExpr {
    unsafe {
        let left_result = c_expr_to_expr(left_expr);
        let right_result = c_expr_to_expr(right_expr);
        match (left_result, right_result) {
            (Ok(left), Ok(right)) => {
                let new_expr = left.clone() + right.clone();
                expr_to_c_expr(new_expr)
            }
            _ => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_sub(left_expr: *mut CExpr, right_expr: *mut CExpr) -> *mut CExpr {
    unsafe {
        let left_result = c_expr_to_expr(left_expr);
        let right_result = c_expr_to_expr(right_expr);
        match (left_result, right_result) {
            (Ok(left), Ok(right)) => {
                let new_expr = left.clone() - right.clone();
                expr_to_c_expr(new_expr)
            }
            _ => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_mul(left_expr: *mut CExpr, right_expr: *mut CExpr) -> *mut CExpr {
    unsafe {
        let left_result = c_expr_to_expr(left_expr);
        let right_result = c_expr_to_expr(right_expr);
        match (left_result, right_result) {
            (Ok(left), Ok(right)) => {
                let new_expr = left.clone() * right.clone();
                expr_to_c_expr(new_expr)
            }
            _ => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_div(left_expr: *mut CExpr, right_expr: *mut CExpr) -> *mut CExpr {
    unsafe {
        let left_result = c_expr_to_expr(left_expr);
        let right_result = c_expr_to_expr(right_expr);
        match (left_result, right_result) {
            (Ok(left), Ok(right)) => {
                let new_expr = left.clone() / right.clone();
                expr_to_c_expr(new_expr)
            }
            _ => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_add_value(expr_ptr: *mut CExpr, value: f64) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone() + lit(value);
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_sub_value(expr_ptr: *mut CExpr, value: f64) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone() - lit(value);
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_mul_value(expr_ptr: *mut CExpr, value: f64) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone() * lit(value);
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_div_value(expr_ptr: *mut CExpr, value: f64) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone() / lit(value);
                expr_to_c_expr(new_expr)
            }
            Err(_) => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_and(left_expr: *mut CExpr, right_expr: *mut CExpr) -> *mut CExpr {
    unsafe {
        let left_result = c_expr_to_expr(left_expr);
        let right_result = c_expr_to_expr(right_expr);
        match (left_result, right_result) {
            (Ok(left), Ok(right)) => {
                let new_expr = left.clone().and(right.clone());
                expr_to_c_expr(new_expr)
            }
            _ => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_or(left_expr: *mut CExpr, right_expr: *mut CExpr) -> *mut CExpr {
    unsafe {
        let left_result = c_expr_to_expr(left_expr);
        let right_result = c_expr_to_expr(right_expr);
        match (left_result, right_result) {
            (Ok(left), Ok(right)) => {
                let new_expr = left.clone().or(right.clone());
                expr_to_c_expr(new_expr)
            }
            _ => ptr::null_mut(),
        }
    }
}

#[no_mangle]
pub extern "C" fn expr_not(expr_ptr: *mut CExpr) -> *mut CExpr {
    unsafe {
        let expr_result = c_expr_to_expr(expr_ptr);
        match expr_result {
            Ok(expr) => {
                let new_expr = expr.clone().not();
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
