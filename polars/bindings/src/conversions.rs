use polars::prelude::*;
use std::cell::RefCell;
use std::ffi::c_void;
use std::rc::Rc;

#[repr(C)]
pub struct CDataFrame {
    pub inner: *mut c_void,
}

#[repr(C)]
pub struct CExpr {
    pub inner: *mut c_void,
}

#[repr(C)]
pub struct CGroupBy {
    pub inner: *mut c_void,
}

#[repr(C)]
pub struct CLazyFrame {
    pub inner: *mut c_void,
}

pub fn polars_df_to_c_df(df: DataFrame) -> *mut CDataFrame {
    let rc_df = Rc::new(RefCell::new(df));
    let boxed_df = Box::new(rc_df);
    let inner = Box::into_raw(boxed_df) as *mut c_void;
    let c_df = CDataFrame { inner };
    Box::into_raw(Box::new(c_df))
}

pub fn lazyframe_to_c_lazyframe(lf: LazyFrame) -> *mut CLazyFrame {
    let boxed_lf = Box::new(lf);
    let inner = Box::into_raw(boxed_lf) as *mut c_void;
    let c_lf = CLazyFrame { inner };
    Box::into_raw(Box::new(c_lf))
}

pub unsafe fn c_lazyframe_to_lazyframe(c_lf: *mut CLazyFrame) -> Result<LazyFrame, String> {
    if c_lf.is_null() || (*c_lf).inner.is_null() {
        return Err("CLazyFrame or inner pointer is null".to_string());
    }
    let lf_ptr = (*c_lf).inner as *mut LazyFrame;
    Ok((*lf_ptr).clone())
}

pub unsafe fn c_df_to_polars_df(c_df: *mut CDataFrame) -> Result<Rc<RefCell<DataFrame>>, String> {
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
    let ptr = Box::into_raw(boxed_expr) as *mut c_void;
    let c_expr = CExpr { inner: ptr };
    Box::into_raw(Box::new(c_expr))
}

pub unsafe fn c_expr_to_expr(c_expr: *mut CExpr) -> Result<Expr, String> {
    if c_expr.is_null() || (*c_expr).inner.is_null() {
        return Err("CExpr or inner pointer is null".to_string());
    }
    // Clone the underlying Expr without taking ownership so the caller can reuse the pointer.
    let expr_ptr = (*c_expr).inner as *mut Expr;
    Ok((*expr_ptr).clone())
}

pub fn groupby_to_c_groupby(gb: LazyGroupBy) -> *mut CGroupBy {
    let boxed_gb = Box::new(gb);
    let inner = Box::into_raw(boxed_gb) as *mut c_void;
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
