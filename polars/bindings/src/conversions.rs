use polars::prelude::*;
use std::cell::RefCell;
use std::rc::Rc;

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
