#ifndef POLARS_GO_H
#define POLARS_GO_H

#include <stdint.h>
#include <stddef.h>

typedef struct CDataFrame {
  void* handle;
} CDataFrame;

typedef struct CExpr {
  void* inner;
} CExpr;

typedef struct CGroupBy {
  void* handle;
} CGroupBy;

extern CDataFrame* read_csv(const char* path);
extern CDataFrame* read_parquet(const char* path);
extern void free_dataframe(CDataFrame* df);
extern const char* write_csv(CDataFrame* df, const char* path);
extern const char* write_parquet(CDataFrame* df, const char* path);
extern size_t dataframe_width(const CDataFrame* df);
extern size_t dataframe_height(const CDataFrame* df);
extern const char* dataframe_column_name(const CDataFrame* df, size_t index);
extern CDataFrame* filter(CDataFrame* df, CExpr* expr);
extern CDataFrame* select_columns(CDataFrame *df, CExpr* *exprs, int exprs_len);
extern CDataFrame* head(CDataFrame* df, size_t n);
extern CExpr* col(const char* name);
extern CExpr* col_gt(CExpr* expr, int64_t value);
extern CExpr* col_lt(CExpr* expr, int64_t value);
extern CExpr* col_eq(CExpr* expr, int64_t value);
extern CExpr* col_ne(CExpr* expr, int64_t value);
extern CExpr* col_ge(CExpr* expr, int64_t value);
extern CExpr* col_le(CExpr* expr, int64_t value);
extern CGroupBy* group_by(CDataFrame* df, const char* columns);
extern const char* columns(CDataFrame* df);
extern const char* print_dataframe(CDataFrame* df);
extern const char* get_last_error_message();
extern void free_expr(CExpr* expr);
extern void free_groupby(CGroupBy* groupby);
extern CExpr* expr_alias(CExpr* expr, const char* alias);
extern CExpr* lit_int64(int64_t val);
extern CExpr* lit_int32(int32_t val);
extern CExpr* lit_float64(double val);
extern CExpr* lit_float32(float val);
extern CExpr* lit_string(const char* val);
extern CExpr* lit_bool(uint8_t val);
extern CDataFrame* with_columns(CDataFrame* df, CExpr** exprs_ptr, int exprs_len);
extern CExpr* expr_add(CExpr* left_expr, CExpr* right_expr);
extern CExpr* expr_sub(CExpr* left_expr, CExpr* right_expr);
extern CExpr* expr_mul(CExpr* left_expr, CExpr* right_expr);
extern CExpr* expr_div(CExpr* left_expr, CExpr* right_expr);
extern CExpr* expr_add_value(CExpr* expr, double value);
extern CExpr* expr_sub_value(CExpr* expr, double value);
extern CExpr* expr_mul_value(CExpr* expr, double value);
extern CExpr* expr_div_value(CExpr* expr, double value);
extern CExpr* expr_and(CExpr* left_expr, CExpr* right_expr);
extern CExpr* expr_or(CExpr* left_expr, CExpr* right_expr);
extern CExpr* expr_not(CExpr* expr);
extern CDataFrame* groupby_agg(CGroupBy* groupby, CExpr** exprs_ptr, int exprs_len);
extern CDataFrame* groupby_sum(CGroupBy* groupby, const char* column);
extern CDataFrame* groupby_mean(CGroupBy* groupby, const char* column);
extern CDataFrame* groupby_count(CGroupBy* groupby);
extern CDataFrame* groupby_min(CGroupBy* groupby, const char* column);
extern CDataFrame* groupby_max(CGroupBy* groupby, const char* column);
extern CDataFrame* groupby_std(CGroupBy* groupby, const char* column);
extern CExpr* expr_sum(CExpr* expr);
extern CExpr* expr_mean(CExpr* expr);
extern CExpr* expr_min(CExpr* expr);
extern CExpr* expr_max(CExpr* expr);
extern CExpr* expr_std(CExpr* expr);
extern CExpr* expr_count();

#endif
