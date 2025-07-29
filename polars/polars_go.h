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
extern CExpr* col_gt_f64(CExpr* expr, double value);
extern CExpr* col_lt_f64(CExpr* expr, double value);
extern CExpr* col_eq_f64(CExpr* expr, double value);
extern CExpr* col_ne_f64(CExpr* expr, double value);
extern CExpr* col_ge_f64(CExpr* expr, double value);
extern CExpr* col_le_f64(CExpr* expr, double value);
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
extern CDataFrame* sort_by_columns(CDataFrame* df, const char* columns, const char* descending);
extern CDataFrame* sort_by_exprs(CDataFrame* df, CExpr** exprs, int exprs_len, const char* descending);

// Column type enum for mixed DataFrame creation
typedef enum {
    COLUMN_STRING = 0,
    COLUMN_INT64 = 1,
    COLUMN_FLOAT64 = 2,
    COLUMN_BOOL = 3,
} CColumnType;

// Column specification for mixed DataFrame creation
typedef struct {
    const char* name;
    CColumnType column_type;
    const void* data;
    int length;
} CColumnSpec;

extern CDataFrame* create_dataframe_mixed(const CColumnSpec* column_specs, int column_count);

// Join type enum
typedef enum {
    JOIN_INNER = 0,
    JOIN_LEFT = 1,
    JOIN_RIGHT = 2,
    JOIN_OUTER = 3,
} CJoinType;

// Join functions
extern CDataFrame* join_dataframes(CDataFrame* left_df, CDataFrame* right_df, const char* left_on, const char* right_on, CJoinType join_type);
extern CDataFrame* join_dataframes_multiple_keys(CDataFrame* left_df, CDataFrame* right_df, const char* left_on, const char* right_on, CJoinType join_type);

#endif
