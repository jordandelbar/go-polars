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

typedef struct CLazyFrame {
  void* handle;
} CLazyFrame;

extern CDataFrame* read_csv(const char* path);
extern CDataFrame* read_parquet(const char* path);
extern CDataFrame* read_json(const char* path);
extern void free_dataframe(CDataFrame* df);
extern const char* write_csv(CDataFrame* df, const char* path);
extern const char* write_parquet(CDataFrame* df, const char* path);
extern const char* write_json(CDataFrame* df, const char* path);
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
extern CExpr* col_eq_str(CExpr* expr, const char* value);
extern CExpr* col_ne_str(CExpr* expr, const char* value);
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
extern CExpr* expr_is_null(CExpr* expr);
extern CExpr* expr_is_not_null(CExpr* expr);
extern CExpr* expr_fill_null_int64(CExpr* expr, int64_t value);
extern CExpr* expr_fill_null_f64(CExpr* expr, double value);
extern CExpr* expr_fill_null_str(CExpr* expr, const char* value);
extern CExpr* expr_fill_null_bool(CExpr* expr, uint8_t value);
// String namespace functions
extern CExpr* expr_str_strip_chars(CExpr* expr, const char* chars);
extern CExpr* expr_str_to_lowercase(CExpr* expr);
extern CExpr* expr_str_to_uppercase(CExpr* expr);
extern CExpr* expr_str_contains(CExpr* expr, const char* pattern, uint8_t literal);
extern CExpr* expr_str_starts_with(CExpr* expr, const char* prefix);
extern CExpr* expr_str_ends_with(CExpr* expr, const char* suffix);
extern CExpr* expr_str_len_chars(CExpr* expr);
extern CExpr* expr_str_replace(CExpr* expr, const char* pattern, const char* value, uint8_t literal);
extern CExpr* expr_str_replace_all(CExpr* expr, const char* pattern, const char* value, uint8_t literal);
extern CExpr* expr_str_slice(CExpr* expr, int64_t offset, int64_t length);
extern CExpr* expr_str_extract(CExpr* expr, const char* pattern, uint32_t group_index);
extern CExpr* expr_str_split(CExpr* expr, const char* by);
extern CExpr* expr_str_split_inclusive(CExpr* expr, const char* by);

// List namespace functions
extern CExpr* expr_list_len(CExpr* expr);
extern CExpr* expr_list_first(CExpr* expr);
extern CExpr* expr_list_last(CExpr* expr);
extern CExpr* expr_list_get(CExpr* expr, int64_t index);
extern CExpr* expr_list_join(CExpr* expr, const char* separator);
extern CExpr* expr_list_sum(CExpr* expr);
extern CExpr* expr_list_max(CExpr* expr);
extern CExpr* expr_list_min(CExpr* expr);
extern CExpr* expr_list_mean(CExpr* expr);
extern CExpr* expr_list_sort(CExpr* expr, uint8_t descending, uint8_t nulls_last);
extern CExpr* expr_list_reverse(CExpr* expr);
extern CExpr* expr_list_unique(CExpr* expr, uint8_t maintain_order);
extern CExpr* expr_list_head(CExpr* expr, int64_t length);
extern CExpr* expr_list_tail(CExpr* expr, int64_t length);
extern CExpr* expr_list_slice(CExpr* expr, int64_t offset, int64_t length);

// is_in function
extern CExpr* expr_is_in_str(CExpr* expr, const char** values, int len);
extern CExpr* expr_is_in_int64(CExpr* expr, const int64_t* values, int len);
extern CExpr* expr_is_in_f64(CExpr* expr, const double* values, int len);

extern CExpr* expr_when_then_otherwise(CExpr** conditions, CExpr** results, int len, CExpr* otherwise_expr);
extern CDataFrame* groupby_agg(CGroupBy* groupby, CExpr** exprs_ptr, int exprs_len);
extern CDataFrame* groupby_sum(CGroupBy* groupby, const char* column);
extern CDataFrame* groupby_mean(CGroupBy* groupby, const char* column);
extern CDataFrame* groupby_count(CGroupBy* groupby);
extern CDataFrame* groupby_min(CGroupBy* groupby, const char* column);
extern CDataFrame* groupby_max(CGroupBy* groupby, const char* column);
extern CDataFrame* groupby_std(CGroupBy* groupby, const char* column);
extern CDataFrame* groupby_var(CGroupBy* groupby, const char* column);
extern CDataFrame* groupby_quantile(CGroupBy* groupby, const char* column, double quantile);
extern CDataFrame* groupby_first(CGroupBy* groupby, const char* column);
extern CDataFrame* groupby_last(CGroupBy* groupby, const char* column);
extern CDataFrame* groupby_n_unique(CGroupBy* groupby, const char* column);
extern CDataFrame* groupby_approx_n_unique(CGroupBy* groupby, const char* column);
extern CDataFrame* groupby_product(CGroupBy* groupby, const char* column);
extern CDataFrame* groupby_median(CGroupBy* groupby, const char* column);
extern CExpr* expr_sum(CExpr* expr);
extern CExpr* expr_mean(CExpr* expr);
extern CExpr* expr_min(CExpr* expr);
extern CExpr* expr_max(CExpr* expr);
extern CExpr* expr_std(CExpr* expr);
extern CExpr* expr_var(CExpr* expr);
extern CExpr* expr_quantile(CExpr* expr, double quantile);
extern CExpr* expr_first(CExpr* expr);
extern CExpr* expr_last(CExpr* expr);
extern CExpr* expr_n_unique(CExpr* expr);
extern CExpr* expr_approx_n_unique(CExpr* expr);
extern CExpr* expr_product(CExpr* expr);
extern CExpr* expr_median(CExpr* expr);
extern CExpr* expr_count();
extern CDataFrame* sort_by_columns(CDataFrame* df, const char* columns, const char* descending);
extern CDataFrame* sort_by_exprs(CDataFrame* df, CExpr** exprs, int exprs_len, const char* descending);

// Column type enum for mixed DataFrame creation
typedef enum {
    COLUMN_STRING = 0,
    COLUMN_INT64 = 1,
    COLUMN_FLOAT64 = 2,
    COLUMN_BOOL = 3,
    COLUMN_DATE = 4,
    COLUMN_DATETIME_MS = 5,
    COLUMN_LIST_STRING = 6,
    COLUMN_LIST_INT64 = 7,
    COLUMN_LIST_FLOAT64 = 8,
    COLUMN_LIST_BOOL = 9,
    COLUMN_LIST_DATETIME_MS = 10,
} CColumnType;

// Column specification for mixed DataFrame creation
typedef struct {
    const char* name;
    CColumnType column_type;
    const void* data;
    int length;
  const int* sub_lengths; // Only for list columns: length per row (size == length)
  const uint8_t* validity; // Optional per-row validity bitmap (1=valid,0=null); size==length for list columns
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
extern CDataFrame* concat_df_vertical(CDataFrame** dfs, int dfs_len, uint8_t rechunk);
extern CDataFrame* concat_df_horizontal(CDataFrame** dfs, int dfs_len, uint8_t rechunk);
extern CDataFrame* drop_columns(CDataFrame* df, const char* columns);
extern CLazyFrame* scan_csv(const char* path);
extern CLazyFrame* scan_csv_with_schema(
  const char* path,
  const char** override_columns,
  const int32_t* override_dtypes,
  int override_len,
  const char** schema_columns,
  const int32_t* schema_dtypes,
  int schema_len,
  int64_t infer_schema_length,
  uint8_t ignore_errors,
  uint8_t truncate_ragged_lines,
  uint8_t has_header);
extern CLazyFrame* scan_parquet(const char* path);
extern CLazyFrame* scan_ndjson(const char* path);
extern CLazyFrame* dataframe_lazy(CDataFrame* df);
extern CDataFrame* lazy_collect(CLazyFrame* lf, uint8_t streaming);
extern void free_lazyframe(CLazyFrame* lf);

// Data type enum for casting and schema inspection
typedef enum {
    DATA_TYPE_INVALID = -1,
    DATA_TYPE_BOOLEAN = 0,
    DATA_TYPE_INT32 = 1,
    DATA_TYPE_INT64 = 2,
    DATA_TYPE_FLOAT32 = 3,
    DATA_TYPE_FLOAT64 = 4,
  DATA_TYPE_UTF8 = 5,
  DATA_TYPE_DATE = 6,
  DATA_TYPE_DATETIME_MS = 7,
} CDataType;

extern CExpr* expr_cast(CExpr* expr, int32_t data_type);
extern int32_t dataframe_column_dtype(const CDataFrame* df, size_t index);

#endif
