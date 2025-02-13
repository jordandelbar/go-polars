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

#endif
