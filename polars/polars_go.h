#ifndef POLARS_GO_H
#define POLARS_GO_H

#include <stdlib.h>

typedef struct CDataFrame {
    void* handle;
} CDataFrame;

typedef struct CExpr {
    void* handle;
} CExpr;

extern void* read_csv(const char* path);
extern void free_dataframe(CDataFrame* df);
extern const char* write_csv(CDataFrame* df, const char* path);

extern void* col(const char* name);
extern void* col_gt(CExpr* expr, long value);

extern void* filter(CDataFrame* df, CExpr* expr);
extern void* head(CDataFrame* df, size_t n);

extern const char* columns(CDataFrame* df);

extern const char* print_dataframe(CDataFrame* df);

extern const char* get_last_error();

extern size_t dataframe_width(CDataFrame* df);
extern size_t dataframe_height(CDataFrame* df);
extern const char* dataframe_column_name(CDataFrame* df, size_t index);

#endif
