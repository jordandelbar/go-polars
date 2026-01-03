package polars

/*
#cgo CFLAGS: -I${SRCDIR}
#cgo linux LDFLAGS: -lpolars_go -ldl -lm -lpthread
#cgo darwin LDFLAGS: -lpolars_go -framework CoreFoundation -framework Security -framework IOKit
#cgo windows LDFLAGS: -lpolars_go -lws2_32 -luserenv -ladvapi32 -lkernel32
*/
import "C"
