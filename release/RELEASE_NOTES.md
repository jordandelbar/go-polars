# go-polars Linux Release v0.0.14

## Binary Information

- **File**: `libpolars_go-linux-amd64-v0.0.14.so`
- **Platform**: Linux x86_64
- **Size**: 46M
- **SHA256**: `f34e5d9da8f12e0c2c207d98ba7b3d475a6bae632ebb13cf9271d631a6c764b3`
- **MD5**: `5783ffb7a499b095d86826ce2583f940`

## Installation

1. Download the binary file
2. Verify the checksum:
   ```bash
   sha256sum -c libpolars_go-linux-amd64-v0.0.14.so.sha256
   ```
3. Copy to your project:
   ```bash
   mkdir -p polars/bin
   cp libpolars_go-linux-amd64-v0.0.14.so polars/bin/libpolars_go.so
   ```

## Build Information

- **Built on**: Sat Jul 19 02:44:17 PM UTC 2025
- **Polars version**: polars
- **Rust version**: rustc 1.87.0 (17067e9ac 2025-05-09)
- **Build machine**: Linux fedora 6.15.5-200.fc42.x86_64 #1 SMP PREEMPT_DYNAMIC Sun Jul  6 09:16:17 UTC 2025 x86_64 GNU/Linux

## Usage

After installation, you can use go-polars in your Go projects. Make sure the library is in your library path or use the provided Go bindings that handle loading automatically.
