# go-polars Linux Release v0.0.16

## Binary Information

- **File**: `libpolars_go-linux-amd64-v0.0.16.so`
- **Platform**: Linux x86_64
- **Size**: 46M
- **SHA256**: `1cafec92ea06a49d5e5a3657c081adee2d493a524ee00d82468974d9b90512c9`
- **MD5**: `97861686f6e57fe111e4d24b31a97805`

## Installation

1. Download the binary file
2. Verify the checksum:
   ```bash
   sha256sum -c libpolars_go-linux-amd64-v0.0.16.so.sha256
   ```
3. Copy to your project:
   ```bash
   mkdir -p polars/bin
   cp libpolars_go-linux-amd64-v0.0.16.so polars/bin/libpolars_go.so
   ```

## Build Information

- **Built on**: Sat Jul 19 06:48:07 PM UTC 2025
- **Polars version**: polars
- **Rust version**: rustc 1.87.0 (17067e9ac 2025-05-09)
- **Build machine**: Linux fedora 6.15.6-200.fc42.x86_64 #1 SMP PREEMPT_DYNAMIC Thu Jul 10 15:22:32 UTC 2025 x86_64 GNU/Linux

## Usage

After installation, you can use go-polars in your Go projects. Make sure the library is in your library path or use the provided Go bindings that handle loading automatically.
