#!/bin/bash
# Script to build the mooncake wheel package
# Usage: ./scripts/build_wheel.sh

set -e  # Exit immediately if a command exits with a non-zero status

# Ensure LD_LIBRARY_PATH includes /usr/local/lib
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib

echo "Creating directory structure..."
mkdir -p mooncake-wheel/mooncake/lib_so/
mkdir -p mooncake-wheel/mooncake/transfer/

echo "Copying Python modules..."
# Copy mooncake_vllm_adaptor to root level for backward compatibility
cp build/mooncake-integration/mooncake_vllm_adaptor.*.so mooncake-wheel/mooncake/mooncake_vllm_adaptor.so
cp build/mooncake-integration/mooncake_sglang_adaptor.*.so mooncake-wheel/mooncake/mooncake_sglang_adaptor.so

# Copy engine.so to mooncake directory (will be imported by transfer module)
cp build/mooncake-integration/engine.*.so mooncake-wheel/mooncake/engine.so

echo "Copying master binary and shared libraries..."
# Copy master binary and shared libraries
cp build/mooncake-store/src/mooncake_master mooncake-wheel/mooncake/

WHITELISTED_LIBS=(
    "libgflags.so"
    "libjsoncpp.so"
    "libunwind.so"
    "libzstd.so"
    "libglog.so"
    "libetcd_wrapper.so"
)

TARGET_DIR="mooncake-wheel/mooncake/lib_so"
mkdir -p "$TARGET_DIR" # Ensure target directory exists
# --- Define common library paths to check ---
SEARCH_PATHS=(
  /usr/local/lib
  /usr/lib
  /lib
  /usr/lib64 
  /lib64  
  /lib/x86_64-linux-gnu/
)
# -----------------------------------------
echo "Copying whitelisted libraries from multiple paths..."
for lib in "${WHITELISTED_LIBS[@]}"; do
  found="false" # Flag to track if lib was found
  for search_dir in "${SEARCH_PATHS[@]}"; do
    lib_path="$search_dir/$lib"
    if [ -f "$lib_path" ]; then
      echo "Found $lib in $search_dir. Copying..."
      # Use -L to copy the actual file, not a symlink if it is one
      cp -L "$lib_path" "$TARGET_DIR/" || echo "Error copying $lib_path"
      found="true"
      break # Stop searching other paths once found
    fi
  done
  if [ "$found" = "false" ]; then
    echo "Warning: $lib not found in any specified search paths: ${SEARCH_PATHS[*]}"
    exit 1
  fi
done

echo "Library copying process finished."

# Set RPATH for all binaries and shared objects
patchelf --set-rpath '$ORIGIN/lib_so' --force-rpath mooncake-wheel/mooncake/mooncake_master
patchelf --set-rpath '$ORIGIN/lib_so' --force-rpath mooncake-wheel/mooncake/*.so


echo "Building wheel package..."
# Build the wheel package
cd mooncake-wheel

echo "Building wheel with default version"
python -m build


cd ..

echo "Wheel package built successfully!"
