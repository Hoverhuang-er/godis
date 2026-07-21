#!/bin/bash
set -euo pipefail

# godis v1.3.0 Release Build Script
# Builds godis for all target platforms and packages as godis_${OS}_${ARCH}.zip

VERSION="1.3.0"
BUILD_DIR="build"
BINARY_NAME="godis"

# Target platforms: OS-ARCH
declare -a TARGETS=(
    "linux amd64"
    "linux arm64"
    "linux riscv64"
    "windows amd64"
    "windows arm64"
    "windows riscv64"
    "darwin amd64"
    "darwin arm64"
    "darwin riscv64"
)

rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}"

echo "=== godis v${VERSION} Release Build ==="
echo ""

LDFLAGS="-s -w -X github.com/hdt3213/godis/database.godisVersion=${VERSION}"

for target in "${TARGETS[@]}"; do
    IFS=' ' read -r os arch <<< "${target}"

    echo "Building ${os}/${arch}..."

    output_name="${BINARY_NAME}"
    if [ "${os}" = "windows" ]; then
        output_name="${BINARY_NAME}.exe"
    fi

    export GOOS="${os}"
    export GOARCH="${arch}"
    export CGO_ENABLED=0
    export GOEXPERIMENT=jsonv2

    go build -tags greenteagc -ldflags="${LDFLAGS}" -o "${BUILD_DIR}/${output_name}" .

    archive_name="godis_${os}_${arch}.zip"
    cd "${BUILD_DIR}"

    if [ "${os}" = "windows" ]; then
        zip "${archive_name}" "${output_name}"
    else
        zip "${archive_name}" "${output_name}"
    fi

    rm -f "${output_name}"

    cd ..

    echo "  -> godis_${os}_${arch}.zip"
    echo ""
done

echo "=== Build Complete ==="
echo ""

ls -lh "${BUILD_DIR}/"

echo ""
echo "SHA256 checksums:"
cd "${BUILD_DIR}"
shasum -a 256 *.zip | tee checksums.txt
echo ""
echo "All artifacts in: ${BUILD_DIR}/"
