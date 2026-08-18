#!/usr/bin/env bash
# Build a deployment .zip for this Lambda example.
#
# Packages three things together into one flat directory, then zips it:
#   1. This example's own lambda_function.py
#   2. scripts/register_consolidation.py, copied in from the repo root so
#      there's a single source of truth for the actual consolidation logic
#      instead of a second, drifting copy living under examples/
#   3. This example's pinned dependencies (requirements.txt), installed
#      directly into the package directory
#
# IMPORTANT -- build on (or for) the same architecture as your Lambda
# function. pyarrow ships compiled, platform-specific binaries. The
# straightforward `pip install` below only produces a correct package if
# you run this script on Linux matching your function's configured
# architecture (arm64 is recommended -- see README.md). If you're building
# on macOS or Windows, or want a guaranteed-correct build regardless of
# your own machine, build inside the official Lambda base image instead:
#
#   docker run --rm -v "$PWD":/var/task -w /var/task \
#     public.ecr.aws/sam/build-python3.12:latest-arm64 \
#     pip install -r requirements.txt -t build
#
# then run the rest of this script's copy/trim/zip steps as-is.
#
# Usage:
#   ./build.sh
#   aws lambda create-function ... (see README.md -- this package is too
#   large for direct --zip-file upload; it needs the S3 upload path)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"
BUILD_DIR="$HERE/build"
ZIP_PATH="$HERE/deployment.zip"

rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

echo "Installing dependencies into $BUILD_DIR ..."
pip install -r "$HERE/requirements.txt" -t "$BUILD_DIR" --quiet

echo "Copying handler and shared consolidation script ..."
cp "$HERE/lambda_function.py" "$BUILD_DIR/"
cp "$REPO_ROOT/scripts/register_consolidation.py" "$BUILD_DIR/"

UNTRIMMED_SIZE=$(du -sh "$BUILD_DIR" | cut -f1)

echo "Trimming unused pyarrow components (safe -- tested against a real"
echo "import of everything this tool actually uses; do NOT also remove"
echo "pyarrow/libarrow_substrait.so.2500 -- pyarrow's own core import"
echo "depends on it at load time even though this tool never calls"
echo "Substrait functionality directly) ..."
# Trimming is a best-effort size optimization, not a correctness requirement --
# the untrimmed package works fine on its own, just larger. Don't let a
# restrictive filesystem (some CI runners, certain mounted/synced dev
# folders) that blocks deleting these specific files abort the whole build;
# just report at the end whether it actually took effect on this machine.
rm -rf "$BUILD_DIR/pyarrow/tests" "$BUILD_DIR/pyarrow/include" || true
rm -f "$BUILD_DIR"/pyarrow/libarrow_flight.so.* "$BUILD_DIR"/pyarrow/_flight*.so || true
find "$BUILD_DIR" -name "__pycache__" -type d -prune -exec rm -rf {} + 2>/dev/null || true

TRIMMED_SIZE=$(du -sh "$BUILD_DIR" | cut -f1)
if [ "$UNTRIMMED_SIZE" = "$TRIMMED_SIZE" ]; then
  echo "  NOTE: trim step ran but package size didn't change ($UNTRIMMED_SIZE)."
  echo "  This filesystem may not allow deleting these files (some CI runners"
  echo "  and synced/mounted dev folders block it) -- the untrimmed package"
  echo "  still works fine, just larger. See README.md for the real measured"
  echo "  before/after numbers from a normal filesystem."
else
  echo "  Trimmed: $UNTRIMMED_SIZE -> $TRIMMED_SIZE"
fi

echo "Sanity-checking the trimmed package still imports cleanly ..."
( cd "$BUILD_DIR" && python3 -c "
import pyarrow, pyarrow.parquet, pyarrow.dataset
import pyiceberg
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.manifest import DataFile, DataFileContent
import register_consolidation
print('OK: trimmed package imports cleanly, pyarrow', pyarrow.__version__)
" )

echo "Zipping ..."
rm -f "$ZIP_PATH"
if ( cd "$BUILD_DIR" && zip -r -X -q "$ZIP_PATH" . ) 2>/dev/null; then
  :
else
  echo "  (zip CLI unavailable or failed -- falling back to Python's zipfile module)"
  python3 -c "
import zipfile, os
build_dir, zip_path = '$BUILD_DIR', '$ZIP_PATH'
with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
    for root, _dirs, files in os.walk(build_dir):
        for f in files:
            full = os.path.join(root, f)
            zf.write(full, os.path.relpath(full, build_dir))
"
fi

ZIP_SIZE=$(du -h "$ZIP_PATH" | cut -f1)
UNZIPPED_SIZE=$(du -sh "$BUILD_DIR" | cut -f1)
echo "Built: $ZIP_PATH ($ZIP_SIZE zipped, $UNZIPPED_SIZE unzipped)"
echo "Lambda's direct --zip-file upload caps at 50MB zipped, so this package"
echo "must go through the S3 upload path regardless -- see README.md."
echo "Unzipped, it needs to stay under Lambda's 250MB hard ceiling -- check"
echo "the number above against that after any dependency version bump."
