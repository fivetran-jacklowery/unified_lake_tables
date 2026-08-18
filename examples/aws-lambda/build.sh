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
# IMPORTANT -- pyarrow ships compiled, platform-specific binaries, and
# Lambda's runtime is Linux. A plain `pip install` on this same machine
# only produces a Lambda-compatible package if this machine IS Linux
# matching your function's architecture. On any other OS (macOS, Windows),
# `pip install -t build` happily installs THAT OS's wheels instead --
# the build finishes and even a local `python3 -c "import pyarrow"` sanity
# check on that same machine passes, because it's importing a native
# build for the machine it's running on. It will still fail the moment
# Lambda actually invokes it. This was hit for real building this exact
# example on a Mac, not a hypothetical.
#
# So: this script uses Docker (the official Lambda build image) to do BOTH
# the dependency install and the import sanity-check, whenever Docker is
# available -- not just the install -- specifically so a passing sanity
# check here actually means something. It only falls back to a plain host
# `pip install` if Docker isn't available, and refuses to silently trust
# that fallback's sanity check if the host isn't Linux.
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
LAMBDA_BUILD_IMAGE="public.ecr.aws/sam/build-python3.12:latest-arm64"

rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

SANITY_CHECK='
import pyarrow, pyarrow.parquet, pyarrow.dataset
import pyiceberg
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.manifest import DataFile, DataFileContent
import register_consolidation
print("OK: package imports cleanly, pyarrow", pyarrow.__version__)
'

if command -v docker >/dev/null 2>&1; then
  echo "Docker found -- installing dependencies inside $LAMBDA_BUILD_IMAGE"
  echo "(guarantees a Linux/arm64-correct build regardless of this machine's own OS) ..."
  docker run --rm -v "$HERE":/var/task -w /var/task "$LAMBDA_BUILD_IMAGE" \
    pip install -r requirements.txt -t build --quiet
  USED_DOCKER=1
else
  echo "Docker not found -- falling back to a plain host 'pip install'."
  echo "This ONLY produces a working Lambda package if this machine is"
  echo "Linux/arm64. Installing Docker and re-running this script is strongly"
  echo "recommended on macOS/Windows -- see this script's header comment."
  pip install -r "$HERE/requirements.txt" -t "$BUILD_DIR" --quiet
  USED_DOCKER=0
fi

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

if [ "$USED_DOCKER" = "1" ]; then
  echo "Sanity-checking the package imports cleanly INSIDE the Lambda build image"
  echo "(this is the check that actually matters -- a host-machine import check"
  echo "would pass even for a mismatched-platform build, so this runs in the"
  echo "same Linux/arm64 environment Lambda itself uses) ..."
  docker run --rm -v "$BUILD_DIR":/var/task -w /var/task "$LAMBDA_BUILD_IMAGE" \
    python3 -c "$SANITY_CHECK"
else
  HOST_OS="$(uname -s)"
  if [ "$HOST_OS" != "Linux" ]; then
    echo "SKIPPING sanity-check import: this machine is $HOST_OS, not Linux, and"
    echo "Docker isn't available. A local import check here would silently pass"
    echo "against THIS machine's own native pyarrow build and prove nothing about"
    echo "whether the package works on Lambda's Linux runtime -- running it would"
    echo "give false confidence, so this deliberately does not run it. Install"
    echo "Docker and re-run this script before deploying anything built this way."
  else
    echo "Sanity-checking the package imports cleanly ..."
    ( cd "$BUILD_DIR" && python3 -c "$SANITY_CHECK" )
  fi
fi

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
