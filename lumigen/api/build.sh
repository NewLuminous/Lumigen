#!/bin/bash

# ==============================================================================
# Docker Build Script for the Lumigen Application
# ==============================================================================
#
# This script securely builds the Docker image for the application.
#
# It is designed to be location-independent. You can run it from any directory,
# as it intelligently locates the project root. It will create a temporary
# staging area if an external config directory is provided, ensuring a clean
# and secure build process.
#
# --- USAGE ---
#
# ./api/build.sh [OPTIONS]
#
# --- OPTIONS ---
#
#   -n, --name      The name for the Docker image.
#                   (Default: newluminous/lumigen)
#
#   -t, --tag       The tag for the Docker image.
#                   (Default: latest)
#
#   -c, --conf-dir  ABSOLUTE or RELATIVE path to an external Spark conf directory.
#                   This path is NOT inside the project's git repository.
#                   (Default: None)
#
#   -h, --help      Show this help message.
#
# --- EXAMPLES ---
#
# 1. Basic build with default name and tag:
#    (Run from the project root directory 'lumigen/')
#    ./api/build.sh
#
# 2. Build with a custom image name and tag:
#    ./api/build.sh --name my-registry/my-app --tag v1.2.0
#
# 3. Build using a specific Spark configuration for production:
#    (Assumes your production configs are in a secure location, e.g., your home directory)
#    ./api/build.sh -t prod-latest -c ~/secure-configs/spark-production
#
# 4. Build from a different directory:
#    (Assuming you are in your home directory '~')
#    bash path/to/your/project/api/build.sh
#
# ==============================================================================

# Exit immediately if a command exits with a non-zero status.
set -e

# --- Default Configuration ---
DEFAULT_IMAGE_NAME="newluminous/lumigen"
DEFAULT_IMAGE_TAG="latest"
EXTERNAL_SPARK_CONF_DIR=""

# --- Script Initialization ---
# This block makes the script runnable from any location.
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_ROOT=$(realpath "${SCRIPT_DIR}/..")
DOCKERFILE_PATH="api/Dockerfile"
TMP_CONF_DIR_NAME=".tmp_spark_conf"
TMP_CONF_DIR_PATH="${PROJECT_ROOT}/${TMP_CONF_DIR_NAME}"

# --- Helper Functions ---
usage() {
    # The 'cat << EOF' syntax is a "here document", great for multi-line strings.
    cat << EOF
Usage: $0 [OPTIONS]

This script securely builds the Docker image for the Lumigen application.

Options:
  -n, --name      Image name (Default: ${DEFAULT_IMAGE_NAME})
  -t, --tag       Image tag (Default: ${DEFAULT_IMAGE_TAG})
  -c, --conf-dir  Path to an EXTERNAL Spark conf dir (Default: None)
  -h, --help      Display this help message

Examples:
  # Basic build
  $0

  # Custom tag
  $0 -t my-feature-branch

  # Provide external production configuration
  $0 -t prod-v1 -c /etc/spark/conf.d/production
EOF
}

# --- Cleanup Function ---
cleanup() {
    echo "---"
    echo "Executing cleanup..."
    if [ -d "${TMP_CONF_DIR_PATH}" ]; then
        echo "Removing temporary directory: ${TMP_CONF_DIR_PATH}"
        rm -rf "${TMP_CONF_DIR_PATH}"
    fi
    echo "Cleanup complete."
}

# 'trap' ensures the 'cleanup' function runs on script exit (success, error, or Ctrl+C).
trap cleanup EXIT

# --- Parse Command-Line Arguments ---
IMAGE_NAME="${DEFAULT_IMAGE_NAME}"
IMAGE_TAG="${DEFAULT_IMAGE_TAG}"

while [[ "$#" -gt 0 ]]; do
    case $1 in
        -n|--name)
            if [[ -z "$2" || "$2" == -* ]]; then echo "Error: --name requires a value." >&2; exit 1; fi
            IMAGE_NAME="$2"; shift ;;
        -t|--tag)
            if [[ -z "$2" || "$2" == -* ]]; then echo "Error: --tag requires a value." >&2; exit 1; fi
            IMAGE_TAG="$2"; shift ;;
        -c|--conf-dir)
            if [[ -z "$2" || "$2" == -* ]]; then echo "Error: --conf-dir requires a value." >&2; exit 1; fi
            EXTERNAL_SPARK_CONF_DIR="$2"; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Unknown parameter passed: $1"; usage; exit 1 ;;
    esac
    shift
done

# --- Main Build Logic ---
DOCKER_BUILD_ARG_SPARK_CONF=""

if [ -n "${EXTERNAL_SPARK_CONF_DIR}" ]; then
    echo "Preparing external Spark configuration..."
    if [ ! -d "${EXTERNAL_SPARK_CONF_DIR}" ]; then
        echo "Error: External Spark configuration directory not found at '${EXTERNAL_SPARK_CONF_DIR}'" >&2
        exit 1
    fi
    echo "Creating temporary staging directory: ${TMP_CONF_DIR_PATH}"
    mkdir -p "${TMP_CONF_DIR_PATH}"
    echo "Copying files from '${EXTERNAL_SPARK_CONF_DIR}' to staging area..."
    cp -r "${EXTERNAL_SPARK_CONF_DIR}/." "${TMP_CONF_DIR_PATH}"
    DOCKER_BUILD_ARG_SPARK_CONF="${TMP_CONF_DIR_NAME}"
fi

FULL_IMAGE_NAME="${IMAGE_NAME}:${IMAGE_TAG}"

echo "================================================="
echo "Starting Docker Build"
echo "-------------------------------------------------"
echo "Project Root:      ${PROJECT_ROOT}"
echo "Dockerfile:        ${PROJECT_ROOT}/${DOCKERFILE_PATH}"
echo "Image to Build:    ${FULL_IMAGE_NAME}"
if [ -n "${DOCKER_BUILD_ARG_SPARK_CONF}" ]; then
    echo "Spark Conf Arg:    ${DOCKER_BUILD_ARG_SPARK_CONF} (from '${EXTERNAL_SPARK_CONF_DIR}')"
else
    echo "Spark Conf Arg:    Not provided."
fi
echo "================================================="
echo ""

cd "${PROJECT_ROOT}" || { echo "Failed to change to project root: ${PROJECT_ROOT}"; exit 1; }

docker build \
    -f "${DOCKERFILE_PATH}" \
    -t "${FULL_IMAGE_NAME}" \
    --build-arg SPARK_CONF_DIR="${DOCKER_BUILD_ARG_SPARK_CONF}" \
    .

# The 'trap' will automatically call the cleanup function upon exit.

echo ""
echo "================================================="
echo "✅ Docker build successful!"
echo "Image created: ${FULL_IMAGE_NAME}"
echo "================================================="