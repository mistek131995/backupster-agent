#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROJECT_PATH="${ROOT_DIR}/BackupsterAgent/BackupsterAgent.csproj"
CONFIGURATION="${CONFIGURATION:-Release}"
RUNTIME="${RUNTIME:-linux-x64}"
PUBLISH_DIR="${ROOT_DIR}/publish/${RUNTIME}"
ARTIFACTS_DIR="${ARTIFACTS_DIR:-${ROOT_DIR}/artifacts}"
NFPM_VERSION="${NFPM_VERSION:-2.41.0}"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

msbuild_property() {
  local property_name="$1"
  local value
  value="$(dotnet msbuild "${PROJECT_PATH}" -nologo "-getProperty:${property_name}")"
  printf '%s\n' "${value}" | awk 'NF { line = $0 } END { gsub(/^[ \t]+|[ \t]+$/, "", line); print line }'
}

ensure_nfpm() {
  if command -v nfpm >/dev/null 2>&1; then
    NFPM_BIN="$(command -v nfpm)"
    return
  fi

  require_command curl
  require_command tar

  local machine
  machine="$(uname -m)"
  if [ "${machine}" != "x86_64" ] && [ "${machine}" != "amd64" ]; then
    echo "nfpm auto-download supports only x86_64/amd64 Linux. Install nfpm manually for ${machine}." >&2
    exit 1
  fi

  local tools_dir="${ROOT_DIR}/.tools/nfpm-${NFPM_VERSION}"
  local archive="${tools_dir}/nfpm.tar.gz"
  mkdir -p "${tools_dir}"

  if [ ! -x "${tools_dir}/nfpm" ]; then
    echo "Downloading nfpm ${NFPM_VERSION}..."
    curl -fsSL "https://github.com/goreleaser/nfpm/releases/download/v${NFPM_VERSION}/nfpm_${NFPM_VERSION}_Linux_x86_64.tar.gz" -o "${archive}"
    tar -xzf "${archive}" -C "${tools_dir}" nfpm
    chmod +x "${tools_dir}/nfpm"
  fi

  NFPM_BIN="${tools_dir}/nfpm"
}

require_command dotnet
require_command zip

VERSION="$(msbuild_property Version)"
if [ -z "${VERSION}" ]; then
  echo "Project Version is empty: ${PROJECT_PATH}" >&2
  exit 1
fi

echo "Building BackupsterAgent ${VERSION} for ${RUNTIME}..."

rm -rf "${PUBLISH_DIR}"
mkdir -p "${PUBLISH_DIR}" "${ARTIFACTS_DIR}"

dotnet publish "${PROJECT_PATH}" \
  -c "${CONFIGURATION}" \
  -r "${RUNTIME}" \
  --self-contained true \
  -p:PublishSingleFile=true \
  -p:Version="${VERSION}" \
  -p:InformationalVersion="${VERSION}" \
  -o "${PUBLISH_DIR}"

ZIP_PATH="${ARTIFACTS_DIR}/BackupsterAgent-v${VERSION}-${RUNTIME}.zip"
rm -f "${ZIP_PATH}"
(
  cd "${PUBLISH_DIR}"
  zip -qr "${ZIP_PATH}" .
)
echo "Created ${ZIP_PATH}"

if [ "${SKIP_DEB:-0}" != "1" ]; then
  ensure_nfpm
  (
    cd "${ROOT_DIR}"
    VERSION="${VERSION}" "${NFPM_BIN}" pkg --packager deb --config packaging/nfpm.yaml --target "${ARTIFACTS_DIR}/"
  )
  echo "Created ${ARTIFACTS_DIR}/backupster-agent_${VERSION}_amd64.deb"
fi
