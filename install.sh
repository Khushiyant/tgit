#!/bin/bash
set -e

# Configuration
OWNER="Khushiyant"
REPO="vekt"
BINARY_NAME="vekt"
INSTALL_DIR="/usr/local/bin"

# Detect OS and Architecture
OS="$(uname -s)"
ARCH="$(uname -m)"

case "$OS" in
    Linux)
        OS_TYPE="linux"
        ;;
    Darwin)
        OS_TYPE="macos"
        ;;
    *)
        echo "Unsupported OS: $OS"
        exit 1
        ;;
esac

case "$ARCH" in
    x86_64)
        ARCH_TYPE="amd64"
        ;;
    arm64|aarch64)
        if [ "$OS_TYPE" = "linux" ]; then
            echo "ARM64 Linux not yet supported in CI build" # Adjust if you add arm64 linux later
            exit 1
        fi
        ARCH_TYPE="arm64"
        ;;
    *)
        echo "Unsupported architecture: $ARCH"
        exit 1
        ;;
esac

ASSET_NAME="vekt-${OS_TYPE}-${ARCH_TYPE}"
DOWNLOAD_URL="https://github.com/${OWNER}/${REPO}/releases/latest/download/${ASSET_NAME}"

echo "Installing ${BINARY_NAME} for ${OS_TYPE}/${ARCH_TYPE}..."

# Download
if command -v curl >/dev/null 2>&1; then
    curl -fL -o "${BINARY_NAME}" "${DOWNLOAD_URL}"
elif command -v wget >/dev/null 2>&1; then
    wget -O "${BINARY_NAME}" "${DOWNLOAD_URL}"
else
    echo "Error: Neither curl nor wget was found."
    exit 1
fi

# Make executable
chmod +x "${BINARY_NAME}"

# Move to install directory (requires sudo)
echo "Moving binary to ${INSTALL_DIR} (may require password)..."
if [ -w "${INSTALL_DIR}" ]; then
    mv "${BINARY_NAME}" "${INSTALL_DIR}/${BINARY_NAME}"
else
    sudo mv "${BINARY_NAME}" "${INSTALL_DIR}/${BINARY_NAME}"
fi

echo "Success! Run 'vekt --help' to get started."