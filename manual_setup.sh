#!/bin/bash

echo "========================================"
echo "   MANUAL GIT-CRYPT SETUP"
echo "   (Key file in same folder)"
echo "========================================"
echo

# Check if we're in a git repository
if [ ! -d ".git" ]; then
    echo "ERROR: Not in a git repository"
    echo "Please navigate to the property-scraper directory first"
    echo
    exit 1
fi

# Check if key file exists
if [ ! -f "git-crypt-key" ]; then
    echo "ERROR: git-crypt-key file not found"
    echo
    echo "Please place the git-crypt-key file in this directory"
    echo "The key file should be in the same folder as this script"
    echo
    echo "Expected location: $(pwd)/git-crypt-key"
    echo
    exit 1
fi

echo "Found git-crypt-key file"
echo

# Check if git-crypt is installed
if ! command -v git-crypt &> /dev/null; then
    echo "ERROR: git-crypt not found"
    echo
    echo "Please install git-crypt:"
    echo "  macOS: brew install git-crypt"
    echo "  Ubuntu/Debian: sudo apt-get install git-crypt"
    echo "  Or download manually from: https://github.com/AGWA/git-crypt/releases"
    echo
    echo "For detailed instructions, see: docs/manual-git-crypt-setup.md"
    echo
    exit 1
fi

echo "Found git-crypt: $(which git-crypt)"
echo

# Set proper permissions on key file
chmod 600 git-crypt-key

# Check current encryption status
echo "Checking current encryption status..."
if git-crypt status --quiet 2>/dev/null; then
    echo "Repository is already unlocked"
    echo
    echo "Current status:"
    git-crypt status | grep "encrypted"
    echo
    echo "Data and config files should be accessible now"
    echo
    exit 0
fi

echo "Repository is locked, attempting to unlock..."
echo

# Attempt to unlock the repository
if ! git-crypt unlock git-crypt-key; then
    echo "ERROR: Failed to unlock repository"
    echo
    echo "Possible issues:"
    echo "1. Wrong key file"
    echo "2. Corrupted key file"
    echo "3. Repository not properly initialized"
    echo
    exit 1
fi

echo
echo "SUCCESS: Repository unlocked successfully!"
echo

# Show encryption status
echo "Current encryption status:"
git-crypt status | grep "encrypted"
echo

# Check if data files are accessible
echo "Checking data files..."
if [ -d "data/01_raw" ]; then
    echo "Found data directory"
    if ls data/01_raw/*.csv 1> /dev/null 2>&1; then
        echo "Data files are accessible"
        echo "Found $(ls data/01_raw/*.csv | wc -l) CSV files in data/01_raw/"
    else
        echo "Warning: No CSV files found in data/01_raw/"
    fi
else
    echo "Warning: data/01_raw directory not found"
fi

# Check if config files are accessible
echo "Checking config files..."
if [ -d "conf/base" ]; then
    echo "Found config directory"
    if ls conf/base/*.yml 1> /dev/null 2>&1; then
        echo "Config files are accessible"
        echo "Found $(ls conf/base/*.yml | wc -l) YML files in conf/base/"
    else
        echo "Warning: No YML files found in conf/base/"
    fi
else
    echo "Warning: conf/base directory not found"
fi

echo
echo "========================================"
echo "SETUP COMPLETE"
echo "========================================"
echo
echo "You can now access your data and config files normally"
echo "The repository will remain unlocked until you run: git-crypt lock"
echo
echo "For future reference, see: docs/manual-git-crypt-setup.md"
echo 