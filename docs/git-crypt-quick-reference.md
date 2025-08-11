# Git-Crypt Quick Reference

## Setup on New Computer
```bash
# 1. Install git-crypt
brew install git-crypt  # macOS
sudo apt-get install git-crypt  # Ubuntu/Debian

# 2. Clone repository
git clone <repo-url>
cd property-scraper

# 3. Unlock with key
git-crypt unlock git-crypt-key
```

## Daily Commands
```bash
# Check encryption status
git-crypt status

# Add new data files (auto-encrypted)
git add data/new_file.csv
git commit -m "Add new data"

# Add new config files (auto-encrypted)
git add conf/new_config.yml
git commit -m "Add new config"

# Lock repository (encrypt all)
git-crypt lock

# Unlock repository (decrypt all)
git-crypt unlock git-crypt-key
```

## Key Management
```bash
# Export key for sharing
git-crypt export-key git-crypt-key

# Change key (emergency)
git-crypt rekey
git-crypt export-key new-key
```

## Troubleshooting
```bash
# Files appear encrypted
git-crypt unlock git-crypt-key

# Key import fails
chmod 600 git-crypt-key
git-crypt unlock git-crypt-key

# New files not encrypted
git-crypt status -f
```

## Security Notes
- Keep `git-crypt-key` secure and never commit it
- Share key only with authorized users
- Backup key in multiple secure locations
- Rotate keys when team changes 