# Git-Crypt Setup and Usage Guide

## Overview
This project uses git-crypt to encrypt sensitive data files while keeping them in version control. This allows authorized users to access the data while keeping it secure from unauthorized access.

## What's Encrypted
- All files in the `data/` directory and its subdirectories
- All files in the `conf/` directory and its subdirectories
- Files with extensions: `.parquet`, `.csv`, `.xlsx`, `.json`, `.xls`, `.yml`, `.yaml`, `.ini`, `.cfg`
- Sensitive configuration files: `*.key`, `*.pem`, `*.p12`, `*.pfx`, `secrets.*`, `.env*`

## Current Setup Status
✅ git-crypt initialized  
✅ .gitattributes configured  
✅ .gitignore updated  
✅ Data files staged for encryption  
✅ Encryption key exported  

## For Current Computer (Already Set Up)
Your current computer is already configured and can decrypt files automatically. When you clone or pull the repository, files will be automatically decrypted.

## For Other Computers (New Setup)

### Prerequisites
1. Install git-crypt:
   ```bash
   # macOS
   brew install git-crypt
   
   # Ubuntu/Debian
   sudo apt-get install git-crypt
   
   # Windows (using Chocolatey)
   choco install git-crypt
   ```

### Step 1: Clone the Repository
```bash
git clone <your-repository-url>
cd property-scraper
```

### Step 2: Import the Encryption Key
You need the `git-crypt-key` file that was exported from the original setup. Place it in the repository root and run:

```bash
git-crypt unlock git-crypt-key
```

### Step 3: Verify Decryption
Check that files are decrypted:
```bash
ls -la data/01_raw/
# You should see readable files, not encrypted blobs
```

## Security Best Practices

### 1. Key Management
- **Store the `git-crypt-key` file securely** - This is your master key
- **Never commit the key file** - It should be in `.gitignore`
- **Share the key only with authorized users** - Use secure channels (encrypted email, password managers, etc.)
- **Backup the key** - Store it in multiple secure locations

### 2. Access Control
- Only share the encryption key with team members who need access
- Consider using GPG keys for additional security (advanced setup)
- Regularly rotate keys if team membership changes

### 3. Repository Security
- Ensure the repository itself is private/restricted
- Use strong authentication for repository access
- Monitor repository access logs

## Troubleshooting

### Files Appear Encrypted
If files appear as binary blobs instead of readable content:
```bash
git-crypt status
git-crypt unlock git-crypt-key
```

### Key Import Issues
If the key import fails:
1. Verify the key file is in the repository root
2. Check file permissions: `chmod 600 git-crypt-key`
3. Try unlocking again: `git-crypt unlock git-crypt-key`

### New Files Not Encrypted
If new files aren't being encrypted:
1. Check `.gitattributes` configuration
2. Ensure files match the patterns in `.gitattributes`
3. Re-add files: `git add <file>` and commit

## Commands Reference

### Basic Operations
```bash
# Check encryption status
git-crypt status

# Lock repository (encrypt all files)
git-crypt lock

# Unlock repository (decrypt files)
git-crypt unlock git-crypt-key

# Export key for sharing
git-crypt export-key git-crypt-key

# Show which files are encrypted
git-crypt ls-files
```

### Adding New Files
```bash
# Add new data files (they'll be automatically encrypted)
git add data/new_file.csv
git commit -m "Add new data file"
```

### Working with Encrypted Files
- Files are automatically encrypted when committed
- Files are automatically decrypted when checked out (if unlocked)
- You can work with files normally when the repository is unlocked

## Emergency Procedures

### If Key is Lost
If the encryption key is lost, encrypted files cannot be recovered. Always maintain secure backups of the key.

### If Repository is Compromised
1. Generate a new key: `git-crypt rekey`
2. Export the new key: `git-crypt export-key new-key`
3. Share the new key with authorized users
4. Force push the changes: `git push --force`

### Repository Recovery
If you need to recover the repository on a new machine:
1. Clone the repository
2. Import the encryption key
3. Unlock the repository
4. Verify all files are accessible

## Team Collaboration

### Adding New Team Members
1. Share the `git-crypt-key` file securely
2. Have them follow the "For Other Computers" setup
3. Verify they can access the data

### Removing Team Members
1. Generate a new key: `git-crypt rekey`
2. Export and share the new key with remaining team members
3. Force push the changes to invalidate the old key

## Monitoring and Maintenance

### Regular Checks
- Verify encryption is working: `git-crypt status`
- Check for unauthorized access attempts
- Review team access permissions

### Updates
- Keep git-crypt updated: `brew upgrade git-crypt`
- Monitor for security updates
- Review and update access controls regularly

## Support
For issues with git-crypt setup or usage, refer to:
- [git-crypt documentation](https://github.com/AGWA/git-crypt)
- [git-crypt man pages](https://www.agwa.name/projects/git-crypt/)
- Contact your system administrator or security team 