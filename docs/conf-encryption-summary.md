# Configuration Directory Encryption Summary

## Overview
The `conf/` directory is now encrypted with git-crypt alongside the `data/` directory. This ensures that all configuration files are protected and synchronized across authorized users.

## What's Now Encrypted in conf/

### Base Configuration (`conf/base/`)
- `catalog.yml` - Data catalog configuration
- `parameters.yml` - Project parameters
- `logging.yml` - Logging configuration
- `spark.yml` - Spark configuration
- Any future YAML, JSON, INI, or CFG files

### Local Configuration (`conf/local/`)
- `.gitkeep` - Directory placeholder
- Any user-specific configuration files
- Local environment settings

### Documentation
- `README.md` - Configuration documentation

## Benefits of Encrypting conf/

1. **Complete Project Synchronization** - All project files (data + config) are now in sync
2. **Team Collaboration** - Configuration changes are shared securely
3. **Environment Consistency** - All team members have the same configuration
4. **Version Control** - Configuration changes are tracked and versioned
5. **Security** - Sensitive configuration data is protected

## What's Still Excluded

The following patterns are still excluded from git (not encrypted, just ignored):
- `conf/**/*credentials*` - Credential files
- `conf/**/*secret*` - Secret files
- `conf/**/*password*` - Password files
- `conf/**/*token*` - Token files
- `conf/**/*key*` - Key files

## Usage

### Adding New Configuration Files
```bash
# New config files are automatically encrypted
git add conf/new_config.yml
git commit -m "Add new configuration"
```

### Working with Configuration
- Configuration files are automatically decrypted when repository is unlocked
- Changes are automatically encrypted when committed
- All authorized users will have access to the same configuration

### Decryption Process
The same decryption process works for both data and config:
```bash
git-crypt unlock git-crypt-key
```

## File Extensions Encrypted
- `.yml` / `.yaml` - YAML configuration files
- `.json` - JSON configuration files
- `.ini` - INI configuration files
- `.cfg` - Configuration files
- All files in `conf/` directory

## Security Considerations

1. **Sensitive Data** - Don't put actual credentials, passwords, or tokens in encrypted files
2. **Template Approach** - Use template files with placeholder values
3. **Environment Variables** - Use environment variables for truly sensitive data
4. **Key Management** - Same key file decrypts both data and config

## Migration Notes

- Existing configuration files have been automatically encrypted
- No changes needed to existing workflows
- All team members will need the same git-crypt-key file
- Configuration files are now part of the secure repository sync

## Verification

To verify configuration files are properly encrypted/decrypted:
```bash
# Check encryption status
git-crypt status

# Verify config files are accessible
ls -la conf/base/
cat conf/base/parameters.yml  # Should be readable when unlocked
``` 