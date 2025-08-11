# Manual Git-Crypt Setup Guide

This guide provides step-by-step instructions for manually setting up git-crypt on different operating systems without automated tools.

## Prerequisites

- Git installed on your system
- Basic command line knowledge
- Access to download files from the internet

## Operating System Specific Instructions

### Windows (No Admin Rights)

#### Method 1: Direct Download (Recommended)

1. **Download git-crypt:**
   - Go to: https://github.com/AGWA/git-crypt/releases
   - Find the latest release (e.g., v0.7.0)
   - Download: `git-crypt-0.7.0-windows-x64.zip`

2. **Extract the file:**
   - Right-click the zip file → "Extract All..."
   - Extract to a folder (e.g., `C:\temp\git-crypt`)
   - You should see `git-crypt.exe` in the extracted folder

3. **Clone your repository:**
   ```cmd
   git clone <your-repository-url>
   cd property-scraper
   ```

4. **Place the key file:**
   - Copy `git-crypt-key` to the repository root
   - The key file should be in the same folder as the `.git` folder

5. **Decrypt using full path:**
   ```cmd
   C:\temp\git-crypt\git-crypt.exe unlock git-crypt-key
   ```

#### Method 2: Add to User PATH

1. **Download and extract git-crypt** (same as Method 1)

2. **Create a user bin directory:**
   ```cmd
   mkdir %USERPROFILE%\bin
   ```

3. **Copy git-crypt.exe:**
   ```cmd
   copy C:\temp\git-crypt\git-crypt.exe %USERPROFILE%\bin\
   ```

4. **Add to user PATH:**
   - Press `Win + R`, type `sysdm.cpl`, press Enter
   - Click "Environment Variables"
   - Under "User variables", find "Path" and click "Edit"
   - Click "New" and add: `%USERPROFILE%\bin`
   - Click OK on all dialogs
   - Restart Command Prompt

5. **Clone and decrypt:**
   ```cmd
   git clone <your-repository-url>
   cd property-scraper
   # Place git-crypt-key file here
   git-crypt unlock git-crypt-key
   ```

### macOS

#### Method 1: Using Homebrew (Admin Rights Required)

1. **Install Homebrew** (if not already installed):
   ```bash
   /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
   ```

2. **Install git-crypt:**
   ```bash
   brew install git-crypt
   ```

3. **Clone and decrypt:**
   ```bash
   git clone <your-repository-url>
   cd property-scraper
   # Place git-crypt-key file here
   git-crypt unlock git-crypt-key
   ```

#### Method 2: Manual Installation (No Admin Rights)

1. **Download git-crypt:**
   - Go to: https://github.com/AGWA/git-crypt/releases
   - Download: `git-crypt-0.7.0-darwin-x64.tar.gz`

2. **Extract and install:**
   ```bash
   tar -xzf git-crypt-0.7.0-darwin-x64.tar.gz
   sudo cp git-crypt /usr/local/bin/
   # If no sudo access, copy to user directory:
   mkdir -p ~/bin
   cp git-crypt ~/bin/
   echo 'export PATH="$HOME/bin:$PATH"' >> ~/.zshrc
   source ~/.zshrc
   ```

3. **Clone and decrypt:**
   ```bash
   git clone <your-repository-url>
   cd property-scraper
   # Place git-crypt-key file here
   git-crypt unlock git-crypt-key
   ```

### Linux (Ubuntu/Debian)

#### Method 1: Package Manager (Admin Rights Required)

1. **Install git-crypt:**
   ```bash
   sudo apt-get update
   sudo apt-get install git-crypt
   ```

2. **Clone and decrypt:**
   ```bash
   git clone <your-repository-url>
   cd property-scraper
   # Place git-crypt-key file here
   git-crypt unlock git-crypt-key
   ```

#### Method 2: Manual Installation (No Admin Rights)

1. **Download git-crypt:**
   ```bash
   wget https://github.com/AGWA/git-crypt/releases/download/0.7.0/git-crypt-0.7.0-linux-x64.tar.gz
   ```

2. **Extract and install:**
   ```bash
   tar -xzf git-crypt-0.7.0-linux-x64.tar.gz
   mkdir -p ~/bin
   cp git-crypt ~/bin/
   echo 'export PATH="$HOME/bin:$PATH"' >> ~/.bashrc
   source ~/.bashrc
   ```

3. **Clone and decrypt:**
   ```bash
   git clone <your-repository-url>
   cd property-scraper
   # Place git-crypt-key file here
   git-crypt unlock git-crypt-key
   ```

## Step-by-Step Setup Process

### Step 1: Prepare Your Environment

1. **Ensure Git is installed:**
   ```bash
   git --version
   ```

2. **Create a working directory:**
   ```bash
   mkdir ~/projects
   cd ~/projects
   ```

### Step 2: Download and Install git-crypt

Follow the instructions for your operating system above.

### Step 3: Clone the Repository

```bash
git clone <your-repository-url>
cd property-scraper
```

### Step 4: Place the Key File

1. **Copy the key file** to the repository root:
   ```bash
   # The key file should be in the same folder as .git
   ls -la
   # You should see: .git/  git-crypt-key
   ```

2. **Set proper permissions** (Linux/macOS):
   ```bash
   chmod 600 git-crypt-key
   ```

### Step 5: Decrypt the Repository

```bash
git-crypt unlock git-crypt-key
```

### Step 6: Verify Decryption

```bash
# Check encryption status
git-crypt status

# Verify data files are accessible
ls -la data/01_raw/

# Verify config files are accessible
ls -la conf/base/

# Try reading a file
head -5 data/01_raw/*.csv
```

## Troubleshooting

### Common Issues and Solutions

#### "git-crypt: command not found"

**Windows:**
- Use the full path: `C:\path\to\git-crypt.exe unlock git-crypt-key`
- Or add git-crypt to your PATH

**macOS/Linux:**
- Check if git-crypt is in your PATH: `which git-crypt`
- If not, use the full path: `/path/to/git-crypt unlock git-crypt-key`

#### "Failed to unlock repository"

**Possible causes:**
1. Wrong key file
2. Corrupted key file
3. Repository not properly initialized

**Solutions:**
1. Verify the key file is correct and complete
2. Check file permissions (should be 600 on Unix systems)
3. Ensure you're in the repository root directory
4. Try: `git-crypt status` to check repository state

#### "Permission denied"

**Windows:**
- Run Command Prompt as Administrator
- Or use a different directory for git-crypt

**macOS/Linux:**
- Check file permissions: `ls -la git-crypt-key`
- Set correct permissions: `chmod 600 git-crypt-key`

#### "Repository is already unlocked"

This is normal if the repository was previously unlocked. You can:
- Continue working normally
- Or lock it: `git-crypt lock`
- Then unlock again: `git-crypt unlock git-crypt-key`

### Verification Commands

```bash
# Check git-crypt installation
git-crypt --version

# Check repository status
git-crypt status

# List encrypted files
git-crypt ls-files

# Check if files are readable
file data/01_raw/*.csv
file conf/base/*.yml
```

## Security Best Practices

### Key Management
- **Keep the key file secure** - This is your master key
- **Don't share the key** with unauthorized users
- **Backup the key** in multiple secure locations
- **Use secure channels** to transfer the key (encrypted email, password managers)

### File Permissions
- **Set restrictive permissions** on the key file:
  ```bash
  chmod 600 git-crypt-key  # Unix systems
  ```
- **Don't commit the key** to version control (it's in .gitignore)

### Working with Encrypted Files
- **Lock the repository** when not in use: `git-crypt lock`
- **Unlock when needed**: `git-crypt unlock git-crypt-key`
- **Check status regularly**: `git-crypt status`

## Advanced Usage

### Locking and Unlocking
```bash
# Lock repository (encrypt all files)
git-crypt lock

# Unlock repository (decrypt files)
git-crypt unlock git-crypt-key

# Check status
git-crypt status
```

### Adding New Files
```bash
# New files are automatically encrypted when committed
git add new_file.csv
git commit -m "Add new file"
```

### Working with Multiple Repositories
```bash
# Each repository needs its own key file
# Copy the key file to each repository root
cp git-crypt-key /path/to/other/repo/
cd /path/to/other/repo/
git-crypt unlock git-crypt-key
```

## Support and Resources

### Official Documentation
- [git-crypt GitHub Repository](https://github.com/AGWA/git-crypt)
- [git-crypt Documentation](https://www.agwa.name/projects/git-crypt/)

### Getting Help
- Check the troubleshooting section above
- Review the official documentation
- Search for issues on the git-crypt GitHub repository
- Contact your system administrator for installation help

### Alternative Installation Methods
- **Using WSL** (Windows Subsystem for Linux)
- **Using Docker** (if available)
- **Using Git Bash** (Windows)
- **Using package managers** (apt, yum, brew, etc.) 