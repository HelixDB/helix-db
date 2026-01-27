use std::fs;
use std::path::PathBuf;

use crate::config::HelixConfig;

/// Tracks resources created during init/add operations for automatic cleanup on failure
pub struct CleanupTracker {
    /// Files created during the operation (tracked in creation order)
    created_files: Vec<PathBuf>,
    /// Directories created during the operation (tracked in creation order)
    created_dirs: Vec<PathBuf>,
    /// In-memory backup of the config before modification
    original_config: Option<HelixConfig>,
    /// Path to the config file
    config_path: Option<PathBuf>,
}

/// Summary of cleanup operations
pub struct CleanupSummary {
    pub files_removed: usize,
    pub files_failed: usize,
    pub dirs_removed: usize,
    pub dirs_failed: usize,
    pub config_restored: bool,
    pub errors: Vec<String>,
}

impl Default for CleanupTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl CleanupTracker {
    /// Create a new cleanup tracker
    pub fn new() -> Self {
        Self {
            created_files: Vec::new(),
            created_dirs: Vec::new(),
            original_config: None,
            config_path: None,
        }
    }

    /// Track a file that was created
    pub fn track_file(&mut self, path: PathBuf) {
        self.created_files.push(path);
    }

    /// Track a directory that was created
    pub fn track_dir(&mut self, path: PathBuf) {
        self.created_dirs.push(path);
    }

    /// Backup the config in memory before modification
    pub fn backup_config(&mut self, config: &HelixConfig, config_path: PathBuf) {
        self.original_config = Some(config.clone());
        self.config_path = Some(config_path);
    }

    /// Execute cleanup in reverse order of creation
    /// Logs errors but continues cleanup process
    pub fn cleanup(self) -> CleanupSummary {
        let mut summary = CleanupSummary {
            files_removed: 0,
            files_failed: 0,
            dirs_removed: 0,
            dirs_failed: 0,
            config_restored: false,
            errors: Vec::new(),
        };

        // Step 1: Restore config from in-memory backup if modified
        if let (Some(original_config), Some(config_path)) = (self.original_config, self.config_path)
        {
            match original_config.save_to_file(&config_path) {
                Ok(_) => {
                    summary.config_restored = true;
                    eprintln!("Restored config file to original state");
                }
                Err(e) => {
                    let error_msg = format!("Failed to restore config: {}", e);
                    eprintln!("Error: {}", error_msg);
                    summary.errors.push(error_msg);
                }
            }
        }

        // Step 2: Delete files in reverse order (newest first)
        for file_path in self.created_files.iter().rev() {
            match fs::remove_file(file_path) {
                Ok(_) => {
                    summary.files_removed += 1;
                    eprintln!("Removed file: {}", file_path.display());
                }
                Err(e) => {
                    summary.files_failed += 1;
                    let error_msg = format!("Failed to remove file {}: {}", file_path.display(), e);
                    eprintln!("Warning: {}", error_msg);
                    summary.errors.push(error_msg);
                }
            }
        }

        // Step 3: Delete directories in reverse order (deepest first)
        // Sort by path depth (deepest first) to ensure we delete children before parents
        let mut sorted_dirs = self.created_dirs.clone();
        sorted_dirs.sort_by(|a, b| {
            let a_depth = a.components().count();
            let b_depth = b.components().count();
            b_depth.cmp(&a_depth) // Reverse order (deepest first)
        });

        for dir_path in sorted_dirs.iter() {
            // Only try to remove if directory exists
            if !dir_path.exists() {
                continue;
            }

            // Try to remove directory - will only succeed if empty
            match fs::remove_dir(dir_path) {
                Ok(_) => {
                    summary.dirs_removed += 1;
                    eprintln!("Removed directory: {}", dir_path.display());
                }
                Err(_e) => {
                    // This might fail if directory is not empty, which is fine
                    // We only want to remove directories we created if they're still empty
                    summary.dirs_failed += 1;
                    // Don't add to errors since this is expected for non-empty dirs
                }
            }
        }

        summary
    }

    /// Check if any resources are being tracked
    pub fn has_tracked_resources(&self) -> bool {
        !self.created_files.is_empty()
            || !self.created_dirs.is_empty()
            || self.original_config.is_some()
    }
}

impl CleanupSummary {
    /// Log the cleanup summary
    pub fn log_summary(&self) {
        if self.files_removed > 0 || self.dirs_removed > 0 || self.config_restored {
            eprintln!("Cleanup summary:");
            if self.config_restored {
                eprintln!("  - Config file restored");
            }
            if self.files_removed > 0 {
                eprintln!("  - Removed {} file(s)", self.files_removed);
            }
            if self.dirs_removed > 0 {
                eprintln!("  - Removed {} directory(ies)", self.dirs_removed);
            }
        }

        if self.files_failed > 0 || self.dirs_failed > 0 || !self.errors.is_empty() {
            eprintln!("Cleanup encountered {} error(s):", self.errors.len());
            for error in &self.errors {
                eprintln!("  - {}", error);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn test_track_and_cleanup_files() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");

        // Create a file
        let mut file = fs::File::create(&file_path).unwrap();
        file.write_all(b"test").unwrap();

        let mut tracker = CleanupTracker::new();
        tracker.track_file(file_path.clone());

        assert!(file_path.exists());

        // Cleanup should remove the file
        let summary = tracker.cleanup();
        assert_eq!(summary.files_removed, 1);
        assert_eq!(summary.files_failed, 0);
        assert!(!file_path.exists());
    }

    #[test]
    fn test_track_and_cleanup_dirs() {
        let temp_dir = TempDir::new().unwrap();
        let dir_path = temp_dir.path().join("test_dir");

        // Create a directory
        fs::create_dir(&dir_path).unwrap();

        let mut tracker = CleanupTracker::new();
        tracker.track_dir(dir_path.clone());

        assert!(dir_path.exists());

        // Cleanup should remove the directory
        let summary = tracker.cleanup();
        assert_eq!(summary.dirs_removed, 1);
        assert!(!dir_path.exists());
    }

    #[test]
    fn test_cleanup_order() {
        let temp_dir = TempDir::new().unwrap();

        // Create nested structure
        let parent_dir = temp_dir.path().join("parent");
        let child_dir = parent_dir.join("child");
        let file_path = child_dir.join("file.txt");

        fs::create_dir(&parent_dir).unwrap();
        fs::create_dir(&child_dir).unwrap();
        fs::File::create(&file_path).unwrap();

        let mut tracker = CleanupTracker::new();
        tracker.track_dir(parent_dir.clone());
        tracker.track_dir(child_dir.clone());
        tracker.track_file(file_path.clone());

        // Cleanup should handle nested structure
        let summary = tracker.cleanup();
        assert_eq!(summary.files_removed, 1);
        assert!(summary.dirs_removed >= 1); // At least child dir should be removed
    }
}
