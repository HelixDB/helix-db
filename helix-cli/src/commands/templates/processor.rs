use crate::utils::print_status;
use eyre::Result;
use handlebars::Handlebars;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// Processes templates and applies variable substitution
pub struct TemplateProcessor;

impl TemplateProcessor {
    /// Copy already-rendered template files from cache to destination
    pub fn process(cache_dir: &Path, project_dir: &Path) -> Result<()> {
        print_status("TEMPLATE", "Copying template files...");

        Self::copy_to_dir(cache_dir, project_dir)?;

        print_status("TEMPLATE", "Template applied successfully");

        Ok(())
    }

    /// Render template from source to cache directory
    pub fn render_to_cache(
        template_dir: &Path,
        cache_dir: &Path,
        variables: &HashMap<String, String>,
    ) -> Result<()> {
        // Create Handlebars instance
        let hbs = Handlebars::new();
        Self::render_dir_recursive(template_dir, cache_dir, &hbs, variables)?;

        Ok(())
    }

    /// copy cached template files to destination
    fn copy_to_dir(src: &Path, dst: &Path) -> Result<()> {
        fs::create_dir_all(dst)?;

        for entry in fs::read_dir(src)? {
            let entry = entry?;
            let path = entry.path();
            let file_name = entry.file_name();
            let file_name_str = file_name.to_string_lossy();

            // Skip .git directory
            if file_name_str == ".git" {
                continue;
            }

            if path.is_dir() {
                let dest_dir = dst.join(&file_name);
                Self::copy_to_dir(&path, &dest_dir)?;
            } else {
                let dest_file = dst.join(&file_name);
                fs::copy(&path, &dest_file)?;
            }
        }

        Ok(())
    }

    /// Recursively render directory with variable substitution
    fn render_dir_recursive(
        src: &Path,
        dst: &Path,
        hbs: &Handlebars,
        variables: &HashMap<String, String>,
    ) -> Result<()> {
        fs::create_dir_all(dst)?;

        for entry in fs::read_dir(src)? {
            let entry = entry?;
            let path = entry.path();
            let file_name = entry.file_name();
            let file_name_str = file_name.to_string_lossy();

            // Skip hidden files that start with dot (except for .gitignore)
            if file_name_str.starts_with('.') && file_name_str != ".gitignore" {
                continue;
            }

            match path.is_dir() {
                true => {
                    let dest_dir = dst.join(&file_name);
                    Self::render_dir_recursive(&path, &dest_dir, hbs, variables)?;
                }
                false => {
                    let dest_file = dst.join(&file_name);
                    match file_name_str.ends_with(".hbs") {
                        true => Self::render_template_file(&path, &dest_file, hbs, variables)?,
                        false => {
                            fs::copy(&path, &dest_file)?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Render a .hbs template file to destination (removing .hbs extension)
    fn render_template_file(
        src: &Path,
        dest: &Path,
        hbs: &Handlebars,
        variables: &HashMap<String, String>,
    ) -> Result<()> {
        let content = fs::read_to_string(src)?;

        let rendered = hbs
            .render_template(&content, variables)
            .map_err(|e| eyre::eyre!("Template render error: {}", e))?;

        let dest_without_hbs = dest.with_extension("");

        fs::write(&dest_without_hbs, rendered)?;

        Ok(())
    }
}
