use eyre::Result;
use handlebars::Handlebars;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// Processes templates and applies variable substitution
pub struct TemplateProcessor;

impl TemplateProcessor {
    /// Render template from cache to destination
    pub fn render_to_dir(
        template_dir: &Path,
        cache_dir: &Path,
        variables: &HashMap<String, String>,
    ) -> Result<()> {
        // Create Handlebars instance
        let hbs = Handlebars::new();
        Self::render_dir_recursive(template_dir, cache_dir, &hbs, variables)?;

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

            // Skip symlinks
            if path.is_symlink() {
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
