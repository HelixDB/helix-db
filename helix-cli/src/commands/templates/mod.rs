pub mod fetcher;
pub mod processor;

pub use fetcher::TemplateFetcher;
pub use processor::TemplateProcessor;

const OFFICIAL_TEMPLATES_ORG: &str = "helix-db";

/// Represents different ways to reference a template
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TemplateSource {
    Official {
        name: String,
        git_ref: Option<String>,
    },
    GitUrl {
        url: String,
        git_ref: Option<String>,
    },
}

impl TemplateSource {
    pub fn parse(s: &str) -> eyre::Result<Self> {
        let s = s.trim();

        if s.is_empty() {
            return Err(eyre::eyre!("Template name cannot be empty"));
        }

        if s.starts_with("https://") || s.starts_with("http://") {
            let (url, git_ref) = Self::split_at_ref(s, 8);
            return Ok(TemplateSource::GitUrl { url, git_ref });
        }

        if s.starts_with("git@") {
            let (url, git_ref) = if s.matches('@').count() > 1 {
                Self::split_at_ref(s, 0)
            } else {
                (s.to_string(), None)
            };
            return Ok(TemplateSource::GitUrl { url, git_ref });
        }

        let (name, git_ref) = Self::split_at_ref(s, 0);
        if !name
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
        {
            return Err(eyre::eyre!("Invalid template name"));
        }

        Ok(TemplateSource::Official { name, git_ref })
    }

    // split the git url and git branch from the string
    fn split_at_ref(s: &str, skip: usize) -> (String, Option<String>) {
        if let Some((base, git_ref)) = s.rsplit_once('@') {
            if skip > 0 && !base[skip..].contains('/') {
                return (s.to_string(), None);
            }
            (base.to_string(), Some(git_ref.to_string()))
        } else {
            (s.to_string(), None)
        }
    }

    pub fn to_git_url(&self) -> String {
        match self {
            TemplateSource::Official { name, .. } => {
                format!("https://github.com/{}/{}", OFFICIAL_TEMPLATES_ORG, name)
            }
            TemplateSource::GitUrl { url, .. } => url.clone(),
        }
    }

    pub fn git_ref(&self) -> Option<&str> {
        match self {
            TemplateSource::Official { git_ref, .. } | TemplateSource::GitUrl { git_ref, .. } => {
                git_ref.as_deref()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_official() {
        let src = TemplateSource::parse("basic@v1.0").unwrap();
        assert_eq!(
            src.to_git_url(),
            format!("https://github.com/{}/basic", OFFICIAL_TEMPLATES_ORG)
        );
        assert_eq!(src.git_ref(), Some("v1.0"));
    }

    #[test]
    fn test_parse_https_url() {
        let src = TemplateSource::parse("https://github.com/user/repo@main").unwrap();
        assert_eq!(src.to_git_url(), "https://github.com/user/repo");
        assert_eq!(src.git_ref(), Some("main"));
    }

    #[test]
    fn test_parse_ssh_url() {
        let src = TemplateSource::parse("git@github.com:user/repo.git@v2").unwrap();
        assert_eq!(src.git_ref(), Some("v2"));
    }

    #[test]
    fn test_parse_invalid() {
        assert!(TemplateSource::parse("").is_err());
        assert!(TemplateSource::parse("bad/name").is_err());
    }
}
