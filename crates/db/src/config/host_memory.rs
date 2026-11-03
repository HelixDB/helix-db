//! Memory ceiling of the current process, for memory-proportional defaults.
//!
//! The ceiling is the tightest cgroup memory limit on the process's cgroup
//! and its ancestors (cgroup v2 `memory.max`, or cgroup v1
//! `memory.limit_in_bytes`), capped by physical memory. A container therefore
//! sizes its defaults from its own limit rather than from the host's RAM. Each
//! hierarchy is read where the mount table says it is mounted. The ceiling is
//! read once per process; configuration that must not depend on the host sets
//! its budgets explicitly instead.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// Returns the memory this process may use, in bytes.
///
/// Returns `None` only when neither a cgroup limit nor the physical memory
/// size is readable, for example on a platform without `sysconf`.
///
/// # Examples
///
/// ```
/// let ceiling = db::config::memory_ceiling_bytes();
/// assert!(ceiling.is_some_and(|bytes| bytes > 0));
/// assert_eq!(ceiling, db::config::memory_ceiling_bytes(), "read once per process");
/// ```
pub fn memory_ceiling_bytes() -> Option<u64> {
    static CEILING: OnceLock<Option<u64>> = OnceLock::new();
    *CEILING.get_or_init(|| {
        let mounts = CgroupMounts::parse(
            &std::fs::read_to_string("/proc/self/mountinfo").unwrap_or_default(),
        );
        let cgroup = std::fs::read_to_string("/proc/self/cgroup")
            .ok()
            .and_then(|membership| cgroup_limit_bytes(&membership, &mounts));
        cgroup.into_iter().chain(physical_memory_bytes()).min()
    })
}

/// Mount points of the cgroup hierarchies that expose memory limits.
#[derive(Debug, PartialEq, Eq)]
struct CgroupMounts {
    /// The unified v2 hierarchy.
    unified: PathBuf,
    /// The v1 hierarchy that carries the `memory` controller.
    memory: PathBuf,
}

impl CgroupMounts {
    /// Locates each hierarchy in `mountinfo`, the content of `/proc/self/mountinfo`.
    ///
    /// Each line is `id parent major:minor root mount-point options
    /// [optional...] - fstype source super-options`. The v2 hierarchy is the
    /// first `cgroup2` mount, and the v1 memory hierarchy is the first
    /// `cgroup` mount whose super options list `memory`; it may be co-mounted
    /// with other controllers under any path. A hierarchy with no mount line
    /// keeps its conventional path under `/sys/fs/cgroup`, so an unreadable
    /// mount table reads the same files as a standard layout.
    fn parse(mountinfo: &str) -> Self {
        let mounts = mountinfo
            .lines()
            .filter_map(|line| {
                let (mount, filesystem) = line.split_once(" - ")?;
                let mount_point = mount.split(' ').nth(4)?;
                let mut filesystem = filesystem.split(' ');
                let (fstype, _, options) =
                    (filesystem.next()?, filesystem.next()?, filesystem.next()?);
                Some((fstype, options, mount_point))
            })
            .collect::<Vec<_>>();
        let unified = mounts
            .iter()
            .find(|(fstype, _, _)| *fstype == "cgroup2")
            .map_or_else(
                || PathBuf::from("/sys/fs/cgroup"),
                |(_, _, mount_point)| PathBuf::from(mount_point),
            );
        let memory = mounts
            .iter()
            .find(|(fstype, options, _)| {
                *fstype == "cgroup" && options.split(',').any(|option| option == "memory")
            })
            .map_or_else(
                || PathBuf::from("/sys/fs/cgroup/memory"),
                |(_, _, mount_point)| PathBuf::from(mount_point),
            );
        Self { unified, memory }
    }
}

/// Returns the tightest memory limit over the process's cgroup hierarchies.
///
/// `membership` is the content of `/proc/self/cgroup` and `mounts` locates
/// each hierarchy. Each line is `id:controllers:path`. The unified v2
/// hierarchy lists no controllers and exposes `memory.max`; a v1 hierarchy
/// listing the `memory` controller exposes `memory.limit_in_bytes`. A limit
/// on any ancestor also binds the process, and a container whose cgroup
/// namespace or bind mount hides its path sees its own limit at the mount
/// root, so every cgroup from the path up to the root is read. Unlimited
/// (`max`), missing, and malformed values are skipped.
fn cgroup_limit_bytes(membership: &str, mounts: &CgroupMounts) -> Option<u64> {
    membership
        .lines()
        .filter_map(|line| {
            let mut fields = line.splitn(3, ':');
            let (_, controllers, path) = (fields.next()?, fields.next()?, fields.next()?);
            let (mount, limit_file) = match controllers {
                "" => (&mounts.unified, "memory.max"),
                listed if listed.split(',').any(|controller| controller == "memory") => {
                    (&mounts.memory, "memory.limit_in_bytes")
                }
                _ => return None,
            };
            Path::new(path.trim_start_matches('/'))
                .ancestors()
                .filter_map(|cgroup| {
                    std::fs::read_to_string(mount.join(cgroup).join(limit_file))
                        .ok()?
                        .trim()
                        .parse::<u64>()
                        .ok()
                })
                .min()
        })
        .min()
}

/// Returns the host's physical memory, in bytes.
#[cfg(unix)]
fn physical_memory_bytes() -> Option<u64> {
    // SAFETY: `sysconf` only reads system configuration values and has no
    // memory-safety preconditions; failures are reported as -1.
    let (pages, page_size) = unsafe {
        (
            libc::sysconf(libc::_SC_PHYS_PAGES),
            libc::sysconf(libc::_SC_PAGESIZE),
        )
    };
    u64::try_from(pages)
        .ok()?
        .checked_mul(u64::try_from(page_size).ok()?)
        .filter(|bytes| *bytes > 0)
}

/// Returns the host's physical memory, which this platform cannot report.
#[cfg(not(unix))]
fn physical_memory_bytes() -> Option<u64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Writes one cgroup limit file below `root`.
    fn limit(root: &Path, cgroup: &str, file: &str, value: &str) {
        let directory = root.join(cgroup);
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join(file), value).unwrap();
    }

    /// Mounts both hierarchies at their conventional paths below `root`.
    fn mounts(root: &Path) -> CgroupMounts {
        CgroupMounts {
            unified: root.to_path_buf(),
            memory: root.join("memory"),
        }
    }

    #[test]
    fn cgroup_v2_limit_is_the_tightest_ancestor() {
        let root = tempfile::tempdir().unwrap();
        limit(root.path(), "", "memory.max", "max\n");
        limit(root.path(), "kubepods", "memory.max", "1073741824\n");
        limit(root.path(), "kubepods/pod", "memory.max", "max\n");
        limit(
            root.path(),
            "kubepods/pod/container",
            "memory.max",
            "2147483648\n",
        );

        assert_eq!(
            cgroup_limit_bytes("0::/kubepods/pod/container\n", &mounts(root.path())),
            Some(1 << 30)
        );
        assert_eq!(cgroup_limit_bytes("0::/\n", &mounts(root.path())), None);
        assert_eq!(
            cgroup_limit_bytes("0::/unknown/path\n", &mounts(root.path())),
            None
        );
    }

    #[test]
    fn namespaced_cgroup_reads_its_limit_at_the_mount_root() {
        let root = tempfile::tempdir().unwrap();
        limit(root.path(), "", "memory.max", "536870912\n");
        assert_eq!(
            cgroup_limit_bytes("0::/\n", &mounts(root.path())),
            Some(512 << 20)
        );

        // A v1 container sees the host's cgroup path but its own limit at the mount.
        let v1 = tempfile::tempdir().unwrap();
        limit(v1.path(), "memory", "memory.limit_in_bytes", "268435456\n");
        assert_eq!(
            cgroup_limit_bytes(
                "5:cpu,cpuacct:/docker/abc\n4:cpuset,memory:/docker/abc\n",
                &mounts(v1.path())
            ),
            Some(256 << 20)
        );
    }

    #[test]
    fn unrelated_or_malformed_hierarchies_have_no_limit() {
        let root = tempfile::tempdir().unwrap();
        limit(
            root.path(),
            "memory",
            "memory.limit_in_bytes",
            "not-a-number\n",
        );
        limit(root.path(), "cpu", "memory.limit_in_bytes", "1024\n");

        assert_eq!(cgroup_limit_bytes("", &mounts(root.path())), None);
        assert_eq!(
            cgroup_limit_bytes("malformed\n", &mounts(root.path())),
            None
        );
        assert_eq!(cgroup_limit_bytes("3:cpu:/\n", &mounts(root.path())), None);
        assert_eq!(
            cgroup_limit_bytes("4:memory:/\n", &mounts(root.path())),
            None
        );
    }

    #[test]
    fn mount_table_locates_each_hierarchy() {
        let mounts = CgroupMounts::parse(concat!(
            "22 1 8:1 / / rw,relatime shared:1 - ext4 /dev/root rw\n",
            "malformed line\n",
            "30 23 0:26 / /run/cg2 rw,nosuid shared:4 - cgroup2 cgroup2 rw,nsdelegate\n",
            "31 23 0:27 / /run/cg2-copy rw shared:5 - cgroup2 cgroup2 rw\n",
            "33 25 0:29 / /run/cg1/systemd rw shared:13 - cgroup cgroup rw,xattr,name=systemd\n",
            "34 25 0:30 / /run/cg1/cpu,cpuacct rw shared:14 - cgroup cgroup rw,cpu,cpuacct\n",
            "35 25 0:31 / /run/cg1/cpuset,memory rw shared:15 - cgroup cgroup rw,cpuset,memory\n",
        ));
        assert_eq!(
            mounts,
            CgroupMounts {
                unified: PathBuf::from("/run/cg2"),
                memory: PathBuf::from("/run/cg1/cpuset,memory"),
            }
        );
    }

    #[test]
    fn missing_hierarchies_keep_their_conventional_mounts() {
        let conventional = CgroupMounts {
            unified: PathBuf::from("/sys/fs/cgroup"),
            memory: PathBuf::from("/sys/fs/cgroup/memory"),
        };
        assert_eq!(CgroupMounts::parse(""), conventional);
        // Only a `cgroup` mount listing exactly `memory` is the v1 memory hierarchy.
        assert_eq!(
            CgroupMounts::parse(concat!(
                "34 25 0:30 / /run/cg1/cpu rw shared:14 - cgroup cgroup rw,cpu\n",
                "36 25 0:32 / /run/other rw - tmpfs tmpfs rw,memory\n",
                "truncated - cgroup\n",
                "1 2 3 - cgroup cgroup rw,memory\n",
            )),
            conventional
        );
        assert_eq!(
            CgroupMounts::parse(
                "30 23 0:26 / /run/cg2 rw - cgroup2 cgroup2 rw,memory_recursiveprot\n"
            ),
            CgroupMounts {
                unified: PathBuf::from("/run/cg2"),
                memory: PathBuf::from("/sys/fs/cgroup/memory"),
            }
        );
    }

    #[test]
    fn v1_limit_is_read_where_the_mount_table_puts_it() {
        let root = tempfile::tempdir().unwrap();
        limit(
            root.path(),
            "controllers/cpuset,memory",
            "memory.limit_in_bytes",
            "268435456\n",
        );
        let membership = "4:cpuset,memory:/docker/abc\n";
        // The conventional `root/memory` mount does not exist here.
        assert_eq!(cgroup_limit_bytes(membership, &mounts(root.path())), None);

        let mountinfo = format!(
            "35 25 0:31 /docker/abc {} rw shared:15 - cgroup cgroup rw,cpuset,memory\n",
            root.path().join("controllers/cpuset,memory").display()
        );
        assert_eq!(
            cgroup_limit_bytes(membership, &CgroupMounts::parse(&mountinfo)),
            Some(256 << 20)
        );
    }

    #[test]
    fn process_memory_ceiling_is_positive_and_stable() {
        let ceiling = memory_ceiling_bytes();
        #[cfg(unix)]
        assert!(ceiling.is_some_and(|bytes| bytes > 0));
        assert_eq!(memory_ceiling_bytes(), ceiling);
        assert!(physical_memory_bytes().is_none_or(|physical| ceiling <= Some(physical)));
    }
}
