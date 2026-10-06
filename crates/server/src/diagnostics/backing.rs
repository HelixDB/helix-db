//! What backs a directory: its filesystem and, for a filesystem on block
//! devices, the kind of disk underneath.
//!
//! Linux only. The filesystem is the mount in `/proc/self/mountinfo` whose
//! device number is the directory's; the disk comes from `/sys/dev/block`.
//! A partition reports its whole disk. A device-mapper or RAID device
//! reports the slowest disk it is built on, ranked rotational, then
//! network-attached, then local SSD, then instance storage.
//!
//! A disk is instance storage when its model is a cloud's ephemeral local
//! NVMe (Amazon EC2 instance store, Google Cloud Local SSD, Azure NVMe Direct
//! Disk) and network-attached when it is a cloud block-storage model (Amazon
//! EBS, Google Persistent Disk, Azure managed disks). Any other disk with a
//! model is a local SSD unless it is rotational. A disk without a model,
//! such as a Xen or virtio disk, could be either, and virtio disks report
//! themselves rotational whatever backs them, so it is [`Backing::Unknown`].

use std::path::{Path, PathBuf};

/// What a directory is stored on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Backing {
    /// A RAM-backed filesystem such as tmpfs: it holds memory and loses its
    /// contents on restart.
    Memory { filesystem: String },
    /// A container's writable layer, such as overlay: it is deleted with
    /// the container.
    ContainerLayer { filesystem: String },
    /// A network, FUSE, or VM-shared filesystem.
    Remote { filesystem: String },
    /// A filesystem on block devices, named by the device it is mounted
    /// from.
    Block { device: String, disk: Disk },
    /// The backing could not be determined.
    Unknown { reason: String },
}

/// The kind of disk under a block filesystem, slowest first.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Disk {
    /// A rotational hard disk.
    Rotational,
    /// Network-attached block storage, such as Amazon EBS.
    Network { model: String },
    /// A local SSD or NVMe drive other than instance storage.
    LocalSsd { model: String },
    /// A cloud's local NVMe, erased when the instance stops: Amazon EC2
    /// instance storage, Google Cloud Local SSD, or Azure NVMe Direct Disk.
    InstanceStore { model: String },
}

/// Classifies the storage behind `path`.
#[cfg(target_os = "linux")]
pub(crate) fn classify(path: &Path) -> Backing {
    use std::os::unix::fs::MetadataExt;

    let device = match std::fs::metadata(path) {
        Ok(metadata) => metadata.dev(),
        Err(error) => {
            return Backing::Unknown {
                reason: format!("cannot read `{}`: {error}", path.display()),
            };
        }
    };
    match std::fs::read_to_string("/proc/self/mountinfo") {
        Ok(mountinfo) => classify_device(
            (rustix::fs::major(device), rustix::fs::minor(device)),
            &mountinfo,
            Path::new("/sys"),
        ),
        Err(error) => Backing::Unknown {
            reason: format!("cannot read /proc/self/mountinfo: {error}"),
        },
    }
}

/// Classifies the storage behind `path`; only Linux exposes it.
#[cfg(not(target_os = "linux"))]
pub(crate) fn classify(path: &Path) -> Backing {
    Backing::Unknown {
        reason: format!(
            "cannot tell what stores `{}`: storage detection needs Linux",
            path.display()
        ),
    }
}

/// Classifies the filesystem with device number `(major, minor)`, given
/// the text of `/proc/self/mountinfo` and the sysfs root.
fn classify_device(device: (u32, u32), mountinfo: &str, sys: &Path) -> Backing {
    let number = format!("{}:{}", device.0, device.1);
    // Bind mounts of one filesystem share its device number; the last such
    // line is the most recent mount.
    let Some((filesystem, source)) = mountinfo.lines().rev().find_map(|line| {
        // `id parent major:minor root mount-point options [optional...] -
        // fstype source super-options`
        let (fields, filesystem) = line.split_once(" - ")?;
        (fields.split(' ').nth(2)? == number).then(|| {
            let mut filesystem = filesystem.split(' ');
            (
                filesystem.next().unwrap_or_default().to_owned(),
                filesystem.next().unwrap_or_default().to_owned(),
            )
        })
    }) else {
        return Backing::Unknown {
            reason: format!("no mount has device {number}"),
        };
    };
    match filesystem.as_str() {
        "tmpfs" | "ramfs" => return Backing::Memory { filesystem },
        "overlay" | "aufs" | "fuse-overlayfs" => return Backing::ContainerLayer { filesystem },
        "nfs" | "nfs4" | "cifs" | "smb3" | "smbfs" | "ceph" | "glusterfs" | "lustre" | "9p"
        | "virtiofs" | "fuse" => return Backing::Remote { filesystem },
        remote if remote.starts_with("fuse.") => return Backing::Remote { filesystem },
        _block => {}
    }
    let Ok(device_dir) = sys.join("dev/block").join(&number).canonicalize() else {
        return Backing::Unknown {
            reason: format!("{filesystem} filesystem `{source}` has no block device {number}"),
        };
    };
    let device_dir = whole_disk(device_dir);
    let device = device_dir
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or(number);
    match slowest_disk(&device_dir) {
        Ok(disk) => Backing::Block { device, disk },
        Err(reason) => Backing::Unknown { reason },
    }
}

/// The disk a partition belongs to, or `device_dir` itself.
fn whole_disk(device_dir: PathBuf) -> PathBuf {
    if !device_dir.join("partition").exists() {
        return device_dir;
    }
    device_dir
        .parent()
        .map_or(device_dir.clone(), Path::to_path_buf)
}

/// The slowest disk under `device_dir`: the device itself, or the slowest
/// of the devices it is built on.
fn slowest_disk(device_dir: &Path) -> Result<Disk, String> {
    let members = std::fs::read_dir(device_dir.join("slaves"))
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .filter_map(|entry| entry.path().canonicalize().ok())
                .map(whole_disk)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if members.is_empty() {
        return disk(device_dir);
    }
    members
        .iter()
        .map(|member| slowest_disk(member))
        .collect::<Result<Vec<_>, _>>()
        .map(|disks| {
            disks
                .into_iter()
                .min()
                .expect("a device built on others has at least one member")
        })
}

/// The kind of one disk, from its model and rotational flag.
fn disk(disk_dir: &Path) -> Result<Disk, String> {
    /// Model names of network-attached cloud block storage.
    const NETWORK_MODELS: [&str; 5] = [
        "Amazon Elastic Block Store",
        "PersistentDisk",
        "nvme_card-pd",
        "Virtual Disk",
        "Virtual_Disk",
    ];
    let read = |file: &str| {
        std::fs::read_to_string(disk_dir.join(file))
            .map(|value| value.trim().to_owned())
            .unwrap_or_default()
    };
    // NVMe namespaces read their controller's model through `device`.
    let model = read("device/model");
    if model.contains("Instance Storage")
        || model.contains("NVMe Direct Disk")
        || model == "nvme_card"
    {
        Ok(Disk::InstanceStore { model })
    } else if NETWORK_MODELS.iter().any(|network| model.contains(network)) {
        Ok(Disk::Network { model })
    } else if model.is_empty() {
        Err(format!(
            "cannot tell whether disk `{}` is local or network-attached: it reports no model",
            disk_dir
                .file_name()
                .unwrap_or(disk_dir.as_os_str())
                .to_string_lossy()
        ))
    } else if read("queue/rotational") == "1" {
        Ok(Disk::Rotational)
    } else {
        Ok(Disk::LocalSsd { model })
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::symlink;

    use super::*;

    /// A fake sysfs: each disk is `devices/<name>` with an optional model
    /// and rotational flag, linked from `dev/block/<number>`.
    struct Sysfs(tempfile::TempDir);

    impl Sysfs {
        fn new() -> Self {
            let root = tempfile::tempdir().unwrap();
            std::fs::create_dir_all(root.path().join("dev/block")).unwrap();
            Self(root)
        }

        fn root(&self) -> &Path {
            self.0.path()
        }

        fn disk(&self, name: &str, number: &str, model: Option<&str>, rotational: bool) -> PathBuf {
            let dir = self.root().join("devices").join(name);
            std::fs::create_dir_all(dir.join("queue")).unwrap();
            std::fs::write(
                dir.join("queue/rotational"),
                if rotational { "1\n" } else { "0\n" },
            )
            .unwrap();
            if let Some(model) = model {
                std::fs::create_dir_all(dir.join("device")).unwrap();
                std::fs::write(dir.join("device/model"), format!("{model}    \n")).unwrap();
            }
            symlink(&dir, self.root().join("dev/block").join(number)).unwrap();
            dir
        }

        fn partition(&self, disk: &Path, name: &str, number: &str) {
            let dir = disk.join(name);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("partition"), "1\n").unwrap();
            symlink(&dir, self.root().join("dev/block").join(number)).unwrap();
        }

        fn array(&self, name: &str, number: &str, members: &[&Path]) {
            let dir = self.root().join("devices/virtual").join(name);
            std::fs::create_dir_all(dir.join("slaves")).unwrap();
            members.iter().for_each(|member| {
                symlink(member, dir.join("slaves").join(member.file_name().unwrap())).unwrap();
            });
            symlink(&dir, self.root().join("dev/block").join(number)).unwrap();
        }
    }

    fn mount(number: &str, filesystem: &str, source: &str) -> String {
        format!(
            "36 25 {number} / /var/cache/helix rw,relatime shared:1 - {filesystem} {source} rw\n"
        )
    }

    #[test]
    fn memory_container_and_remote_filesystems_need_no_disk() {
        let sys = Sysfs::new();
        for (filesystem, expected) in [
            (
                "tmpfs",
                Backing::Memory {
                    filesystem: "tmpfs".into(),
                },
            ),
            (
                "ramfs",
                Backing::Memory {
                    filesystem: "ramfs".into(),
                },
            ),
            (
                "overlay",
                Backing::ContainerLayer {
                    filesystem: "overlay".into(),
                },
            ),
            (
                "aufs",
                Backing::ContainerLayer {
                    filesystem: "aufs".into(),
                },
            ),
            (
                "nfs4",
                Backing::Remote {
                    filesystem: "nfs4".into(),
                },
            ),
            (
                "virtiofs",
                Backing::Remote {
                    filesystem: "virtiofs".into(),
                },
            ),
            (
                "fuse.s3fs",
                Backing::Remote {
                    filesystem: "fuse.s3fs".into(),
                },
            ),
        ] {
            assert_eq!(
                classify_device((0, 52), &mount("0:52", filesystem, "none"), sys.root()),
                expected,
                "{filesystem}"
            );
        }
    }

    #[test]
    fn the_last_mount_with_the_device_number_wins() {
        let sys = Sysfs::new();
        let mountinfo = [
            mount("0:52", "tmpfs", "tmpfs"),
            mount("0:53", "overlay", "overlay"),
            mount("0:52", "overlay", "overlay"),
            "malformed line without a separator\n".to_owned(),
        ]
        .concat();
        assert_eq!(
            classify_device((0, 52), &mountinfo, sys.root()),
            Backing::ContainerLayer {
                filesystem: "overlay".into()
            }
        );
        assert_eq!(
            classify_device((0, 54), &mountinfo, sys.root()),
            Backing::Unknown {
                reason: "no mount has device 0:54".into()
            }
        );
    }

    #[test]
    fn cloud_disk_models_are_instance_storage_or_network_attached() {
        let sys = Sysfs::new();
        sys.disk(
            "nvme1n1",
            "259:1",
            Some("Amazon EC2 NVMe Instance Storage"),
            false,
        );
        sys.disk(
            "nvme0n1",
            "259:0",
            Some("Amazon Elastic Block Store"),
            false,
        );
        sys.disk("sdb", "8:16", Some("PersistentDisk"), false);
        sys.disk("sdc", "8:32", Some("Virtual Disk"), false);
        sys.disk("nvme2n1", "259:2", Some("Samsung SSD 990 PRO 2TB"), false);
        sys.disk("sda", "8:0", Some("WDC WD40EFRX"), true);
        sys.disk("nvme3n1", "259:3", Some("nvme_card"), false);
        sys.disk("nvme4n1", "259:4", Some("nvme_card-pd"), false);
        sys.disk(
            "nvme5n1",
            "259:5",
            Some("Microsoft NVMe Direct Disk v2"),
            false,
        );
        for (number, device, disk) in [
            (
                "259:1",
                "nvme1n1",
                Disk::InstanceStore {
                    model: "Amazon EC2 NVMe Instance Storage".into(),
                },
            ),
            (
                "259:0",
                "nvme0n1",
                Disk::Network {
                    model: "Amazon Elastic Block Store".into(),
                },
            ),
            (
                "8:16",
                "sdb",
                Disk::Network {
                    model: "PersistentDisk".into(),
                },
            ),
            (
                "8:32",
                "sdc",
                Disk::Network {
                    model: "Virtual Disk".into(),
                },
            ),
            (
                "259:2",
                "nvme2n1",
                Disk::LocalSsd {
                    model: "Samsung SSD 990 PRO 2TB".into(),
                },
            ),
            ("8:0", "sda", Disk::Rotational),
            (
                "259:3",
                "nvme3n1",
                Disk::InstanceStore {
                    model: "nvme_card".into(),
                },
            ),
            (
                "259:4",
                "nvme4n1",
                Disk::Network {
                    model: "nvme_card-pd".into(),
                },
            ),
            (
                "259:5",
                "nvme5n1",
                Disk::InstanceStore {
                    model: "Microsoft NVMe Direct Disk v2".into(),
                },
            ),
        ] {
            let (major, minor) = number.split_once(':').unwrap();
            assert_eq!(
                classify_device(
                    (major.parse().unwrap(), minor.parse().unwrap()),
                    &mount(number, "ext4", &format!("/dev/{device}")),
                    sys.root(),
                ),
                Backing::Block {
                    device: device.into(),
                    disk
                },
                "{device}"
            );
        }
    }

    #[test]
    fn a_partition_reports_its_disk_and_a_disk_without_a_model_is_unknown() {
        let sys = Sysfs::new();
        let ebs = sys.disk(
            "nvme0n1",
            "259:0",
            Some("Amazon Elastic Block Store"),
            false,
        );
        sys.partition(&ebs, "nvme0n1p1", "259:5");
        let xen = sys.disk("xvdf", "202:80", None, false);
        sys.partition(&xen, "xvdf1", "202:81");

        assert_eq!(
            classify_device(
                (259, 5),
                &mount("259:5", "xfs", "/dev/nvme0n1p1"),
                sys.root()
            ),
            Backing::Block {
                device: "nvme0n1".into(),
                disk: Disk::Network {
                    model: "Amazon Elastic Block Store".into()
                },
            }
        );
        assert!(matches!(
            classify_device((202, 81), &mount("202:81", "ext4", "/dev/xvdf1"), sys.root()),
            Backing::Unknown { reason } if reason.contains("`xvdf`")
        ));
        // Virtio disks report themselves rotational whatever backs them.
        sys.disk("vda", "252:0", None, true);
        assert!(matches!(
            classify_device((252, 0), &mount("252:0", "ext4", "/dev/vda"), sys.root()),
            Backing::Unknown { reason } if reason.contains("`vda`")
        ));
    }

    #[test]
    fn arrays_report_their_slowest_member() {
        let sys = Sysfs::new();
        let store_a = sys.disk(
            "nvme1n1",
            "259:1",
            Some("Amazon EC2 NVMe Instance Storage"),
            false,
        );
        let store_b = sys.disk(
            "nvme2n1",
            "259:2",
            Some("Amazon EC2 NVMe Instance Storage"),
            false,
        );
        let ebs = sys.disk(
            "nvme3n1",
            "259:3",
            Some("Amazon Elastic Block Store"),
            false,
        );
        let bare = sys.disk("vdb", "252:16", None, false);
        sys.array("md0", "9:0", &[&store_a, &store_b]);
        sys.array("md1", "9:1", &[&store_a, &ebs]);
        sys.array("md2", "9:2", &[&store_a, &bare]);
        sys.array("dm-0", "253:0", &[]);

        let classify = |number: &str| {
            let (major, minor) = number.split_once(':').unwrap();
            classify_device(
                (major.parse().unwrap(), minor.parse().unwrap()),
                &mount(number, "ext4", "/dev/md"),
                sys.root(),
            )
        };
        assert_eq!(
            classify("9:0"),
            Backing::Block {
                device: "md0".into(),
                disk: Disk::InstanceStore {
                    model: "Amazon EC2 NVMe Instance Storage".into()
                },
            }
        );
        assert_eq!(
            classify("9:1"),
            Backing::Block {
                device: "md1".into(),
                disk: Disk::Network {
                    model: "Amazon Elastic Block Store".into()
                },
            }
        );
        assert!(matches!(classify("9:2"), Backing::Unknown { reason } if reason.contains("`vdb`")));
        // An array whose members cannot be listed is a disk without a model.
        assert!(
            matches!(classify("253:0"), Backing::Unknown { reason } if reason.contains("`dm-0`"))
        );
    }

    #[test]
    fn a_block_filesystem_without_a_sysfs_device_is_unknown() {
        let sys = Sysfs::new();
        assert_eq!(
            classify_device(
                (0, 40),
                &mount("0:40", "btrfs", "/dev/nvme0n1p2"),
                sys.root()
            ),
            Backing::Unknown {
                reason: "btrfs filesystem `/dev/nvme0n1p2` has no block device 0:40".into(),
            }
        );
    }

    #[test]
    fn the_running_system_classifies_its_temporary_directory() {
        // Whatever stores it, a readable directory is classified without
        // panicking; an unreadable one is unknown.
        let _ = classify(&std::env::temp_dir());
        assert!(matches!(
            classify(Path::new("/nonexistent/helix-doctor")),
            Backing::Unknown { .. }
        ));
    }
}
