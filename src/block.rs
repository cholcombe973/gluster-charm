extern crate juju;
extern crate libudev;
extern crate regex;

use self::regex::Regex;
use super::apt::apt_install;
use uuid::Uuid;

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

// Formats a block device at Path p with XFS
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum MetadataProfile {
    Raid0,
    Raid1,
    Raid5,
    Raid6,
    Raid10,
    Single,
    Dup,
}

// This will be used to make intelligent decisions about setting up the device
#[derive(Debug)]
pub struct Device {
    pub id: Option<Uuid>,
    pub name: String,
    pub media_type: MediaType,
    pub capacity: u64,
    pub fs_type: FilesystemType,
}

#[derive(Debug)]
pub enum MediaType {
    SolidState,
    Rotational,
    Loopback,
    Unknown,
}

#[derive(Debug, Eq, PartialEq)]
pub enum FilesystemType {
    Btrfs,
    Ext4,
    Xfs,
    Zfs,
    Unknown,
}

impl FilesystemType {
    pub fn from_str(fs_type: &str) -> FilesystemType {
        match fs_type {
            "btrfs" => FilesystemType::Btrfs,
            "ext4" => FilesystemType::Ext4,
            "xfs" => FilesystemType::Xfs,
            "zfs" => FilesystemType::Zfs,
            _ => FilesystemType::Unknown,
        }
    }
}

impl MetadataProfile {
    pub fn to_string(self) -> String {
        match self {
            MetadataProfile::Raid0 => "raid0".to_string(),
            MetadataProfile::Raid1 => "raid1".to_string(),
            MetadataProfile::Raid5 => "raid5".to_string(),
            MetadataProfile::Raid6 => "raid6".to_string(),
            MetadataProfile::Raid10 => "raid10".to_string(),
            MetadataProfile::Single => "single".to_string(),
            MetadataProfile::Dup => "dup".to_string(),
        }
    }
}

#[derive(Debug)]
pub enum Filesystem {
    Btrfs {
        metadata_profile: MetadataProfile,
        leaf_size: u64,
        node_size: u64,
    },
    Ext4 {
        inode_size: u64,
        reserved_blocks_percentage: u8,
    },
    Xfs {
        // This is optional.  Boost knobs are on by default:
        // http://xfs.org/index.php/XFS_FAQ#Q:_I_want_to_tune_my_XFS_filesystems_for_.3Csomething.3E
        inode_size: Option<u64>,
        force: bool,
    },
    Zfs {
        /// The default blocksize for volumes is 8 Kbytes. Any
        /// power of 2 from 512 bytes to 128 Kbytes is valid.
        block_size: Option<u64>,
        /// Enable compression on the volume. Default is false
        compression: Option<bool>,
    },
}

impl Filesystem {
    #[allow(dead_code)]
    pub fn new(name: &str) -> Filesystem {
        match name.trim() {
            // Defaults.  Can be changed as needed by the caller
            "zfs" => {
                Filesystem::Zfs {
                    block_size: None,
                    compression: None,
                }
            }
            "xfs" => {
                Filesystem::Xfs {
                    inode_size: None,
                    force: false,
                }
            }
            "btrfs" => {
                Filesystem::Btrfs {
                    metadata_profile: MetadataProfile::Single,
                    leaf_size: 32768,
                    node_size: 32768,
                }
            }
            "ext4" => {
                Filesystem::Ext4 {
                    inode_size: 256,
                    reserved_blocks_percentage: 0,
                }
            }
            _ => {
                Filesystem::Xfs {
                    inode_size: None,
                    force: false,
                }
            }
        }
    }
}

fn run_command<S: AsRef<OsStr>>(command: &str, arg_list: &[S]) -> Output {
    let mut cmd = Command::new(command);
    cmd.args(arg_list);
    let output = cmd.output().unwrap_or_else(|e| panic!("failed to execute process: {} ", e));
    return output;
}

// This assumes the device is formatted at this point
pub fn mount_device(device: &Device, mount_point: &str) -> Result<i32, String> {
    let mut arg_list: Vec<String> = Vec::new();
    match device.id {
        Some(id) => {
            arg_list.push("-U".to_string());
            arg_list.push(id.hyphenated().to_string());
        }
        None => {
            arg_list.push(format!("/dev/{}", device.name));
        }
    };
    // arg_list.push("-t".to_string());
    // match device.fs_type{
    // FilesystemType::Btrfs => {
    // arg_list.push("btrfs".to_string());
    // },
    // FilesystemType::Ext4 => {
    // arg_list.push("ext4".to_string());
    // },
    // FilesystemType::Xfs => {
    // arg_list.push("xfs".to_string());
    // },
    // FilesystemType::Unknown => {
    // return Err("Unable to mount unknown filesystem type".to_string());
    // }
    // };
    //
    arg_list.push(mount_point.to_string());

    return process_output(run_command("mount", &arg_list));
}

fn process_output(output: Output) -> Result<i32, String> {
    log!(format!("Command output: {:?}", output));

    if output.status.success() {
        Ok(0)
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        Err(stderr)
    }
}

pub fn format_block_device(device: &PathBuf, filesystem: &Filesystem) -> Result<i32, String> {
    match filesystem {
        &Filesystem::Btrfs { ref metadata_profile, ref leaf_size, ref node_size } => {
            let arg_list: Vec<String> = vec!["-m".to_string(),
                                             metadata_profile.clone().to_string(),
                                             "-l".to_string(),
                                             leaf_size.to_string(),
                                             "-n".to_string(),
                                             node_size.to_string(),
                                             device.to_string_lossy().to_string()];
            // Check if mkfs.btrfs is installed
            if !Path::new("/sbin/mkfs.btrfs").exists() {
                log!("Installing btrfs utils");
                apt_install(vec!["btrfs-tools"])?;
            }
            return process_output(run_command("mkfs.btrfs", &arg_list));
        }
        &Filesystem::Xfs { ref inode_size, ref force } => {
            let mut arg_list: Vec<String> = Vec::new();

            if (*inode_size).is_some() {
                arg_list.push("-i".to_string());
                arg_list.push(inode_size.unwrap().to_string());
            }

            if *force {
                arg_list.push("-f".to_string());
            }

            arg_list.push(device.to_string_lossy().to_string());

            // Check if mkfs.xfs is installed
            if !Path::new("/sbin/mkfs.xfs").exists() {
                log!("Installing xfs utils");
                apt_install(vec!["xfsprogs"])?;
            }
            return process_output(run_command("/sbin/mkfs.xfs", &arg_list));
        }
        &Filesystem::Zfs { ref block_size, ref compression } => {
            //let mut arg_list: Vec<String>; = Vec::new();
            // Check if zfs is installed
            if !Path::new("/sbin/zfs").exists() {
                log!("Installing zfs utils");
                apt_install(vec!["zfsutils-linux"])?;
            }
            let base_name = device.file_name();
            match base_name {
                Some(name) => {
                    //Mount at /mnt/{dev_name}
                    let arg_list: Vec<String> = vec!["create".to_string(),
                                                     "-f".to_string(),
                                                     "-m".to_string(),
                                                     format!("/mnt/{}",
                                                             name.to_string_lossy().into_owned()),
                                                     name.to_string_lossy().into_owned(),
                                                     device.to_string_lossy().into_owned()];
                    process_output(run_command("/sbin/zpool", &arg_list))?;

                    if block_size.is_some() {
                        process_output(run_command("/sbin/zfs",
                                                   &vec!["set".to_string(),
                                                         format!("recordsize={}",
                                                                 block_size.unwrap()),
                                                         name.to_string_lossy().into_owned()]))?;
                    }
                    if compression.is_some() {
                        process_output(run_command("/sbin/zfs",
                                                   &vec!["set".to_string(),
                                                         "compression=on".to_string(),
                                                         name.to_string_lossy().into_owned()]))?;
                    }
                    process_output(run_command("/sbin/zfs",
                                               &vec!["set".to_string(),
                                                     "acltype=posixacl".to_string(),
                                                     name.to_string_lossy().into_owned()]))?;
                    Ok(0)
                }
                None => Err(format!("Unable to determine filename for device: {:?}", device)),
            }
        }
        &Filesystem::Ext4 { ref inode_size, ref reserved_blocks_percentage } => {
            let arg_list: Vec<String> = vec!["-I".to_string(),
                                             inode_size.to_string(),
                                             "-m".to_string(),
                                             reserved_blocks_percentage.to_string(),
                                             device.to_string_lossy().to_string()];

            return process_output(run_command("mkfs.ext4", &arg_list));
        }
    }
}

#[test]
fn test_get_device_info() {
    print!("{:?}", get_device_info(&PathBuf::from("/dev/sda1")));
    print!("{:?}", get_device_info(&PathBuf::from("/dev/loop0")));
}

fn get_size(device: &libudev::Device) -> Option<u64> {
    match device.attribute_value("size") {
        // 512 is the block size
        Some(size_str) => {
            let size = size_str.to_str().unwrap_or("0").parse::<u64>().unwrap_or(0);
            return Some(size * 512);
        }
        None => return None,
    }
}

fn get_uuid(device: &libudev::Device) -> Option<Uuid> {
    match device.property_value("ID_FS_UUID") {
        Some(value) => {
            match Uuid::parse_str(value.to_str().unwrap()) {
                Ok(result) => return Some(result),
                Err(_) => return None,
            }
        }
        None => return None,
    }
}

fn get_fs_type(device: &libudev::Device) -> FilesystemType {
    match device.property_value("ID_FS_TYPE") {
        Some(s) => {
            let value = s.to_string_lossy();
            match value.as_ref() {
                "btrfs" => return FilesystemType::Btrfs,
                "xfs" => return FilesystemType::Xfs,
                "ext4" => return FilesystemType::Ext4,
                _ => return FilesystemType::Unknown,
            }
        }
        None => return FilesystemType::Unknown,
    }
}

fn get_media_type(device: &libudev::Device) -> MediaType {
    let device_sysname = device.sysname().to_str();
    let loop_regex = Regex::new(r"loop\d+").unwrap();

    if loop_regex.is_match(device_sysname.unwrap()) {
        return MediaType::Loopback;
    }

    match device.property_value("ID_ATA_ROTATION_RATE_RPM") {
        Some(value) => {
            if value == "0" {
                return MediaType::SolidState;
            } else {
                return MediaType::Rotational;
            }
        }
        None => return MediaType::Unknown,
    }
}

#[allow(dead_code)]
pub fn is_block_device(device_path: &PathBuf) -> Result<bool, String> {
    let context = try!(libudev::Context::new().map_err(|e| e.to_string()));
    let mut enumerator = try!(libudev::Enumerator::new(&context).map_err(|e| e.to_string()));
    let devices = try!(enumerator.scan_devices().map_err(|e| e.to_string()));

    let sysname = try!(device_path.file_name()
        .ok_or(format!("Unable to get file_name on device {:?}", device_path)));

    for device in devices {
        if sysname == device.sysname() {
            if device.subsystem() == "block" {
                return Ok(true);
            }
        }
    }

    return Err(format!("Unable to find device with name {:?}", device_path));
}

// Tries to figure out what type of device this is
pub fn get_device_info(device_path: &PathBuf) -> Result<Device, String> {
    let context = try!(libudev::Context::new().map_err(|e| e.to_string()));
    let mut enumerator = try!(libudev::Enumerator::new(&context).map_err(|e| e.to_string()));
    let devices = try!(enumerator.scan_devices().map_err(|e| e.to_string()));

    let sysname = try!(device_path.file_name()
        .ok_or(format!("Unable to get file_name on device {:?}", device_path)));

    for device in devices {
        if sysname == device.sysname() {
            // This is going to get complicated
            if device.subsystem() == "block" {
                // Ok we're a block device
                let id: Option<Uuid> = get_uuid(&device);
                let media_type = get_media_type(&device);
                let capacity = match get_size(&device) {
                    Some(size) => size,
                    None => 0,
                };
                let fs_type = get_fs_type(&device);

                return Ok(Device {
                    id: id,
                    name: sysname.to_string_lossy().to_string(),
                    media_type: media_type,
                    capacity: capacity,
                    fs_type: fs_type,
                });
            }
        }
    }
    return Err(format!("Unable to find device with name {:?}", device_path));
}
