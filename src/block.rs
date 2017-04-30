extern crate juju;
extern crate libudev;
extern crate regex;
extern crate shellscript;

use self::regex::Regex;
use super::apt::apt_install;
use super::{device_initialized, get_config_value};
use uuid::Uuid;

use std::fmt;
use std::fs::File;
use std::ffi::OsStr;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output};
use std::str::FromStr;

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

#[derive(Debug, Clone)]
pub struct BrickDevice {
    pub is_block_device: bool,
    pub initialized: bool,
    pub mount_path: String,
    pub dev_path: PathBuf,
}

#[derive(Debug)]
pub struct AsyncInit {
    /// The child process needed for this device initialization
    /// This will be an async spawned Child handle
    pub format_child: Child,
    /// After formatting is complete run these commands to setup the filesystem
    /// ZFS needs this.  These should prob be run in sync mode
    pub post_setup_commands: Vec<(String, Vec<String>)>,
    /// The device we're initializing
    pub device: BrickDevice,
}

#[derive(Debug)]
pub enum Scheduler {
    /// Try to balance latency and throughput
    Cfq,
    /// Latency is most important
    Deadline,
    /// Throughput is most important
    Noop,
}

impl fmt::Display for Scheduler {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = match self {
            &Scheduler::Cfq => "cfq",
            &Scheduler::Deadline => "deadline",
            &Scheduler::Noop => "noop",
        };
        write!(f, "{}", s)
    }
}

impl FromStr for Scheduler {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "cfq" => Ok(Scheduler::Cfq),
            "deadline" => Ok(Scheduler::Deadline),
            "noop" => Ok(Scheduler::Noop),
            _ => Err(format!("Unknown scheduler {}", s)),
        }
    }
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
    Ext2,
    Ext3,
    Ext4,
    Xfs,
    Zfs,
    Unknown,
}

impl FilesystemType {
    pub fn from_str(fs_type: &str) -> FilesystemType {
        match fs_type {
            "btrfs" => FilesystemType::Btrfs,
            "ext2" => FilesystemType::Ext2,
            "ext3" => FilesystemType::Ext3,
            "ext4" => FilesystemType::Ext4,
            "xfs" => FilesystemType::Xfs,
            "zfs" => FilesystemType::Zfs,
            _ => FilesystemType::Unknown,
        }
    }
    pub fn to_str(&self) -> &str {
        match self {
            &FilesystemType::Btrfs => "btrfs",
            &FilesystemType::Ext2 => "ext2",
            &FilesystemType::Ext3 => "ext3",
            &FilesystemType::Ext4 => "ext4",
            &FilesystemType::Xfs => "xfs",
            &FilesystemType::Zfs => "zfs",
            &FilesystemType::Unknown => "unknown",
        }
    }
    pub fn to_string(&self) -> String {
        match self {
            &FilesystemType::Btrfs => "btrfs".to_string(),
            &FilesystemType::Ext2 => "ext2".to_string(),
            &FilesystemType::Ext3 => "ext3".to_string(),
            &FilesystemType::Ext4 => "ext4".to_string(),
            &FilesystemType::Xfs => "xfs".to_string(),
            &FilesystemType::Zfs => "zfs".to_string(),
            &FilesystemType::Unknown => "unknown".to_string(),
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
        inode_size: Option<u64>,
        reserved_blocks_percentage: u8,
        stride: Option<u64>,
        stripe_width: Option<u64>,
    },
    Xfs {
        // This is optional.  Boost knobs are on by default:
        // http://xfs.org/index.php/XFS_FAQ#Q:_I_want_to_tune_my_XFS_filesystems_for_.3Csomething.3E
        block_size: Option<u64>, // Note this MUST be a power of 2
        inode_size: Option<u64>,
        stripe_size: Option<u64>, // RAID controllers stripe size in BYTES
        stripe_width: Option<u64>, // IE # of data disks
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
                    stripe_size: None,
                    stripe_width: None,
                    block_size: None,
                    inode_size: Some(512),
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
                    inode_size: Some(512),
                    reserved_blocks_percentage: 0,
                    stride: None,
                    stripe_width: None,
                }
            }
            _ => {
                Filesystem::Xfs {
                    stripe_size: None,
                    stripe_width: None,
                    block_size: None,
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
    arg_list.push(mount_point.to_string());

    return process_output(run_command("mount", &arg_list));
}

pub fn process_output(output: Output) -> Result<i32, String> {
    log!(format!("Command output: {:?}", output));

    if output.status.success() {
        Ok(0)
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        Err(stderr)
    }
}

pub fn format_block_device(brick_device: BrickDevice,
                           filesystem: &Filesystem)
                           -> Result<AsyncInit, String> {
    let device = brick_device.dev_path.clone();
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
            return Ok(AsyncInit {
                          format_child: Command::new("mkfs.btrfs").args(&arg_list)
                              .spawn()
                              .map_err(|e| e.to_string())?,
                          post_setup_commands: vec![],
                          device: brick_device,
                      });
        }
        &Filesystem::Xfs { ref block_size,
                           ref inode_size,
                           ref stripe_size,
                           ref stripe_width,
                           ref force } => {
            let mut arg_list: Vec<String> = Vec::new();

            if (*inode_size).is_some() {
                arg_list.push("-i".to_string());
                arg_list.push(format!("size={}", inode_size.unwrap()));
            }

            if *force {
                arg_list.push("-f".to_string());
            }

            if (*block_size).is_some() {
                let mut block_size = block_size.unwrap();
                if !block_size.is_power_of_two() {
                    log!(format!("block_size {} is not a power of two.
                    Rounding up to nearest power of 2",
                                 block_size));
                    block_size = block_size.next_power_of_two();
                }
                arg_list.push("-b".to_string());
                arg_list.push(format!("size={}", block_size));
            }

            if (*stripe_size).is_some() && (*stripe_width).is_some() {
                arg_list.push("-d".to_string());
                arg_list.push(format!("su={}", stripe_size.unwrap()));
                arg_list.push(format!("sw={}", stripe_width.unwrap()));
            }

            arg_list.push(device.to_string_lossy().to_string());

            // Check if mkfs.xfs is installed
            if !Path::new("/sbin/mkfs.xfs").exists() {
                log!("Installing xfs utils");
                apt_install(vec!["xfsprogs"])?;
            }
            let format_handle = Command::new("/sbin/mkfs.xfs").args(&arg_list)
                .spawn()
                .map_err(|e| e.to_string())?;
            return Ok(AsyncInit {
                          format_child: format_handle,
                          post_setup_commands: vec![],
                          device: brick_device,
                      });
        }
        &Filesystem::Zfs { ref block_size, ref compression } => {
            // Check if zfs is installed
            if !Path::new("/sbin/zfs").exists() {
                log!("Installing zfs utils");
                apt_install(vec!["zfsutils-linux"])?;
            }
            let base_name = device.file_name();
            match base_name {
                Some(name) => {
                    //Mount at /mnt/{dev_name}
                    let mut post_setup_commands: Vec<(String, Vec<String>)> = Vec::new();
                    let arg_list: Vec<String> = vec!["create".to_string(),
                                                     "-f".to_string(),
                                                     "-m".to_string(),
                                                     format!("/mnt/{}",
                                                             name.to_string_lossy().into_owned()),
                                                     name.to_string_lossy().into_owned(),
                                                     device.to_string_lossy().into_owned()];
                    let zpool_create = Command::new("/sbin/zpool").args(&arg_list)
                        .spawn()
                        .map_err(|e| e.to_string())?;

                    if block_size.is_some() {
                        // If zpool creation is successful then we set these
                        let mut block_size = block_size.unwrap();
                        log!(format!("block_size {} is not a power of two.
                    Rounding up to nearest power of 2",
                                     block_size));
                        block_size = block_size.next_power_of_two();
                        post_setup_commands.push(("/sbin/zfs".to_string(),
                                                  vec!["set".to_string(),
                                                       format!("recordsize={}", block_size),
                                                       name.to_string_lossy().into_owned()]));
                    }
                    if compression.is_some() {
                        post_setup_commands.push(("/sbin/zfs".to_string(),
                                                  vec!["set".to_string(),
                                                       "compression=on".to_string(),
                                                       name.to_string_lossy().into_owned()]));
                    }
                    post_setup_commands.push(("/sbin/zfs".to_string(),
                                              vec!["set".to_string(),
                                                   "acltype=posixacl".to_string(),
                                                   name.to_string_lossy().into_owned()]));
                    post_setup_commands.push(("/sbin/zfs".to_string(),
                                              vec!["set".to_string(),
                                                   "atime=off".to_string(),
                                                   name.to_string_lossy().into_owned()]));
                    return Ok(AsyncInit {
                                  format_child: zpool_create,
                                  post_setup_commands: post_setup_commands,
                                  device: brick_device,
                              });
                }
                None => Err(format!("Unable to determine filename for device: {:?}", device)),
            }
        }
        &Filesystem::Ext4 { ref inode_size,
                            ref reserved_blocks_percentage,
                            ref stride,
                            ref stripe_width } => {
            let mut arg_list: Vec<String> = vec!["-m".to_string(),
                                                 reserved_blocks_percentage.to_string()];

            if (*inode_size).is_some() {
                arg_list.push("-I".to_string());
                arg_list.push(inode_size.unwrap().to_string());
            }
            if (*stride).is_some() {
                arg_list.push("-E".to_string());
                arg_list.push(format!("stride={}", stride.unwrap()));
            }
            if (*stripe_width).is_some() {
                arg_list.push("-E".to_string());
                arg_list.push(format!("stripe_width={}", stripe_width.unwrap()));
            }
            arg_list.push(device.to_string_lossy().into_owned());

            return Ok(AsyncInit {
                          format_child: Command::new("mkfs.ext4").args(&arg_list)
                              .spawn()
                              .map_err(|e| e.to_string())?,
                          post_setup_commands: vec![],
                          device: brick_device,
                      });
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
            let size = size_str.to_str()
                .unwrap_or("0")
                .parse::<u64>()
                .unwrap_or(0);
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

fn scan_devices(devices: Vec<String>) -> Result<Vec<BrickDevice>, String> {
    let mut brick_devices: Vec<BrickDevice> = Vec::new();
    for brick in devices {
        let device_path = PathBuf::from(brick);
        // Translate to mount location
        let brick_filename = match device_path.file_name() {
            Some(name) => name,
            None => {
                log!(format!("Unable to determine filename for device: {:?}. Skipping",
                             device_path),
                     Error);
                continue;
            }
        };
        log!(format!("Checking if {:?} is a block device", &device_path));
        let is_block_device = is_block_device(&device_path).unwrap_or(false);
        if !is_block_device {
            log!(format!("Skipping invalid block device: {:?}", &device_path));
            continue;
        }
        log!(format!("Checking if {:?} is initialized", &device_path));
        let initialized = device_initialized(&device_path).unwrap_or(false);
        let mount_path = format!("/mnt/{}", brick_filename.to_string_lossy());
        brick_devices.push(BrickDevice {
                               is_block_device: is_block_device,
                               // All devices start at initialized is false
                               initialized: initialized,
                               dev_path: device_path.clone(),
                               mount_path: mount_path,
                           });
    }
    Ok(brick_devices)
}

pub fn set_elevator(device_path: &PathBuf,
                    elevator: &Scheduler)
                    -> Result<usize, ::std::io::Error> {
    log!(format!("Setting io scheduler for {} to {}",
                 device_path.to_string_lossy(),
                 elevator));
    let device_name = match device_path.file_name() {
        Some(name) => name.to_string_lossy().into_owned(),
        None => "".to_string(),
    };
    let mut f = File::open("/etc/rc.local")?;
    let elevator_cmd = format!("echo {scheduler} > /sys/block/{device}/queue/scheduler",
                               scheduler = elevator,
                               device = device_name);

    let mut script = shellscript::parse(&mut f)?;
    let existing_cmd = script.commands.iter().position(|cmd| cmd.contains(&device_name));
    if let Some(pos) = existing_cmd {
        script.commands.remove(pos);
    }
    script.commands.insert(0, elevator_cmd);
    let mut f = File::create("/etc/rc.local")?;
    let bytes_written = script.write(&mut f)?;
    Ok(bytes_written)
}

pub fn weekly_defrag(mount: &str,
                     fs_type: &FilesystemType,
                     interval: &str)
                     -> Result<usize, ::std::io::Error> {
    log!(format!("Scheduling weekly defrag for {}", mount));
    let crontab = Path::new("/var/spool/cron/crontabs/root");
    let defrag_command = match fs_type {
        &FilesystemType::Ext4 => "e4defrag",
        &FilesystemType::Btrfs => "btrfs filesystem defragment -r",
        &FilesystemType::Xfs => "xfs_fsr",
        _ => "",
    };
    let job = format!("{interval} {cmd} {path}",
                      interval = interval,
                      cmd = defrag_command,
                      path = mount);

    //TODO Change over to using the cronparse library.  Has much better parsing however
    //there's currently no way to add new entries yet
    let mut existing_crontab = {
        if crontab.exists() {
            let mut buff = String::new();
            let mut f = File::open("/var/spool/cron/crontabs/root")?;
            f.read_to_string(&mut buff)?;
            buff.split("\n")
                .map(|s| s.to_string())
                .filter(|s| !s.trim().is_empty())
                .collect::<Vec<String>>()
        } else {
            Vec::new()
        }
    };
    let existing_job_position = existing_crontab.iter().position(|line| line.contains(mount));
    // If we found an existing job we remove the old and insert the new job
    if let Some(pos) = existing_job_position {
        existing_crontab.remove(pos);
    }
    existing_crontab.push(job.clone());

    //Write back out
    let mut f = File::create("/var/spool/cron/crontabs/root")?;
    let mut written_bytes = f.write(&existing_crontab.join("\n").as_bytes())?;
    written_bytes += f.write(&"\n".as_bytes())?;
    Ok(written_bytes)
}

pub fn get_manual_bricks() -> Result<Vec<BrickDevice>, String> {
    log!("Gathering list of manually specified brick devices");
    let manual_config_brick_devices: Vec<String> = get_config_value("brick_devices")
        .unwrap_or("".to_string())
        .split(" ")
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
        .collect();
    log!(format!("List of manual storage brick devices: {:?}",
                 manual_config_brick_devices));
    let bricks = scan_devices(manual_config_brick_devices)?;
    Ok(bricks)
}

pub fn get_juju_bricks() -> Result<Vec<BrickDevice>, String> {
    log!("Gathering list of juju storage brick devices");
    //Get juju storage devices
    let juju_config_brick_devices: Vec<String> = juju::storage_list()
        .unwrap_or("".to_string())
        .lines()
        .filter(|s| !s.is_empty())
        .map(|s| juju::storage_get(s))
        .filter(|s| s.is_ok())
        .filter_map(|s| s.unwrap())
        .map(|s| s.trim().to_string())
        .collect();
    log!(format!("List of juju storage brick devices: {:?}",
                 juju_config_brick_devices));
    let bricks = scan_devices(juju_config_brick_devices)?;
    Ok(bricks)
}
