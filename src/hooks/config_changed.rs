extern crate gluster;
extern crate juju;

use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::Command;

use gluster::volume::*;
use super::super::{create_sysctl, ephemeral_unmount, device_initialized, get_config_value,
                   get_glusterfs_version, initialize_storage, is_mounted};
use super::super::apt;
use super::super::block;
use super::super::upgrade;

pub fn config_changed() -> Result<(), String> {
    // If either of these fail we fail the hook
    check_for_new_devices()?;
    check_for_upgrade()?;

    // If this fails don't fail the hook
    if let Err(err) = check_for_sysctl() {
        log!(format!("Setting sysctl's failed with error: {}", err),
             Error);
    }
    return Ok(());
}

fn check_for_new_devices() -> Result<(), String> {
    log!("Checking for new devices", Info);
    let config = juju::Config::new().map_err(|e| e.to_string())?;
    if config.changed("brick_devices").map_err(|e| e.to_string())? {
        // Get the changed list of brick devices and initialize each one
        let brick_paths: Vec<PathBuf> = get_config_value("brick_devices")
            .unwrap_or("".to_string())
            .split(" ")
            .map(|s| s.to_string())
            .filter(|s| !s.is_empty())
            .map(|s| PathBuf::from(s))
            .collect();
        // Check for any devices that are mounted and skip those.  They're already taken care of
        for brick in brick_paths {
            if block::is_block_device(&brick).is_err() {
                log!(format!("{:?} is not a block device. Skipping.", brick));
                continue;
            }
            // If ephemeral-unmount is set and the directory is mounted we unmount it.
            // Otherwise nothing happens
            ephemeral_unmount()?; // TODO: Should this fail the hook or just skip?

            let brick_filename = match brick.file_name() {
                Some(name) => name,
                None => {
                    log!(format!("Unable to determine filename for device: {:?}. Skipping",
                                 brick),
                         Error);
                    continue;
                }
            };
            let mount_path = format!("/mnt/{}", brick_filename.to_string_lossy());
            log!(format!("Checking if {:?} is mounted", mount_path));
            if Path::new(&mount_path).exists() {
                match is_mounted(&mount_path) {
                    Ok(mounted) => {
                        if mounted {
                            log!(format!("{:?} is mounted. Skipping", brick), Error);
                            continue;
                        }
                    }
                    Err(_) => {}
                };
            }
            if !device_initialized(&brick).map_err(|e| e.to_string())? {
                log!(format!("Calling initialize_storage for {:?}", brick));
                initialize_storage(&brick)?;
            }
        }
    } else {
        log!("No new devices found");
    }
    Ok(())
}

fn check_for_sysctl() -> Result<(), String> {
    let config = juju::Config::new().map_err(|e| e.to_string())?;
    if config.changed("sysctl").map_err(|e| e.to_string())? {
        let config_path = Path::new("/etc/sysctl.d/50-gluster-charm.conf");
        let mut sysctl_file = File::create(config_path).map_err(|e| e.to_string())?;
        let sysctl_dict = juju::config_get("sysctl").map_err(|e| e.to_string())?;
        create_sysctl(sysctl_dict, &mut sysctl_file)?;
        // Reload sysctl's
        let mut cmd = Command::new("sysctl");
        cmd.arg("-p");
        cmd.arg(&config_path.to_string_lossy().into_owned());
        let output = cmd.output().map_err(|e| e.to_string())?;
        if !output.status.success() {
            return Err(String::from_utf8_lossy(&output.stderr).into_owned());
        }
    }
    Ok(())
}

// If the config has changed this will initiated a rolling upgrade
fn check_for_upgrade() -> Result<(), String> {
    let config = juju::Config::new().map_err(|e| e.to_string())?;
    if !config.changed("source").map_err(|e| e.to_string())? {
        // No upgrade requested
        log!("No upgrade requested");
        return Ok(());
    }

    log!("Getting current_version");
    let current_version = get_glusterfs_version()?;

    log!("Adding new source line");
    let source = juju::config_get("source").map_err(|e| e.to_string())?;
    apt::add_source(&source)?;
    log!("Calling apt update");
    apt::apt_update()?;

    log!("Getting proposed_version");
    let proposed_version = apt::get_candidate_package_version("glusterfs-server")?;

    // Using semantic versioning if the new version is greater than we allow the upgrade
    if proposed_version > current_version {
        log!(format!("current_version: {}", current_version));
        log!(format!("new_version: {}", proposed_version));
        log!(format!("{} to {} is a valid upgrade path.  Proceeding.",
                     current_version,
                     proposed_version));
        return upgrade::roll_cluster(&proposed_version);
    } else {
        // Log a helpful error message
        log!(format!("Invalid upgrade path from {} to {}. The new version needs to be \
                            greater than the old version",
                     current_version,
                     proposed_version),
             Error);
        return Ok(());
    }
}
