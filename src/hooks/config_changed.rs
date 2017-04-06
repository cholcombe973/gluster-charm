extern crate gluster;
extern crate juju;

use std::fs::File;
use std::path::Path;
use std::process::Command;

use super::super::{create_sysctl, ephemeral_unmount, finish_initialization, get_glusterfs_version,
                   initialize_storage};
use super::super::apt;
use super::super::block;
use super::super::upgrade;

pub fn config_changed() -> Result<(), String> {
    if let Err(err) = check_for_new_devices() {
        log!(format!("Checking for new devices failed with error: {}", err),
             Error);
    }
    if let Err(err) = check_for_sysctl() {
        log!(format!("Setting sysctl's failed with error: {}", err),
             Error);
    }
    // If fails we fail the hook
    check_for_upgrade()?;
    return Ok(());
}

fn check_for_new_devices() -> Result<(), String> {
    log!("Checking for new devices", Info);
    let config = juju::Config::new().map_err(|e| e.to_string())?;
    log!("Checking for ephemeral unmount");
    ephemeral_unmount()?;
    //if config.changed("brick_devices").map_err(|e| e.to_string())? {
    let mut brick_devices: Vec<block::BrickDevice> = Vec::new();
    // Get user configured storage devices
    let manual_brick_devices = block::get_manual_bricks()?;
    brick_devices.extend(manual_brick_devices);

    // Get the juju storage block devices
    let juju_config_brick_devices = block::get_juju_bricks()?;
    brick_devices.extend(juju_config_brick_devices);

    log!(format!("storage devices: {:?}", brick_devices));

    let mut format_handles: Vec<block::AsyncInit> = Vec::new();
    let mut brick_paths: Vec<String> = Vec::new();
    // Format all drives in parallel
    for device in &mut brick_devices {
        if !device.initialized {
            log!(format!("Calling initialize_storage for {:?}", device.dev_path));
            // Spawn all format commands in the background
            format_handles.push(initialize_storage(device.clone())?);
        } else {
            // The device is already initialized, lets add it to our usable paths list
            log!(format!("{:?} is already initialized", device.dev_path));
            brick_paths.push(device.mount_path.clone());
        }
    }
    // Wait for all children to finish formatting their drives
    for handle in format_handles {
        let output_result = handle.format_child.wait_with_output();
        match output_result {
            Ok(output) => {
                match block::process_output(output) {
                    Ok(_) => {
                        // success
                        // 1. Run any post setup commands if needed
                        finish_initialization(&handle.device.dev_path).map_err(|e| e.to_string())?;
                        brick_paths.push(handle.device.mount_path.clone());
                    }
                    Err(e) => {
                        // Failed
                        log!(format!("Device {:?} formatting failed with error: {}. Skipping",
                                     &handle.device.dev_path,
                                     e),
                             Error);
                    }
                }
            }
            Err(e) => {
                //Failed
                log!(format!("Device {:?} formatting failed with error: {}. Skipping",
                             &handle.device.dev_path,
                             e),
                     Error);
            }
        }
    }
    log!(format!("Usable brick paths: {:?}", brick_paths));
    //} else {
    //    log!("No new devices found");
    //}
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
