mod actions;
mod apt;
mod block;
mod ctdb;
mod hooks;
mod metrics;
mod samba;
mod updatedb;
mod upgrade;

extern crate debian;
extern crate fstab;
extern crate gluster;
extern crate ipnetwork;
extern crate itertools;
#[macro_use]
extern crate juju;
extern crate resolve;
extern crate serde_yaml;
extern crate uuid;

use actions::{disable_volume_quota, enable_volume_quota, list_volume_quotas, set_volume_options};
use hooks::brick_detached::brick_detached;
use hooks::config_changed::config_changed;
use hooks::fuse_relation_joined::fuse_relation_joined;
use hooks::nfs_relation_joined::nfs_relation_joined;
use hooks::server_changed::server_changed;
use hooks::server_removed::server_removed;
use metrics::collect_metrics;

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::fs::create_dir;
use std::io::{Error, ErrorKind, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use debian::version::Version;
use gluster::peer::{peer_probe, peer_status, Peer, State};
use gluster::volume::*;
use ipnetwork::IpNetwork;
use itertools::Itertools;
use juju::{JujuError, unitdata};
use resolve::address::address_name;


#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use super::gluster::volume::{Brick, Transport, Volume, VolumeType};
    use super::gluster::peer::{Peer, State};
    use super::uuid::Uuid;

    #[test]
    fn test_all_peers_are_ready() {
        let peers: Vec<Peer> = vec![Peer {
                                        uuid: Uuid::new_v4(),
                                        hostname: format!("host-{}", Uuid::new_v4()),
                                        status: State::PeerInCluster,
                                    },
                                    Peer {
                                        uuid: Uuid::new_v4(),
                                        hostname: format!("host-{}", Uuid::new_v4()),
                                        status: State::PeerInCluster,
                                    }];
        let ready = super::peers_are_ready(Ok(peers));
        println!("Peers are ready: {}", ready);
        assert!(ready);
    }

    #[test]
    fn test_some_peers_are_ready() {
        let peers: Vec<Peer> = vec![Peer {
                                        uuid: Uuid::new_v4(),
                                        hostname: format!("host-{}", Uuid::new_v4()),
                                        status: State::Connected,
                                    },
                                    Peer {
                                        uuid: Uuid::new_v4(),
                                        hostname: format!("host-{}", Uuid::new_v4()),
                                        status: State::PeerInCluster,
                                    }];
        let ready = super::peers_are_ready(Ok(peers));
        println!("Some peers are ready: {}", ready);
        assert_eq!(ready, false);
    }

    #[test]
    fn test_find_new_peers() {
        let peer1 = Peer {
            uuid: Uuid::new_v4(),
            hostname: format!("host-{}", Uuid::new_v4()),
            status: State::PeerInCluster,
        };
        let peer2 = Peer {
            uuid: Uuid::new_v4(),
            hostname: format!("host-{}", Uuid::new_v4()),
            status: State::PeerInCluster,
        };

        // peer1 and peer2 are in the cluster but only peer1 is actually serving a brick.
        // find_new_peers should return peer2 as a new peer
        let peers: Vec<Peer> = vec![peer1.clone(), peer2.clone()];
        let existing_brick = Brick {
            peer: peer1,
            path: PathBuf::from("/mnt/brick1"),
        };

        let volume_info = Volume {
            name: "Test".to_string(),
            vol_type: VolumeType::Replicate,
            id: Uuid::new_v4(),
            status: "online".to_string(),
            transport: Transport::Tcp,
            bricks: vec![existing_brick],
            options: BTreeMap::new(),
        };
        let new_peers = super::find_new_peers(&peers, &volume_info);
        assert_eq!(new_peers, vec![peer2]);
    }

    #[test]
    fn test_cartesian_product() {
        let peer1 = Peer {
            uuid: Uuid::new_v4(),
            hostname: format!("host-{}", Uuid::new_v4()),
            status: State::PeerInCluster,
        };
        let peer2 = Peer {
            uuid: Uuid::new_v4(),
            hostname: format!("host-{}", Uuid::new_v4()),
            status: State::PeerInCluster,
        };
        let peers = vec![peer1.clone(), peer2.clone()];
        let paths = vec!["/mnt/brick1".to_string(), "/mnt/brick2".to_string()];
        let result = super::brick_and_server_cartesian_product(&peers, &paths);
        println!("brick_and_server_cartesian_product: {:?}", result);
        assert_eq!(result,
                   vec![Brick {
                            peer: peer1.clone(),
                            path: PathBuf::from("/mnt/brick1"),
                        },
                        Brick {
                            peer: peer2.clone(),
                            path: PathBuf::from("/mnt/brick1"),
                        },
                        Brick {
                            peer: peer1.clone(),
                            path: PathBuf::from("/mnt/brick2"),
                        },
                        Brick {
                            peer: peer2.clone(),
                            path: PathBuf::from("/mnt/brick2"),
                        }]);
    }
}

// Need more expressive return values so we can wait on peers
#[derive(Debug)]
#[allow(dead_code)]
enum Status {
    Created,
    WaitForMorePeers,
    InvalidConfig(String),
    FailedToCreate(String),
    FailedToStart(String),
}



fn get_config_value(name: &str) -> Result<String, String> {
    match juju::config_get(&name.to_string()) {
        Ok(v) => Ok(v),
        Err(e) => {
            return Err(e.to_string());
        }
    }
}

// sysctl: a YAML-formatted string of sysctl options eg "{ 'kernel.max_pid': 1337 }"
fn create_sysctl<T: Write>(sysctl: String, f: &mut T) -> Result<usize, String> {
    let deserialized_map: BTreeMap<String, String> =
        serde_yaml::from_str(&sysctl).map_err(|e| e.to_string())?;

    let mut bytes_written = 0;
    for (key, value) in deserialized_map {
        bytes_written += f.write(&format!("{}={}\n", key, value).as_bytes())
            .map_err(|e| e.to_string())?;
    }
    Ok(bytes_written)
}

// Return all the virtual ip networks that will be used
fn get_cluster_networks() -> Result<Vec<ctdb::VirtualIp>, String> {
    let mut cluster_networks: Vec<ctdb::VirtualIp> = Vec::new();
    let config_value = juju::config_get("virtual_ip_addresses").map_err(|e| e.to_string())?;
    let virtual_ips: Vec<&str> = config_value.split(" ").collect();
    for vip in virtual_ips {
        if vip.is_empty() {
            continue;
        }
        let network = ctdb::ipnetwork_from_str(vip)?;
        let interface = ctdb::get_interface_for_address(network)
            .ok_or(format!("Failed to find interface for network {:?}", network))?;
        cluster_networks.push(ctdb::VirtualIp {
                                  cidr: network,
                                  interface: interface,
                              });
    }
    Ok(cluster_networks)
}

fn peers_are_ready(peers: Result<Vec<Peer>, gluster::GlusterError>) -> bool {
    match peers {
        Ok(peer_list) => {
            // Ensure all peers are in a PeerInCluster state
            log!(format!("Got peer status: {:?}", peer_list));
            return peer_list.iter().all(|peer| peer.status == State::PeerInCluster);
        }
        Err(err) => {
            log!(format!("peers_are_ready failed to get peer status: {:?}", err),
                 Error);
            return false;
        }
    }
}

// HDD's are so slow that sometimes the peers take long to join the cluster.
// This will loop and wait for them ie spinlock
fn wait_for_peers() -> Result<(), String> {
    log!("Waiting for all peers to enter the Peer in Cluster status");
    status_set!(Maintenance "Waiting for all peers to enter the \"Peer in Cluster status\"");
    let mut iterations = 0;
    while !peers_are_ready(peer_status()) {
        thread::sleep(Duration::from_secs(1));
        iterations += 1;
        if iterations > 600 {
            return Err("Gluster peers failed to connect after 10 minutes".to_string());
        }
    }
    return Ok(());
}

// Probe in a unit if they haven't joined yet
// This function is confusing because Gluster has weird behavior.
// 1. If you probe in units by their IP address it works.  The CLI will show you their resolved
// hostnames however
// 2. If you probe in units by their hostname instead it'll still work but gluster client mount
// commands will fail if it can not resolve the hostname.
// For example: Probing in containers by hostname will cause the glusterfs client to fail to mount
// on the container host.  :(
// 3. To get around this I'm converting hostnames to ip addresses in the gluster library to mask
// this from the callers.
//
fn probe_in_units(existing_peers: &Vec<Peer>,
                  related_units: Vec<juju::Relation>)
                  -> Result<(), String> {

    log!(format!("Adding in related_units: {:?}", related_units));
    for unit in related_units {
        let address = juju::relation_get_by_unit(&"private-address".to_string(), &unit)
            .map_err(|e| e.to_string())?;
        let address_trimmed = address.trim().to_string();
        let already_probed = existing_peers.iter().any(|peer| peer.hostname == address_trimmed);

        // Probe the peer in
        if !already_probed {
            log!(format!("Adding {} to cluster", &address_trimmed));
            match peer_probe(&address_trimmed) {
                Ok(_) => {
                    log!("Gluster peer probe was successful");
                }
                Err(why) => {
                    log!(format!("Gluster peer probe failed: {:?}", why), Error);
                    return Err(why.to_string());
                }
            };
        }
    }
    return Ok(());
}

fn find_new_peers(peers: &Vec<Peer>, volume_info: &Volume) -> Vec<Peer> {
    let mut new_peers: Vec<Peer> = Vec::new();
    for peer in peers {
        // If this peer is already in the volume, skip it
        let existing_peer = volume_info.bricks.iter().any(|brick| brick.peer.uuid == peer.uuid);
        if !existing_peer {
            new_peers.push(peer.clone());
        }
    }
    return new_peers;
}

fn brick_and_server_cartesian_product(peers: &Vec<Peer>,
                                      paths: &Vec<String>)
                                      -> Vec<gluster::volume::Brick> {
    let mut product: Vec<gluster::volume::Brick> = Vec::new();

    let it = paths.iter().cartesian_product(peers.iter());
    for (path, host) in it {
        let brick = gluster::volume::Brick {
            peer: host.clone(),
            path: PathBuf::from(path),
        };
        product.push(brick);
    }
    return product;
}

fn ephemeral_unmount() -> Result<(), String> {
    match get_config_value("ephemeral_unmount") {
        Ok(mountpoint) => {
            if mountpoint.is_empty() {
                return Ok(());
            }
            // Remove the entry from the fstab if it's set
            let fstab = fstab::FsTab::new(&Path::new("/etc/fstab"));
            log!("Removing ephemeral mount from fstab");
            fstab.remove_entry(&mountpoint).map_err(|e| e.to_string())?;

            if is_mounted(&mountpoint)? {
                let mut cmd = std::process::Command::new("umount");
                cmd.arg(mountpoint);
                let output = cmd.output().map_err(|e| e.to_string())?;
                if !output.status.success() {
                    return Err(String::from_utf8_lossy(&output.stderr).into_owned());
                }
                // Unmounted Ok
                return Ok(());
            }
            // Not mounted
            Ok(())
        }
        _ => {
            // No-op
            Ok(())
        }
    }
}

// Given a dev device path /dev/xvdb this will check to see if the device
// has been formatted and mounted
fn device_initialized(brick_path: &PathBuf) -> Result<bool, JujuError> {
    // Connect to the default unitdata database
    log!("Connecting to unitdata storage");
    let unit_storage = unitdata::Storage::new(None)?;
    log!("Getting unit_info");
    let unit_info = unit_storage.get::<bool>(&brick_path.to_string_lossy())?;
    log!(format!("unit_info: {:?}", unit_info));
    // Either it's Some() and we know about the unit
    // or it's None and we don't know and therefore it's not initialized
    Ok(unit_info.unwrap_or(false))
}

fn finish_initialization(device_path: &PathBuf) -> Result<(), Error> {
    let filesystem_config_value =
        get_config_value("filesystem_type").map_err(|e| Error::new(ErrorKind::Other, e))?;
    let filesystem_type = block::FilesystemType::from_str(&filesystem_config_value);
    let mount_path = format!("/mnt/{}",
                             device_path.file_name().unwrap().to_string_lossy());

    let unit_storage = unitdata::Storage::new(None).map_err(|e| Error::new(ErrorKind::Other, e))?;
    let device_info =
        block::get_device_info(device_path).map_err(|e| Error::new(ErrorKind::Other, e))?;
    log!(format!("device_info: {:?}", device_info), Info);

    //Zfs automatically handles mounting the device
    if filesystem_type != block::FilesystemType::Zfs {
        log!(format!("Mounting block device {:?} at {}", &device_path, mount_path),
             Info);
        status_set!(Maintenance
            format!("Mounting block device {:?} at {}", &device_path, mount_path));

        if !Path::new(&mount_path).exists() {
            log!(format!("Creating mount directory: {}", &mount_path), Info);
            create_dir(&mount_path)?;
        }

        block::mount_device(&device_info, &mount_path)
            .map_err(|e| Error::new(ErrorKind::Other, e))?;
        let fstab_entry = fstab::FsEntry {
            fs_spec: format!("UUID={}",
                             device_info.id
                                 .unwrap()
                                 .hyphenated()
                                 .to_string()),
            mountpoint: PathBuf::from(&mount_path),
            vfs_type: device_info.fs_type.to_string(),
            mount_options: vec!["defaults".to_string()],
            dump: false,
            fsck_order: 2,
        };
        log!(format!("Adding {:?} to fstab", fstab_entry));
        let fstab = fstab::FsTab::new(&Path::new("/etc/fstab"));
        fstab.add_entry(fstab_entry)?;
    }
    unit_storage.set(&device_path.to_string_lossy(), true)
        .map_err(|e| Error::new(ErrorKind::Other, e))?;
    log!(format!("Removing mount path from updatedb {:?}", mount_path),
         Info);
    updatedb::add_to_prunepath(&mount_path, &Path::new("/etc/updatedb.conf"))?;
    Ok(())
}

// Format and mount block devices to ready them for consumption by Gluster
// Return an Initialization struct
fn initialize_storage(device: block::BrickDevice) -> Result<block::AsyncInit, String> {
    let filesystem_config_value = get_config_value("filesystem_type")?;
    let filesystem_type = block::FilesystemType::from_str(&filesystem_config_value);
    let init: block::AsyncInit;

    // Format with the default XFS unless told otherwise
    match filesystem_type {
        block::FilesystemType::Xfs => {
            log!(format!("Formatting block device with XFS: {:?}", &device.dev_path),
                 Info);
            status_set!(Maintenance
                format!("Formatting block device with XFS: {:?}", &device.dev_path));

            let filesystem_type = block::Filesystem::Xfs {
                inode_size: None,
                force: true,
            };
            init = block::format_block_device(device, &filesystem_type)?;
        }
        block::FilesystemType::Ext4 => {
            log!(format!("Formatting block device with Ext4: {:?}", &device.dev_path),
                 Info);
            status_set!(Maintenance
                format!("Formatting block device with Ext4: {:?}", &device.dev_path));

            let filesystem_type = block::Filesystem::Ext4 {
                inode_size: 0,
                reserved_blocks_percentage: 0,
            };
            init = block::format_block_device(device, &filesystem_type)?;
        }
        block::FilesystemType::Btrfs => {
            log!(format!("Formatting block device with Btrfs: {:?}", &device.dev_path),
                 Info);
            status_set!(Maintenance
                format!("Formatting block device with Btrfs: {:?}", &device.dev_path));

            let filesystem_type = block::Filesystem::Btrfs {
                leaf_size: 0,
                node_size: 0,
                metadata_profile: block::MetadataProfile::Single,
            };
            init = block::format_block_device(device, &filesystem_type)?;
        }
        block::FilesystemType::Zfs => {
            log!(format!("Formatting block device with ZFS: {:?}", &device.dev_path),
                 Info);
            status_set!(Maintenance
                format!("Formatting block device with ZFS: {:?}", &device.dev_path));
            let filesystem_type = block::Filesystem::Zfs {
                compression: None,
                block_size: None,
            };
            init = block::format_block_device(device, &filesystem_type)?;
        }
        _ => {
            log!(format!("Formatting block device with XFS: {:?}", &device.dev_path),
                 Info);
            status_set!(Maintenance
                format!("Formatting block device with XFS: {:?}", &device.dev_path));

            let filesystem_type = block::Filesystem::Xfs {
                inode_size: None,
                force: true,
            };
            init = block::format_block_device(device, &filesystem_type)?;
        }
    }
    return Ok(init);
}

fn resolve_first_vip_to_dns() -> Result<String, String> {
    let cluster_networks = get_cluster_networks()?;
    match cluster_networks.first() {
        Some(cluster_network) => {
            match cluster_network.cidr {
                IpNetwork::V4(ref v4_network) => {
                    // Resolve the ipv4 address back to a dns string
                    Ok(address_name(&::std::net::IpAddr::V4(v4_network.ip())))
                }
                IpNetwork::V6(ref v6_network) => {
                    // Resolve the ipv6 address back to a dns string
                    Ok(address_name(&::std::net::IpAddr::V6(v6_network.ip())))
                }
            }
        }
        None => {
            // No vips were set?
            Err("virtual_ip_addresses has no addresses set".to_string())
        }
    }
}

fn get_glusterfs_version() -> Result<Version, String> {
    let mut cmd = std::process::Command::new("dpkg");
    cmd.arg("-s");
    cmd.arg("glusterfs-server");
    let output = cmd.output().map_err(|e| e.to_string())?;
    if output.status.success() {
        let output_str = String::from_utf8_lossy(&output.stdout).into_owned();
        for line in output_str.lines() {
            if line.starts_with("Version") {
                // return the version
                let parts: Vec<&str> = line.split(" ").collect();
                if parts.len() == 2 {
                    let parse_version = Version::parse(&parts[1]).map_err(|e| e.msg)?;
                    return Ok(parse_version);
                } else {
                    return Err(format!("apt-cache Verion string is invalid: {}", line));
                }
            }
        }
    } else {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned());
    }
    return Err("Unable to find glusterfs-server version".to_string());
}

fn is_mounted(directory: &str) -> Result<bool, String> {
    let path = Path::new(directory);
    let parent = path.parent();

    let dir_metadata = try!(fs::metadata(directory).map_err(|e| e.to_string()));
    let file_type = dir_metadata.file_type();

    if file_type.is_symlink() {
        // A symlink can never be a mount point
        return Ok(false);
    }

    if parent.is_some() {
        let parent_metadata = try!(fs::metadata(parent.unwrap()).map_err(|e| e.to_string()));
        if parent_metadata.dev() != dir_metadata.dev() {
            // path/.. on a different device as path
            return Ok(true);
        }
    } else {
        // If the directory doesn't have a parent it's the root filesystem
        return Ok(false);
    }
    return Ok(false);
}

// Mount the cluster at /mnt/glusterfs using fuse
fn mount_cluster(volume_name: &str) -> Result<(), String> {
    if !Path::new("/mnt/glusterfs").exists() {
        create_dir("/mnt/glusterfs").map_err(|e| e.to_string())?;
    }
    if !is_mounted("/mnt/glusterfs")? {
        let mut cmd = std::process::Command::new("mount");
        cmd.arg("-t");
        cmd.arg("glusterfs");
        cmd.arg(&format!("localhost:/{}", volume_name));
        cmd.arg("/mnt/glusterfs");
        let output = cmd.output().map_err(|e| e.to_string())?;
        if output.status.success() {
            log!("Removing /mnt/glusterfs from updatedb", Info);
            updatedb::add_to_prunepath(&String::from("/mnt/glusterfs"),
                                   &Path::new("/etc/updatedb.conf")).map_err(|e| e.to_string())?;
            return Ok(());
        } else {
            return Err(String::from_utf8_lossy(&output.stderr).into_owned());
        }

    }
    return Ok(());
}

// Update the juju status information
fn update_status() -> Result<(), String> {
    let version = get_glusterfs_version()?;
    juju::application_version_set(&format!("{}", version.upstream_version))
        .map_err(|e| e.to_string())?;
    let volume_name = get_config_value("volume_name")?;

    let local_bricks = gluster::get_local_bricks(&volume_name);
    match local_bricks {
        Ok(bricks) => {
            status_set!(Active format!("Unit is ready ({} bricks)", bricks.len()));
            // Ensure the cluster is mounted
            mount_cluster(&volume_name)?;
            Ok(())
        }
        Err(gluster::GlusterError::NoVolumesPresent) => {
            status_set!(Blocked "No bricks found");
            Ok(())
        }
        _ => Ok(()),
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() > 0 {
        // Register our hooks with the Juju library
        let hook_registry: Vec<juju::Hook> =
            vec![hook!("brick-storage-detaching", brick_detached),
                 hook!("collect-metrics", collect_metrics),
                 hook!("config-changed", config_changed),
                 hook!("create-volume-quota", enable_volume_quota),
                 hook!("delete-volume-quota", disable_volume_quota),
                 hook!("fuse-relation-joined", fuse_relation_joined),
                 hook!("list-volume-quotas", list_volume_quotas),
                 hook!("nfs-relation-joined", nfs_relation_joined),
                 hook!("server-relation-changed", server_changed),
                 hook!("server-relation-departed", server_removed),
                 hook!("set-volume-options", set_volume_options),
                 hook!("update-status", update_status)];

        let result = juju::process_hooks(hook_registry);

        if result.is_err() {
            log!(format!("Hook failed with error: {:?}", result.err()), Error);
        }
        update_status();
    }
}
