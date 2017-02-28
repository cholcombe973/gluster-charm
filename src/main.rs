mod actions;
mod apt;
mod block;
mod ctdb;
mod metrics;
mod samba;
mod updatedb;
mod upgrade;

extern crate debian;
extern crate gluster;
extern crate ipnetwork;
extern crate itertools;
#[macro_use]
extern crate juju;
extern crate resolve;
extern crate serde_yaml;
extern crate uuid;

use actions::{disable_volume_quota, enable_volume_quota, list_volume_quotas, set_volume_options};
use metrics::collect_metrics;

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::fs::{create_dir, File};
use std::io::{Read, Write};
use std::net::IpAddr;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::thread;
use std::time::Duration;

use debian::version::Version;
use gluster::{GlusterOption, SplitBrainPolicy, Toggle};
use ipnetwork::IpNetwork;
use itertools::Itertools;
use resolve::address::address_name;
use samba::setup_samba;


#[cfg(test)]
mod tests {
    // extern crate uuid;
    use std::collections::BTreeMap;
    use std::fs::File;
    use std::io::prelude::Read;
    use std::path::PathBuf;
    use super::gluster;
    use super::uuid;

    #[test]
    fn test_all_peers_are_ready() {
        let peers: Vec<gluster::Peer> = vec![gluster::Peer {
                                                 uuid: uuid::Uuid::new_v4(),
                                                 hostname: format!("host-{}", uuid::Uuid::new_v4()),
                                                 status: gluster::State::PeerInCluster,
                                             },
                                             gluster::Peer {
                                                 uuid: uuid::Uuid::new_v4(),
                                                 hostname: format!("host-{}", uuid::Uuid::new_v4()),
                                                 status: gluster::State::PeerInCluster,
                                             }];
        let ready = super::peers_are_ready(Ok(peers));
        println!("Peers are ready: {}", ready);
        assert!(ready);
    }

    #[test]
    fn test_some_peers_are_ready() {
        let peers: Vec<gluster::Peer> = vec![gluster::Peer {
                                                 uuid: uuid::Uuid::new_v4(),
                                                 hostname: format!("host-{}", uuid::Uuid::new_v4()),
                                                 status: gluster::State::Connected,
                                             },
                                             gluster::Peer {
                                                 uuid: uuid::Uuid::new_v4(),
                                                 hostname: format!("host-{}", uuid::Uuid::new_v4()),
                                                 status: gluster::State::PeerInCluster,
                                             }];
        let ready = super::peers_are_ready(Ok(peers));
        println!("Some peers are ready: {}", ready);
        assert_eq!(ready, false);
    }

    #[test]
    fn test_find_new_peers() {
        let peer1 = gluster::Peer {
            uuid: uuid::Uuid::new_v4(),
            hostname: format!("host-{}", uuid::Uuid::new_v4()),
            status: gluster::State::PeerInCluster,
        };
        let peer2 = gluster::Peer {
            uuid: uuid::Uuid::new_v4(),
            hostname: format!("host-{}", uuid::Uuid::new_v4()),
            status: gluster::State::PeerInCluster,
        };

        // peer1 and peer2 are in the cluster but only peer1 is actually serving a brick.
        // find_new_peers should return peer2 as a new peer
        let peers: Vec<gluster::Peer> = vec![peer1.clone(), peer2.clone()];
        let existing_brick = gluster::Brick {
            peer: peer1,
            path: PathBuf::from("/mnt/brick1"),
        };

        let volume_info = gluster::Volume {
            name: "Test".to_string(),
            vol_type: gluster::VolumeType::Replicate,
            id: uuid::Uuid::new_v4(),
            status: "online".to_string(),
            transport: gluster::Transport::Tcp,
            bricks: vec![existing_brick],
            options: BTreeMap::new(),
        };
        let new_peers = super::find_new_peers(&peers, &volume_info);
        assert_eq!(new_peers, vec![peer2]);
    }

    #[test]
    fn test_cartesian_product() {
        let peer1 = gluster::Peer {
            uuid: uuid::Uuid::new_v4(),
            hostname: format!("host-{}", uuid::Uuid::new_v4()),
            status: gluster::State::PeerInCluster,
        };
        let peer2 = gluster::Peer {
            uuid: uuid::Uuid::new_v4(),
            hostname: format!("host-{}", uuid::Uuid::new_v4()),
            status: gluster::State::PeerInCluster,
        };
        let peers = vec![peer1.clone(), peer2.clone()];
        let paths = vec!["/mnt/brick1".to_string(), "/mnt/brick2".to_string()];
        let result = super::brick_and_server_cartesian_product(&peers, &paths);
        println!("brick_and_server_cartesian_product: {:?}", result);
        assert_eq!(result,
                   vec![gluster::Brick {
                            peer: peer1.clone(),
                            path: PathBuf::from("/mnt/brick1"),
                        },
                        gluster::Brick {
                            peer: peer2.clone(),
                            path: PathBuf::from("/mnt/brick1"),
                        },
                        gluster::Brick {
                            peer: peer1.clone(),
                            path: PathBuf::from("/mnt/brick2"),
                        },
                        gluster::Brick {
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

fn config_changed() -> Result<(), String> {
    check_for_upgrade()?;
    if let Err(err) = check_for_sysctl() {
        log!(format!("Setting sysctl's failed with error: {}", err),
             Error);
    }
    return Ok(());
}

fn check_for_sysctl() -> Result<(), String> {
    let config = juju::Config::new().map_err(|e| e.to_string())?;
    if config.changed("sysctl").map_err(|e| e.to_string())? {
        let config_path = Path::new("/etc/sysctl.d/50-gluster-charm.conf");
        let mut sysctl_file = File::create(config_path).map_err(|e| e.to_string())?;
        let sysctl_dict = juju::config_get("sysctl").map_err(|e| e.to_string())?;
        create_sysctl(sysctl_dict, &mut sysctl_file)?;
        // Reload sysctl's
        let mut cmd = std::process::Command::new("sysctl");
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

// Return all the virtual ip networks that will be used
fn get_cluster_networks() -> Result<Vec<ctdb::VirtualIp>, String> {
    let mut cluster_networks: Vec<ctdb::VirtualIp> = Vec::new();
    let config_value = juju::config_get("virtual_ip_addresses").map_err(|e| e.to_string())?;
    let virtual_ips: Vec<&str> = config_value.split(" ")
        .collect();
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

// Add all the peers in the gluster cluster to the ctdb cluster
fn setup_ctdb() -> Result<(), String> {
    if juju::config_get("virtual_ip_addresses").map_err(|e| e.to_string())?.is_empty() {
        // virtual_ip_addresses isn't set.  Skip setting ctdb up
        return Ok(());
    }
    log!("setting up ctdb");
    let peers = gluster::peer_list().map_err(|e| e.to_string())?;
    log!(format!("Got ctdb peer list: {:?}", peers));
    let mut cluster_addresses: Vec<IpAddr> = Vec::new();
    for peer in peers {
        let address = IpAddr::from_str(&peer.hostname).map_err(|e| e.to_string())?;
        cluster_addresses.push(address)
    }

    log!("writing /etc/default/ctdb");
    let mut ctdb_conf = File::create("/etc/default/ctdb").map_err(|e| e.to_string())?;
    ctdb::render_ctdb_configuration(&mut ctdb_conf).map_err(|e| e.to_string())?;

    let cluster_networks = get_cluster_networks()?;

    log!("writing /etc/ctdb/public_addresses");
    let mut public_addresses =
        File::create("/etc/ctdb/public_addresses").map_err(|e| e.to_string())?;
    ctdb::render_ctdb_public_addresses(&mut public_addresses, &cluster_networks)
        .map_err(|e| e.to_string())?;

    log!("writing /etc/ctdb/nodes");
    let mut cluster_nodes = File::create("/etc/ctdb/nodes").map_err(|e| e.to_string())?;

    ctdb::render_ctdb_cluster_nodes(&mut cluster_nodes, &cluster_addresses)
        .map_err(|e| e.to_string())?;

    // Start the ctdb service
    log!("Starting ctdb");
    apt::service_start("ctdb")?;

    Ok(())
}

fn peers_are_ready(peers: Result<Vec<gluster::Peer>, gluster::GlusterError>) -> bool {
    match peers {
        Ok(peer_list) => {
            // Ensure all peers are in a PeerInCluster state
            log!(format!("Got peer status: {:?}", peer_list));
            return peer_list.iter().all(|peer| peer.status == gluster::State::PeerInCluster);
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
    while !peers_are_ready(gluster::peer_status()) {
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
fn probe_in_units(existing_peers: &Vec<gluster::Peer>,
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
            match gluster::peer_probe(&address_trimmed) {
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

fn find_new_peers(peers: &Vec<gluster::Peer>, volume_info: &gluster::Volume) -> Vec<gluster::Peer> {
    let mut new_peers: Vec<gluster::Peer> = Vec::new();
    for peer in peers {
        // If this peer is already in the volume, skip it
        let existing_peer = volume_info.bricks.iter().any(|brick| brick.peer.uuid == peer.uuid);
        if !existing_peer {
            new_peers.push(peer.clone());
        }
    }
    return new_peers;
}

fn brick_and_server_cartesian_product(peers: &Vec<gluster::Peer>,
                                      paths: &Vec<String>)
                                      -> Vec<gluster::Brick> {
    let mut product: Vec<gluster::Brick> = Vec::new();

    let it = paths.iter().cartesian_product(peers.iter());
    for (path, host) in it {
        let brick = gluster::Brick {
            peer: host.clone(),
            path: PathBuf::from(path),
        };
        product.push(brick);
    }
    return product;
}

// This function will take into account the replication level and
// try its hardest to produce a list of bricks that satisfy this:
// 1. Are not already in the volume
// 2. Sufficient hosts to satisfy replication level
// 3. Stripped across the hosts
// If insufficient hosts exist to satisfy this replication level this will return no new bricks
// to add
fn get_brick_list(peers: &Vec<gluster::Peer>,
                  volume: Option<gluster::Volume>)
                  -> Result<Vec<gluster::Brick>, Status> {

    // Default to 3 replicas if the parsing fails
    let replica_config = get_config_value("replication_level").unwrap_or("3".to_string());
    let replicas = replica_config.parse().unwrap_or(3);
    let mut brick_paths: Vec<String> = Vec::new();

    let bricks = juju::storage_list().unwrap();
    log!(format!("storage_list: {:?}", bricks));

    for brick in bricks.lines() {
        // This is the /dev/ location.
        let storage_location = juju::storage_get(brick.trim()).unwrap();
        // Translate to mount location
        let brick_path = PathBuf::from(storage_location.trim());
        let mount_path = format!("/mnt/{}", brick_path.file_name().unwrap().to_string_lossy());

        brick_paths.push(mount_path);
    }

    if volume.is_none() {
        log!("Volume is none");
        // number of bricks % replicas == 0 then we're ok to proceed
        if peers.len() < replicas {
            // Not enough peers to replicate across
            log!("Not enough peers to satisfy the replication level for the Gluster \
                        volume.  Waiting for more peers to join.");
            return Err(Status::WaitForMorePeers);
        } else if peers.len() == replicas {
            // Case 1: A perfect marriage of peers and number of replicas
            log!("Number of peers and number of replicas match");
            return Ok(brick_and_server_cartesian_product(peers, &brick_paths));
        } else {
            // Case 2: We have a mismatch of replicas and hosts
            // Take as many as we can and leave the rest for a later time
            let count = peers.len() - (peers.len() % replicas);
            let mut new_peers = peers.clone();

            // Drop these peers off the end of the list
            new_peers.truncate(count);
            log!(format!("Too many new peers.  Dropping {} peers off the list", count));
            return Ok(brick_and_server_cartesian_product(&new_peers, &brick_paths));
        }
    } else {
        // Existing volume.  Build a differential list.
        log!("Existing volume.  Building differential brick list");
        let mut new_peers = find_new_peers(peers, &volume.unwrap());

        if new_peers.len() < replicas {
            log!("New peers found are less than needed by the replica count");
            return Err(Status::WaitForMorePeers);
        } else if new_peers.len() == replicas {
            log!("New peers and number of replicas match");
            return Ok(brick_and_server_cartesian_product(&new_peers, &brick_paths));
        } else {
            let count = new_peers.len() - (new_peers.len() % replicas);
            // Drop these peers off the end of the list
            log!(format!("Too many new peers.  Dropping {} peers off the list", count));
            new_peers.truncate(count);
            return Ok(brick_and_server_cartesian_product(&new_peers, &brick_paths));
        }
    }
}

// Create a new volume if enough peers are available
fn create_volume(peers: &Vec<gluster::Peer>,
                 volume_info: Option<gluster::Volume>)
                 -> Result<Status, String> {
    let cluster_type_config = get_config_value("cluster_type")?;
    let cluster_type = gluster::VolumeType::from_str(&cluster_type_config);
    let volume_name = get_config_value("volume_name")?;
    let replicas = match get_config_value("replication_level")?.parse() {
        Ok(r) => r,
        Err(e) => {
            log!(format!("Invalid config value for replicas.  Defaulting to 3. Error was \
                                {}",
                         e),
                 Error);
            3
        }
    };

    // Make sure all peers are in the cluster
    // spinlock
    wait_for_peers()?;

    // Build the brick list
    let brick_list = match get_brick_list(&peers, volume_info) {
        Ok(list) => list,
        Err(e) => {
            match e {
                Status::WaitForMorePeers => {
                    log!("Waiting for more peers", Info);
                    status_set!(Maintenance "Waiting for more peers");
                    return Ok(Status::WaitForMorePeers);
                }
                Status::InvalidConfig(config_err) => {
                    return Err(config_err);
                }
                _ => {
                    // Some other error
                    return Err(format!("Unknown error in create volume: {:?}", e));
                }
            }
        }
    };
    log!(format!("Got brick list: {:?}", brick_list));

    // Check to make sure the bricks are formatted and mounted
    // let clean_bricks = try!(check_brick_list(&brick_list).map_err(|e| e.to_string()));

    log!(format!("Creating volume of type {:?} with brick list {:?}",
                 cluster_type,
                 brick_list),
         Info);

    match cluster_type {
        gluster::VolumeType::Distribute => {
            let _ = gluster::volume_create_distributed(&volume_name,
                                                       gluster::Transport::Tcp,
                                                       brick_list,
                                                       true)
                .map_err(|e| e.to_string());
            Ok(Status::Created)
        }
        gluster::VolumeType::Stripe => {
            let _ = gluster::volume_create_striped(&volume_name,
                                                   3,
                                                   gluster::Transport::Tcp,
                                                   brick_list,
                                                   true)
                .map_err(|e| e.to_string());
            Ok(Status::Created)
        }
        gluster::VolumeType::Replicate => {
            let _ = gluster::volume_create_replicated(&volume_name,
                                                      replicas,
                                                      gluster::Transport::Tcp,
                                                      brick_list,
                                                      true)
                .map_err(|e| e.to_string());
            Ok(Status::Created)
        }
        gluster::VolumeType::StripedAndReplicate => {
            let _ = gluster::volume_create_striped_replicated(&volume_name,
                                                              3,
                                                              3,
                                                              gluster::Transport::Tcp,
                                                              brick_list,
                                                              true)
                .map_err(|e| e.to_string());
            Ok(Status::Created)
        }
        gluster::VolumeType::Disperse => {
            let _ = gluster::volume_create_erasure(&volume_name,
                                                   3,
                                                   1,
                                                   gluster::Transport::Tcp,
                                                   brick_list,
                                                   true)
                .map_err(|e| e.to_string());
            Ok(Status::Created)
        }
        // gluster::VolumeType::Tier => {},
        gluster::VolumeType::DistributedAndStripe => {
            let _ = gluster::volume_create_striped(&volume_name,
                                                   3,
                                                   gluster::Transport::Tcp,
                                                   brick_list,
                                                   true)
                .map_err(|e| e.to_string());
            Ok(Status::Created)
        }
        gluster::VolumeType::DistributedAndReplicate => {
            let _ = gluster::volume_create_replicated(&volume_name,
                                                      3,
                                                      gluster::Transport::Tcp,
                                                      brick_list,
                                                      true)
                .map_err(|e| e.to_string());
            Ok(Status::Created)
        }
        gluster::VolumeType::DistributedAndStripedAndReplicate => {
            let _ = gluster::volume_create_striped_replicated(&volume_name,
                                                              3,
                                                              3,
                                                              gluster::Transport::Tcp,
                                                              brick_list,
                                                              true)
                .map_err(|e| e.to_string());
            Ok(Status::Created)
        }
        gluster::VolumeType::DistributedAndDisperse => {
            let _ = gluster::volume_create_erasure(
                &volume_name,
                brick_list.len()-1, //TODO: This number has to be lower than the brick length
                1,
                gluster::Transport::Tcp,
                brick_list,
                true).map_err(|e| e.to_string());
            Ok(Status::Created)
        }
    }
}

// Expands the volume by X servers+bricks
// Adds bricks and then runs a rebalance
fn expand_volume(peers: Vec<gluster::Peer>,
                 volume_info: Option<gluster::Volume>)
                 -> Result<i32, String> {
    let volume_name = get_config_value("volume_name")?;

    // Are there new peers?
    log!(format!("Checking for new peers to expand the volume named {}",
                 volume_name));

    // Build the brick list
    let brick_list = match get_brick_list(&peers, volume_info) {
        Ok(list) => list,
        Err(e) => {
            match e {
                Status::WaitForMorePeers => {
                    log!("Waiting for more peers", Info);
                    return Ok(0);
                }
                Status::InvalidConfig(config_err) => {
                    return Err(config_err);
                }
                _ => {
                    // Some other error
                    return Err(format!("Unknown error in expand volume: {:?}", e));
                }
            }
        }
    };

    // Check to make sure the bricks are formatted and mounted
    // let clean_bricks = try!(check_brick_list(&brick_list).map_err(|e| e.to_string()));

    log!(format!("Expanding volume with brick list: {:?}", brick_list),
         Info);
    match gluster::volume_add_brick(&volume_name, brick_list, true) {
        Ok(o) => Ok(o),
        Err(e) => Err(e.to_string()),
    }
}

fn shrink_volume(peer: gluster::Peer, volume_info: Option<gluster::Volume>) -> Result<i32, String> {
    let volume_name = get_config_value("volume_name")?;

    log!(format!("Shrinking volume named  {}", volume_name), Info);

    let peers: Vec<gluster::Peer> = vec![peer];

    // Build the brick list
    let brick_list = match get_brick_list(&peers, volume_info) {
        Ok(list) => list,
        Err(e) => {
            match e {
                Status::WaitForMorePeers => {
                    log!("Waiting for more peers", Info);
                    return Ok(0);
                }
                Status::InvalidConfig(config_err) => {
                    return Err(config_err);
                }
                _ => {
                    // Some other error
                    return Err(format!("Unknown error in shrink volume: {:?}", e));
                }
            }
        }
    };

    log!(format!("Shrinking volume with brick list: {:?}", brick_list),
         Info);
    match gluster::volume_remove_brick(&volume_name, brick_list, true) {
        Ok(o) => Ok(o),
        Err(e) => Err(e.to_string()),
    }
}

fn start_gluster_volume(volume_name: &str) -> Result<(), String> {
    match gluster::volume_start(&volume_name, false) {
        Ok(_) => {
            log!("Starting volume succeeded.".to_string(), Info);
            status_set!(Active "Starting volume succeeded.");
            mount_cluster(&volume_name)?;
            let mut settings: Vec<GlusterOption> = Vec::new();
            // Starting in gluster 3.8 NFS is disabled in favor of ganesha.  I'd like to stick
            // with the legacy version a bit longer.
            settings.push(GlusterOption::NfsDisable(Toggle::Off));
            settings.push(GlusterOption::DiagnosticsLatencyMeasurement(Toggle::On));
            settings.push(GlusterOption::DiagnosticsCountFopHits(Toggle::On));
            settings.push(GlusterOption::DiagnosticsFopSampleInterval(1));
            // Dump FOP stats every other second
            settings.push(GlusterOption::DiagnosticsStatsDumpInterval(2));
            // 1HR DNS timeout
            settings.push(GlusterOption::DiagnosticsStatsDnscacheTtlSec(3600));

            // Set the split brain policy if requested
            if let Ok(splitbrain_policy) = juju::config_get("splitbrain_policy") {
                match SplitBrainPolicy::from_str(&splitbrain_policy) {
                    Ok(policy) => {
                        settings.push(GlusterOption::FavoriteChildPolicy(policy));
                    }
                    Err(_) => {
                        log!(format!("Failed to parse splitbrain_policy config setting: \
                                            {}.",
                                     splitbrain_policy),
                             Error);
                    }
                };
            }
            let _ = gluster::volume_set_options(&volume_name, settings).map_err(|e| e.to_string())?;

            return Ok(());
        }
        Err(e) => {
            log!(format!("Start volume failed with output: {:?}", e), Error);
            status_set!(Blocked "Start volume failed.  Please check juju debug-log.");
            return Err(e.to_string());
        }
    };
}

fn create_gluster_volume(volume_name: &str, peers: Vec<gluster::Peer>) -> Result<(), String> {
    match create_volume(&peers, None) {
        Ok(status) => {
            match status {
                Status::Created => {
                    log!("Create volume succeeded.", Info);
                    status_set!(Maintenance "Create volume succeeded");
                    start_gluster_volume(&volume_name)?;
                    // Poke the other peers to update their status
                    juju::relation_set("started", "true").map_err(|e| e.to_string())?;
                    return Ok(());
                }
                Status::WaitForMorePeers => {
                    log!("Waiting for all peers to enter the Peer in Cluster status");
                    status_set!(Maintenance
                        "Waiting for all peers to enter the \"Peer in Cluster status\"");
                    return Ok(());
                }
                _ => {
                    // Status is failed
                    // What should I return here?
                    return Ok(());
                }
            }
        }
        Err(e) => {
            log!(format!("Create volume failed with output: {}", e), Error);
            status_set!(Blocked "Create volume failed.  Please check juju debug-log.");
            return Err(e.to_string());
        }
    };
}

fn server_changed() -> Result<(), String> {
    let context = juju::Context::new_from_env();
    let leader = juju::is_leader().map_err(|e| e.to_string())?;
    let volume_name = get_config_value("volume_name")?;

    if leader {
        log!(format!("I am the leader: {}", context.relation_id));
        log!("Loading config", Info);

        let mut f = File::open("config.yaml").map_err(|e| e.to_string())?;
        let mut s = String::new();
        f.read_to_string(&mut s).map_err(|e| e.to_string())?;

        status_set!(Maintenance "Checking for new peers to probe");

        let mut peers = gluster::peer_list().map_err(|e| e.to_string())?;
        log!(format!("peer list: {:?}", peers));
        let related_units = juju::relation_list().map_err(|e| e.to_string())?;
        probe_in_units(&peers, related_units)?;
        // Update our peer list
        peers = gluster::peer_list().map_err(|e| e.to_string())?;

        // Everyone is in.  Lets see if a volume exists
        let volume_info = gluster::volume_info(&volume_name);
        let existing_volume: bool;
        match volume_info {
            Ok(_) => {
                log!(format!("Expanding volume {}", volume_name), Info);
                status_set!(Maintenance format!("Expanding volume {}", volume_name));

                match expand_volume(peers, volume_info.ok()) {
                    Ok(v) => {
                        log!(format!("Expand volume succeeded.  Return code: {}", v),
                             Info);
                        status_set!(Active "Expand volume succeeded.");
                        // Poke the other peers to update their status
                        juju::relation_set("expanded", "true").map_err(|e| e.to_string())?;
                        // Ensure the cluster is mounted
                        mount_cluster(&volume_name)?;
                        setup_ctdb()?;
                        setup_samba(&volume_name)?;
                        return Ok(());
                    }
                    Err(e) => {
                        log!(format!("Expand volume failed with output: {}", e), Error);
                        status_set!(Blocked "Expand volume failed.  Please check juju debug-log.");
                        return Err(e);
                    }
                }
            }
            Err(gluster::GlusterError::NoVolumesPresent) => {
                existing_volume = false;
            }
            _ => {
                return Err("Volume info command failed".to_string());
            }
        }
        if !existing_volume {
            log!(format!("Creating volume {}", volume_name), Info);
            status_set!(Maintenance format!("Creating volume {}", volume_name));
            create_gluster_volume(&volume_name, peers)?;
            mount_cluster(&volume_name)?;
            setup_ctdb()?;
            setup_samba(&volume_name)?;
        }
        return Ok(());
    } else {
        // Non leader units
        let vol_started = juju::relation_get("started").map_err(|e| e.to_string())?;
        if !vol_started.is_empty() {
            mount_cluster(&volume_name)?;
            // Setup ctdb and samba after the volume comes up on non leader units
            setup_ctdb()?;
            setup_samba(&volume_name)?;
        }

        return Ok(());
    }
}

fn server_removed() -> Result<(), String> {
    let private_address = juju::unit_get_private_addr().map_err(|e| e.to_string())?;
    log!(format!("Removing server: {}", private_address), Info);
    return Ok(());
}

fn brick_attached() -> Result<(), String> {
    let filesystem_config_value = get_config_value("filesystem_type")?;
    let filesystem_type = block::FilesystemType::from_str(&filesystem_config_value);
    // Format our bricks and mount them
    let brick_location = juju::storage_get_location().map_err(|e| e.to_string())?;
    let brick_path = PathBuf::from(brick_location.trim());
    let mount_path = format!("/mnt/{}", brick_path.file_name().unwrap().to_string_lossy());

    // Format with the default XFS unless told otherwise
    match filesystem_type {
        block::FilesystemType::Xfs => {
            log!(format!("Formatting block device with XFS: {:?}", &brick_path),
                 Info);
            status_set!(Maintenance format!("Formatting block device with XFS: {:?}", &brick_path));

            let filesystem_type = block::Filesystem::Xfs {
                inode_size: None,
                force: true,
            };
            block::format_block_device(&brick_path, &filesystem_type)?;
        }
        block::FilesystemType::Ext4 => {
            log!(format!("Formatting block device with Ext4: {:?}", &brick_path),
                 Info);
            status_set!(Maintenance
                format!("Formatting block device with Ext4: {:?}", &brick_path));

            let filesystem_type = block::Filesystem::Ext4 {
                inode_size: 0,
                reserved_blocks_percentage: 0,
            };
            block::format_block_device(&brick_path, &filesystem_type).map_err(|e| e.to_string())?;
        }
        block::FilesystemType::Btrfs => {
            log!(format!("Formatting block device with Btrfs: {:?}", &brick_path),
                 Info);
            status_set!(Maintenance
                format!("Formatting block device with Btrfs: {:?}", &brick_path));

            let filesystem_type = block::Filesystem::Btrfs {
                leaf_size: 0,
                node_size: 0,
                metadata_profile: block::MetadataProfile::Single,
            };
            block::format_block_device(&brick_path, &filesystem_type).map_err(|e| e.to_string())?;
        }
        block::FilesystemType::Zfs => {
            log!(format!("Formatting block device with ZFS: {:?}", &brick_path),
                 Info);
            status_set!(Maintenance
                format!("Formatting block device with ZFS: {:?}", &brick_path));
            let filesystem_type = block::Filesystem::Zfs {
                compression: None,
                block_size: None,
            };
            block::format_block_device(&brick_path, &filesystem_type).map_err(|e| e.to_string())?;
            // ZFS mounts the filesystem for us
            return Ok(());
        }
        _ => {
            log!(format!("Formatting block device with XFS: {:?}", &brick_path),
                 Info);
            status_set!(Maintenance format!("Formatting block device with XFS: {:?}", &brick_path));

            let filesystem_type = block::Filesystem::Xfs {
                inode_size: None,
                force: true,
            };
            block::format_block_device(&brick_path, &filesystem_type).map_err(|e| e.to_string())?;
        }
    }
    // Update our block device info to reflect formatting
    let device_info = block::get_device_info(&brick_path)?;
    log!(format!("device_info: {:?}", device_info), Info);

    log!(format!("Mounting block device {:?} at {}", &brick_path, mount_path),
         Info);
    status_set!(Maintenance format!("Mounting block device {:?} at {}", &brick_path, mount_path));

    if !Path::new(&mount_path).exists() {
        create_dir(&mount_path).map_err(|e| e.to_string())?;
    }

    block::mount_device(&device_info, &mount_path)?;

    log!(format!("Removing mount path from updatedb {:?}", mount_path),
         Info);
    updatedb::add_to_prunepath(&mount_path, &Path::new("/etc/updatedb.conf"))
        .map_err(|e| e.to_string())?;
    return Ok(());
}

fn brick_detached() -> Result<(), String> {
    // TODO: Do nothing for now
    return Ok(());
}

fn fuse_relation_joined() -> Result<(), String> {
    // Fuse clients only need one ip address and they can discover the rest
    let public_addr = try!(juju::unit_get_public_addr().map_err(|e| e.to_string())).to_string();
    let volumes = gluster::volume_list();
    juju::relation_set("gluster-public-address", &public_addr).map_err(|e| e.to_string())?;
    if let Some(vols) = volumes {
        juju::relation_set("volumes", &vols.join(" ")).map_err(|e| e.to_string())?;
    }

    Ok(())
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

fn nfs_relation_joined() -> Result<(), String> {;
    let config_value = juju::config_get("virtual_ip_addresses").map_err(|e| e.to_string())?;
    let volumes = gluster::volume_list();
    if let Some(vols) = volumes {
        juju::relation_set("volumes", &vols.join(" ")).map_err(|e| e.to_string())?;
    }

    // virtual_ip_addresses isn't set.  Handing back my public address
    if config_value.is_empty() {
        let public_addr = try!(juju::unit_get_public_addr().map_err(|e| e.to_string())).to_string();
        juju::relation_set("gluster-public-address", &public_addr).map_err(|e| e.to_string())?;
    } else {
        // virtual_ip_addresses is set.  Handing back the DNS resolved address
        let dns_name = resolve_first_vip_to_dns()?;
        juju::relation_set("gluster-public-address", &dns_name).map_err(|e| e.to_string())?;
    }
    Ok(())
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
            vec![hook!("brick-storage-attached", brick_attached),
                 hook!("brick-storage-detaching", brick_detached),
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
