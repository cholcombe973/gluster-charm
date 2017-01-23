mod actions;
mod apt;
mod block;
mod encryption;
mod upgrade;

extern crate debian;
extern crate gluster;
extern crate itertools;
#[macro_use]
extern crate juju;
extern crate log;
extern crate uuid;

use actions::{disable_volume_quota, enable_volume_quota, list_volume_quotas, set_volume_options};

use std::collections::HashMap;
use std::env;
use std::fs;
use std::fs::{create_dir, File};
use std::io::prelude::Read;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use debian::version::Version;
use itertools::Itertools;
use log::LogLevel;


#[cfg(test)]
mod tests {
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
                   vec![
                       gluster::Brick{
                            peer: peer1.clone(),
                            path: PathBuf::from("/mnt/brick1"),
                        },
                        gluster::Brick{
                            peer: peer2.clone(),
                            path: PathBuf::from("/mnt/brick1"),
                        },
                        gluster::Brick{
                            peer: peer1.clone(),
                            path: PathBuf::from("/mnt/brick2"),
                        },
                        gluster::Brick{
                            peer: peer2.clone(),
                            path: PathBuf::from("/mnt/brick2"),
                        },
                    ]);
    }
}

// Need more expressive return values so we can wait on peers
#[derive(Debug)]
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

fn config_changed() -> Result<(), String> {
    check_for_upgrade()?;
    return Ok(());
}

fn leader_settings_changed() -> Result<(), String> {
    // Setup encryption if requested
    if let Some(_) = juju::config_get("encryption").ok() {
        let volume_name = get_config_value("volume_name")?;
        setup_encryption(&volume_name)?;
    }
    Ok(())
}

// If the config has changed this will initiated a rolling upgrade
fn check_for_upgrade() -> Result<(), String> {
    let config = juju::Config::new().map_err(|e| e.to_string())?;
    if !config.changed("source").map_err(|e| e.to_string())? {
        // No upgrade requested
        juju::log(&"No upgrade requested", Some(LogLevel::Debug));
        return Ok(());
    }

    juju::log(&"Getting current_version", Some(LogLevel::Debug));
    let current_version = get_glusterfs_version()?;

    juju::log(&"Adding new source line", Some(LogLevel::Debug));
    let source = juju::config_get("source").map_err(|e| e.to_string())?;
    apt::add_source(&source)?;
    juju::log(&"Calling apt update", Some(LogLevel::Debug));
    apt::apt_update()?;

    juju::log(&"Getting proposed_version", Some(LogLevel::Debug));
    let proposed_version = apt::get_candidate_package_version("glusterfs-server")?;

    // Using semantic versioning if the new version is greater than we allow the upgrade
    if proposed_version > current_version {
        juju::log(&format!("current_version: {}", current_version),
                  Some(LogLevel::Debug));
        juju::log(&format!("new_version: {}", proposed_version),
                  Some(LogLevel::Debug));
        juju::log(&format!("{} to {} is a valid upgrade path.  Proceeding.",
                           current_version,
                           proposed_version),
                  Some(LogLevel::Debug));
        return upgrade::roll_cluster(&proposed_version);
    } else {
        // Log a helpful error message
        juju::log(&format!("Invalid upgrade path from {} to {}. The new version needs to be \
                            greater than the old version",
                           current_version,
                           proposed_version),
                  Some(LogLevel::Error));
        return Ok(());
    }
}

fn peers_are_ready(peers: Result<Vec<gluster::Peer>, gluster::GlusterError>) -> bool {
    if peers.is_err() {
        return false;
    }

    juju::log(&format!("Got peer status: {:?}", peers),
              Some(LogLevel::Debug));
    let result = match peers {
        Ok(result) => result,
        Err(err) => {
            juju::log(&format!("peers_are_ready failed to get peer status: {:?}", err),
                      Some(LogLevel::Error));
            return false;
        }
    };

    // Ensure all peers are in a PeerInCluster state
    result.iter().all(|peer| peer.status == gluster::State::PeerInCluster)
}

// HDD's are so slow that sometimes the peers take long to join the cluster.
// This will loop and wait for them ie spinlock
fn wait_for_peers() -> Result<(), String> {
    juju::log(&"Waiting for all peers to enter the Peer in Cluster status".to_string(),
              Some(LogLevel::Debug));
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

    juju::log(&format!("Adding in related_units: {:?}", related_units),
              Some(LogLevel::Debug));
    for unit in related_units {
        let address = juju::relation_get_by_unit(&"private-address".to_string(), &unit)
            .map_err(|e| e.to_string())?;
        let address_trimmed = address.trim().to_string();

        let mut already_probed: bool = false;

        // I think the localhost test is failing
        for peer in existing_peers {
            if peer.hostname == address_trimmed {
                already_probed = true;
            }
        }

        // Probe the peer in
        if !already_probed {
            juju::log(&format!("Adding {} to cluster", &address_trimmed),
                      Some(LogLevel::Debug));
            match gluster::peer_probe(&address_trimmed) {
                Ok(_) => {
                    juju::log(&"Gluster peer probe was successful".to_string(),
                              Some(LogLevel::Debug))
                }
                Err(why) => {
                    juju::log(&format!("Gluster peer probe failed: {:?}", why),
                              Some(LogLevel::Error));
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
        let mut new_peer: bool = true;

        for brick in volume_info.bricks.iter() {
            if brick.peer.uuid == peer.uuid {
                new_peer = false;
                break;
            }
        }
        if new_peer {
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
    juju::log(&format!("storage_list: {:?}", bricks),
              Some(LogLevel::Debug));

    for brick in bricks.lines() {
        // This is the /dev/ location.
        let storage_location = juju::storage_get(brick.trim()).unwrap();
        // Translate to mount location
        let brick_path = PathBuf::from(storage_location.trim());
        let mount_path = format!("/mnt/{}", brick_path.file_name().unwrap().to_string_lossy());

        brick_paths.push(mount_path);
    }

    if volume.is_none() {
        juju::log(&"Volume is none".to_string(), Some(LogLevel::Debug));
        // number of bricks % replicas == 0 then we're ok to proceed
        if peers.len() < replicas {
            // Not enough peers to replicate across
            juju::log(&"Not enough peers to satisfy the replication level for the Gluster \
                        volume.  Waiting for more peers to join."
                          .to_string(),
                      Some(LogLevel::Debug));
            return Err(Status::WaitForMorePeers);
        } else if peers.len() == replicas {
            // Case 1: A perfect marriage of peers and number of replicas
            juju::log(&"Number of peers and number of replicas match".to_string(),
                      Some(LogLevel::Debug));
            return Ok(brick_and_server_cartesian_product(peers, &brick_paths));
        } else {
            // Case 2: We have a mismatch of replicas and hosts
            // Take as many as we can and leave the rest for a later time
            let count = peers.len() - (peers.len() % replicas);
            let mut new_peers = peers.clone();

            // Drop these peers off the end of the list
            new_peers.truncate(count);
            juju::log(&format!("Too many new peers.  Dropping {} peers off the list", count),
                      Some(LogLevel::Debug));
            return Ok(brick_and_server_cartesian_product(&new_peers, &brick_paths));
        }
    } else {
        // Existing volume.  Build a differential list.
        juju::log(&"Existing volume.  Building differential brick list".to_string(),
                  Some(LogLevel::Debug));
        let mut new_peers = find_new_peers(peers, &volume.unwrap());

        if new_peers.len() < replicas {
            juju::log(&"New peers found are less than needed by the replica count".to_string(),
                      Some(LogLevel::Debug));
            return Err(Status::WaitForMorePeers);
        } else if new_peers.len() == replicas {
            juju::log(&"New peers and number of replicas match".to_string(),
                      Some(LogLevel::Debug));
            return Ok(brick_and_server_cartesian_product(&new_peers, &brick_paths));
        } else {
            let count = new_peers.len() - (new_peers.len() % replicas);
            // Drop these peers off the end of the list
            juju::log(&format!("Too many new peers.  Dropping {} peers off the list", count),
                      Some(LogLevel::Debug));
            new_peers.truncate(count);
            return Ok(brick_and_server_cartesian_product(&new_peers, &brick_paths));
        }
    }
}

fn check_and_create_dir(path: &str) -> Result<(), String> {
    match fs::metadata(path) {
        Ok(_) => return Ok(()),
        Err(e) => {
            match e.kind() {
                std::io::ErrorKind::NotFound => {
                    juju::log(&format!("Creating dir {}", path), Some(LogLevel::Info));
                    status_set!(Maintenance format!("Creating dir {}", path));
                    fs::create_dir(&path).map_err(|e| e.to_string())?;
                    return Ok(());
                }
                _ => {
                    return Err(format!("Error searching for directory {:?} {:?}", &path, e.kind()));
                }
            }
        }
    }
}

// Create a new volume if enough peers are available
fn create_volume(peers: &Vec<gluster::Peer>,
                 volume_info: Option<gluster::Volume>)
                 -> Result<i32, String> {
    let cluster_type_config = get_config_value("cluster_type")?;
    let cluster_type = gluster::VolumeType::from_str(&cluster_type_config);
    let volume_name = get_config_value("volume_name")?;
    let replicas = match get_config_value("replication_level")?.parse() {
        Ok(r) => r,
        Err(e) => {
            juju::log(&format!("Invalid config value for replicas.  Defaulting to 3. Error was \
                                {}",
                               e),
                      Some(LogLevel::Error));
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
                    juju::log(&"Waiting for more peers".to_string(), Some(LogLevel::Debug));
                    status_set!(Maintenance "Waiting for more peers");
                    return Ok(0);
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
    juju::log(&format!("Got brick list: {:?}", brick_list),
              Some(LogLevel::Debug));

    // Check to make sure the bricks are formatted and mounted
    // let clean_bricks = try!(check_brick_list(&brick_list).map_err(|e| e.to_string()));

    juju::log(&format!("Creating volume of type {:?} with brick list {:?}",
                       cluster_type,
                       brick_list),
              Some(LogLevel::Info));

    match cluster_type {
        gluster::VolumeType::Distribute => {
            gluster::volume_create_distributed(&volume_name,
                                               gluster::Transport::Tcp,
                                               brick_list,
                                               true)
                .map_err(|e| e.to_string())
        }
        gluster::VolumeType::Stripe => {
            gluster::volume_create_striped(&volume_name,
                                           3,
                                           gluster::Transport::Tcp,
                                           brick_list,
                                           true)
                .map_err(|e| e.to_string())
        }
        gluster::VolumeType::Replicate => {
            gluster::volume_create_replicated(&volume_name,
                                              replicas,
                                              gluster::Transport::Tcp,
                                              brick_list,
                                              true)
                .map_err(|e| e.to_string())
        }
        gluster::VolumeType::StripedAndReplicate => {
            gluster::volume_create_striped_replicated(&volume_name,
                                                      3,
                                                      3,
                                                      gluster::Transport::Tcp,
                                                      brick_list,
                                                      true)
                .map_err(|e| e.to_string())
        }
        gluster::VolumeType::Disperse => {
            gluster::volume_create_erasure(&volume_name,
                                           3,
                                           1,
                                           gluster::Transport::Tcp,
                                           brick_list,
                                           true)
                .map_err(|e| e.to_string())
        }
        // gluster::VolumeType::Tier => {},
        gluster::VolumeType::DistributedAndStripe => {
            gluster::volume_create_striped(&volume_name,
                                           3,
                                           gluster::Transport::Tcp,
                                           brick_list,
                                           true)
                .map_err(|e| e.to_string())
        }
        gluster::VolumeType::DistributedAndReplicate => {
            gluster::volume_create_replicated(&volume_name,
                                              3,
                                              gluster::Transport::Tcp,
                                              brick_list,
                                              true)
                .map_err(|e| e.to_string())
        }
        gluster::VolumeType::DistributedAndStripedAndReplicate => {
            gluster::volume_create_striped_replicated(&volume_name,
                                                      3,
                                                      3,
                                                      gluster::Transport::Tcp,
                                                      brick_list,
                                                      true)
                .map_err(|e| e.to_string())
        }
        gluster::VolumeType::DistributedAndDisperse =>
            gluster::volume_create_erasure(
                &volume_name,
                brick_list.len()-1, //TODO: This number has to be lower than the brick length
                1,
                gluster::Transport::Tcp,
                brick_list,
                true).map_err(|e| e.to_string()),
    }
}

// Expands the volume by X servers+bricks
// Adds bricks and then runs a rebalance
fn expand_volume(peers: Vec<gluster::Peer>,
                 volume_info: Option<gluster::Volume>)
                 -> Result<i32, String> {
    let volume_name = get_config_value("volume_name")?;

    // Are there new peers?
    juju::log(&format!("Checking for new peers to expand the volume named {}",
                       volume_name),
              Some(LogLevel::Debug));

    // Build the brick list
    let brick_list = match get_brick_list(&peers, volume_info) {
        Ok(list) => list,
        Err(e) => {
            match e {
                Status::WaitForMorePeers => {
                    juju::log(&"Waiting for more peers".to_string(), Some(LogLevel::Debug));
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

    juju::log(&format!("Expanding volume with brick list: {:?}", brick_list),
              Some(LogLevel::Info));
    match gluster::volume_add_brick(&volume_name, brick_list, true) {
        Ok(o) => Ok(o),
        Err(e) => Err(e.to_string()),
    }
}

fn shrink_volume(peer: gluster::Peer, volume_info: Option<gluster::Volume>) -> Result<i32, String> {
    let volume_name = get_config_value("volume_name")?;

    juju::log(&format!("Shrinking volume named  {}", volume_name),
              Some(LogLevel::Info));

    let peers: Vec<gluster::Peer> = vec![peer];

    // Build the brick list
    let brick_list = match get_brick_list(&peers, volume_info) {
        Ok(list) => list,
        Err(e) => {
            match e {
                Status::WaitForMorePeers => {
                    juju::log(&"Waiting for more peers".to_string(), Some(LogLevel::Debug));
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

    juju::log(&format!("Shrinking volume with brick list: {:?}", brick_list),
              Some(LogLevel::Info));
    match gluster::volume_remove_brick(&volume_name, brick_list, true) {
        Ok(o) => Ok(o),
        Err(e) => Err(e.to_string()),
    }
}

fn setup_encryption(volume: &str) -> Result<(), String> {
    let leader = juju::is_leader().map_err(|e| e.to_string())?;
    if leader {
        // The leader creates the public and private keys
        status_set!(Maintenance "Generating Encryption Keys");
        juju::log(&"Generating encryption keys".to_string(),
                  Some(LogLevel::Info));
        let keypair = encryption::generate_keypair(4096).unwrap();
        encryption::save_keys(&keypair.0, &keypair.1).map_err(|e| e.to_string())?;
        encryption::enable_io_encryption(&volume).map_err(|e| e.to_string())?;

        let mut ssl_keys = HashMap::new();
        ssl_keys.insert("public_key".to_string(),
                        String::from_utf8_lossy(&keypair.0).into_owned());
        ssl_keys.insert("private_key".to_string(),
                        String::from_utf8_lossy(&keypair.1).into_owned());

        juju::leader_set(ssl_keys).map_err(|e| e.to_string())?;

        // Enable encryption
        encryption::enable_io_encryption(&volume).map_err(|e| e.to_string())?;
    } else {
        // Everyone else gets those keys from the leader
        let public_key =
            juju::leader_get(Some("public_key".to_string())).map_err(|e| e.to_string())?;
        let private_key =
            juju::leader_get(Some("private_key".to_string())).map_err(|e| e.to_string())?;
        if public_key.is_empty() || private_key.is_empty() {
            juju::log("Public or Private SSL key has not be set by the leader yet",
                      Some(LogLevel::Debug));
            return Ok(());
        }

        encryption::save_keys(public_key.as_bytes(), private_key.as_bytes())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

fn server_changed() -> Result<(), String> {
    let context = juju::Context::new_from_env();
    let leader = juju::is_leader().map_err(|e| e.to_string())?;
    let volume_name = get_config_value("volume_name")?;

    if leader {
        juju::log(&format!("I am the leader: {}", context.relation_id),
                  Some(LogLevel::Debug));
        juju::log(&"Loading config".to_string(), Some(LogLevel::Info));

        let mut f = File::open("config.yaml").map_err(|e| e.to_string())?;
        let mut s = String::new();
        f.read_to_string(&mut s).map_err(|e| e.to_string())?;

        status_set!(Maintenance "Checking for new peers to probe");

        let mut peers = gluster::peer_list().map_err(|e| e.to_string())?;
        juju::log(&format!("peer list: {:?}", peers), Some(LogLevel::Debug));
        let related_units = juju::relation_list().map_err(|e| e.to_string())?;
        probe_in_units(&peers, related_units)?;
        // Update our peer list
        peers = gluster::peer_list().map_err(|e| e.to_string())?;

        // Everyone is in.  Lets see if a volume exists
        let volume_info = gluster::volume_info(&volume_name);
        let existing_volume: bool;
        match volume_info {
            Ok(_) => {
                juju::log(&format!("Expanding volume {}", volume_name),
                          Some(LogLevel::Info));
                status_set!(Maintenance format!("Expanding volume {}", volume_name));

                match expand_volume(peers, volume_info.ok()) {
                    Ok(v) => {
                        juju::log(&format!("Expand volume succeeded.  Return code: {}", v),
                                  Some(LogLevel::Info));
                        status_set!(Active "Expand volume succeeded.");
                        // Ensure the cluster is mounted
                        mount_cluster(&volume_name)?;
                        // Enable encryption if requested
                        if let Some(_) = juju::config_get("encryption").ok() {
                            setup_encryption(&volume_name)?;
                        }
                        return Ok(());
                    }
                    Err(e) => {
                        juju::log(&format!("Expand volume failed with output: {}", e),
                                  Some(LogLevel::Error));
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
            juju::log(&format!("Creating volume {}", volume_name),
                      Some(LogLevel::Info));
            status_set!(Maintenance format!("Creating volume {}", volume_name));
            match create_volume(&peers, None) {
                Ok(_) => {
                    juju::log(&"Create volume succeeded.".to_string(),
                              Some(LogLevel::Info));
                    status_set!(Maintenance "Create volume succeeded");
                }
                Err(e) => {
                    juju::log(&format!("Create volume failed with output: {}", e),
                              Some(LogLevel::Error));
                    status_set!(Blocked "Create volume failed.  Please check juju debug-log.");
                    return Err(e.to_string());
                }
            }
            match gluster::volume_start(&volume_name, false) {
                Ok(_) => {
                    juju::log(&"Starting volume succeeded.".to_string(),
                              Some(LogLevel::Info));
                    status_set!(Active "Starting volume succeeded.");
                    mount_cluster(&volume_name)?;
                    // Enable encryption if requested
                    if let Some(_) = juju::config_get("encryption").ok() {
                        setup_encryption(&volume_name)?;
                    }
                }
                Err(e) => {
                    juju::log(&format!("Start volume failed with output: {:?}", e),
                              Some(LogLevel::Error));
                    status_set!(Blocked "Start volume failed.  Please check juju debug-log.");
                    return Err(e.to_string());
                }
            };
        }
        return Ok(());
    } else {
        status_set!(Active "");
        return Ok(());
    }
}

fn server_removed() -> Result<(), String> {
    let private_address = juju::unit_get_private_addr().map_err(|e| e.to_string())?;
    juju::log(&format!("Removing server: {}", private_address),
              Some(LogLevel::Info));
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
            juju::log(&format!("Formatting block device with XFS: {:?}", &brick_path),
                      Some(LogLevel::Info));
            status_set!(Maintenance format!("Formatting block device with XFS: {:?}", &brick_path));

            let filesystem_type = block::Filesystem::Xfs {
                inode_size: None,
                force: true,
            };
            block::format_block_device(&brick_path, &filesystem_type)?;
        }
        block::FilesystemType::Ext4 => {
            juju::log(&format!("Formatting block device with Ext4: {:?}", &brick_path),
                      Some(LogLevel::Info));
            status_set!(Maintenance
                format!("Formatting block device with Ext4: {:?}", &brick_path));

            let filesystem_type = block::Filesystem::Ext4 {
                inode_size: 0,
                reserved_blocks_percentage: 0,
            };
            block::format_block_device(&brick_path, &filesystem_type).map_err(|e| e.to_string())?;
        }
        block::FilesystemType::Btrfs => {
            juju::log(&format!("Formatting block device with Btrfs: {:?}", &brick_path),
                      Some(LogLevel::Info));
            status_set!(Maintenance
                format!("Formatting block device with Btrfs: {:?}", &brick_path));

            let filesystem_type = block::Filesystem::Btrfs {
                leaf_size: 0,
                node_size: 0,
                metadata_profile: block::MetadataProfile::Single,
            };
            block::format_block_device(&brick_path, &filesystem_type).map_err(|e| e.to_string())?;
        }
        _ => {
            juju::log(&format!("Formatting block device with XFS: {:?}", &brick_path),
                      Some(LogLevel::Info));
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
    juju::log(&format!("device_info: {:?}", device_info),
              Some(LogLevel::Info));

    juju::log(&format!("Mounting block device {:?} at {}", &brick_path, mount_path),
              Some(LogLevel::Info));
    status_set!(Maintenance format!("Mounting block device {:?} at {}", &brick_path, mount_path));

    check_and_create_dir(&mount_path)?;

    block::mount_device(&device_info, &mount_path)?;
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

fn nfs_relation_joined() -> Result<(), String> {
    let public_addr = try!(juju::unit_get_public_addr().map_err(|e| e.to_string())).to_string();
    let volumes = gluster::volume_list();
    juju::relation_set("gluster-public-address", &public_addr).map_err(|e| e.to_string())?;
    if let Some(vols) = volumes {
        juju::relation_set("volumes", &vols.join(" ")).map_err(|e| e.to_string())?;
    }
    return Ok(());
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

    // Ensure the cluster is mounted
    mount_cluster(&volume_name)?;

    let volume_info = gluster::volume_info(&volume_name);
    match volume_info {
        Ok(vol) => {
            status_set!(Active format!("Unit is ready ({} bricks)", vol.bricks.len()));
            Ok(())
        }
        Err(gluster::GlusterError::NoVolumesPresent) => {
            status_set!(Blocked "No volume found");
            Ok(())
        }
        _ => Ok(()),
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() > 0 {
        // Register our hooks with the Juju library
        let hook_registry: Vec<juju::Hook> = vec![
            hook!("brick-storage-attached", brick_attached),
            hook!("brick-storage-detaching", brick_detached),
            hook!("config-changed", config_changed),
            hook!("create-volume-quota", enable_volume_quota),
            hook!("delete-volume-quota", disable_volume_quota),
            hook!("fuse-relation-joined", fuse_relation_joined),
            hook!("leader-settings-changed", leader_settings_changed),
            hook!("list-volume-quotas", list_volume_quotas),
            hook!("nfs-relation-joined", nfs_relation_joined),
            hook!("server-relation-changed", server_changed),
            hook!("server-relation-departed", server_removed),
            hook!("set-volume-options", set_volume_options),
            hook!("update-status", update_status),
        ];

        let result = juju::process_hooks(hook_registry);

        if result.is_err() {
            juju::log(&format!("Hook failed with error: {:?}", result.err()),
                      Some(LogLevel::Error));
        }
    }
}
