extern crate gluster;
extern crate juju;
extern crate rayon;

use std::io::Read;
use std::net::IpAddr;
use std::path::PathBuf;
use std::str::FromStr;

use gluster::{GlusterOption, SplitBrainPolicy, Toggle};
use gluster::peer::{peer_list, Peer};
use gluster::volume::*;
use self::rayon::prelude::*;
use super::super::apt;
use super::super::block;
use super::super::ctdb;
use super::super::samba::setup_samba;
use super::super::{brick_and_server_cartesian_product, device_initialized, ephemeral_unmount,
                   find_new_peers, get_cluster_networks, get_config_value, initialize_storage,
                   mount_cluster, probe_in_units, Status, wait_for_peers};

use std::fs::File;

#[derive(Debug)]
struct Device {
    is_block_device: bool,
    initialized: bool,
    mount_path: String,
    dev_path: PathBuf,
}

pub fn server_changed() -> Result<(), String> {
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

        let mut peers = peer_list().map_err(|e| e.to_string())?;
        log!(format!("peer list: {:?}", peers));
        let related_units = juju::relation_list().map_err(|e| e.to_string())?;
        probe_in_units(&peers, related_units)?;
        // Update our peer list
        peers = peer_list().map_err(|e| e.to_string())?;

        // Everyone is in.  Lets see if a volume exists
        let volume_info = volume_info(&volume_name);
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

fn create_gluster_volume(volume_name: &str, peers: Vec<Peer>) -> Result<(), String> {
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
// Create a new volume if enough peers are available
fn create_volume(peers: &Vec<Peer>, volume_info: Option<Volume>) -> Result<Status, String> {
    let cluster_type_config = get_config_value("cluster_type")?;
    let cluster_type = VolumeType::from_str(&cluster_type_config);
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
        VolumeType::Distribute => {
            let _ = volume_create_distributed(&volume_name, Transport::Tcp, brick_list, true)
                .map_err(|e| e.to_string());
            Ok(Status::Created)
        }
        VolumeType::Stripe => {
            let _ = volume_create_striped(&volume_name, 3, Transport::Tcp, brick_list, true)
                .map_err(|e| e.to_string());
            Ok(Status::Created)
        }
        VolumeType::Replicate => {
            let _ =
                volume_create_replicated(&volume_name, replicas, Transport::Tcp, brick_list, true)
                    .map_err(|e| e.to_string());
            Ok(Status::Created)
        }
        VolumeType::StripedAndReplicate => {
            let _ = volume_create_striped_replicated(&volume_name,
                                                     3,
                                                     3,
                                                     Transport::Tcp,
                                                     brick_list,
                                                     true)
                .map_err(|e| e.to_string());
            Ok(Status::Created)
        }
        VolumeType::Disperse => {
            let _ = volume_create_erasure(&volume_name, 3, 1, Transport::Tcp, brick_list, true)
                .map_err(|e| e.to_string());
            Ok(Status::Created)
        }
        // VolumeType::Tier => {},
        VolumeType::DistributedAndStripe => {
            let _ = volume_create_striped(&volume_name, 3, Transport::Tcp, brick_list, true)
                .map_err(|e| e.to_string());
            Ok(Status::Created)
        }
        VolumeType::DistributedAndReplicate => {
            let _ = volume_create_replicated(&volume_name, 3, Transport::Tcp, brick_list, true)
                .map_err(|e| e.to_string());
            Ok(Status::Created)
        }
        VolumeType::DistributedAndStripedAndReplicate => {
            let _ = volume_create_striped_replicated(&volume_name,
                                                     3,
                                                     3,
                                                     Transport::Tcp,
                                                     brick_list,
                                                     true)
                .map_err(|e| e.to_string());
            Ok(Status::Created)
        }
        VolumeType::DistributedAndDisperse => {
            let _ = volume_create_erasure(
                &volume_name,
                brick_list.len()-1, //TODO: This number has to be lower than the brick length
                1,
                Transport::Tcp,
                brick_list,
                true).map_err(|e| e.to_string());
            Ok(Status::Created)
        }
    }
}
// Expands the volume by X servers+bricks
// Adds bricks and then runs a rebalance
fn expand_volume(peers: Vec<Peer>, volume_info: Option<Volume>) -> Result<i32, String> {
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
    match volume_add_brick(&volume_name, brick_list, true) {
        Ok(o) => Ok(o),
        Err(e) => Err(e.to_string()),
    }
}

// This function will take into account the replication level and
// try its hardest to produce a list of bricks that satisfy this:
// 1. Are not already in the volume
// 2. Sufficient hosts to satisfy replication level
// 3. Stripped across the hosts
// If insufficient hosts exist to satisfy this replication level this will return no new bricks
// to add
fn get_brick_list(peers: &Vec<Peer>,
                  volume: Option<Volume>)
                  -> Result<Vec<gluster::volume::Brick>, Status> {
    // Default to 3 replicas if the parsing fails
    let mut brick_devices: Vec<Device> = Vec::new();

    let replica_config = get_config_value("replication_level").unwrap_or("3".to_string());
    let replicas = replica_config.parse().unwrap_or(3);

    // TODO: Should this fail the hook or just keep going?
    log!("Checking for ephemeral unmount");
    ephemeral_unmount().map_err(|e| Status::InvalidConfig(e))?;

    // Get user configured storage devices
    log!("Gathering list of manually specified brick devices");
    let manual_config_brick_devices: Vec<String> = get_config_value("brick_devices")
        .unwrap_or("".to_string())
        .split(" ")
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
        .collect();
    for brick in manual_config_brick_devices {
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
        let is_block_device = block::is_block_device(&device_path).unwrap_or(false);
        if !is_block_device {
            log!("Skipping invalid block device");
            continue;
        }
        log!(format!("Checking if {:?} is initialized", &device_path));
        let initialized = device_initialized(&device_path).unwrap_or(false);
        let mount_path = format!("/mnt/{}", brick_filename.to_string_lossy());
        brick_devices.push(Device {
            is_block_device: is_block_device,
            // All devices start at initialized is false
            initialized: initialized,
            dev_path: device_path.clone(),
            mount_path: mount_path,
        });
    }
    log!("Gathering list of juju storage brick devices");
    //Get juju storage devices
    for brick in juju::storage_list().unwrap_or("".to_string()).lines() {
        // This is the /dev/ location.
        let storage_location = juju::storage_get(brick.trim()).unwrap();
        // Translate to mount location
        let device_path = PathBuf::from(storage_location.trim());
        let brick_filename = match device_path.file_name() {
            Some(name) => name,
            None => {
                log!(format!("Unable to determine filename for device: {:?}. Skipping",
                             brick),
                     Error);
                continue;
            }
        };
        let mount_path = format!("/mnt/{}", brick_filename.to_string_lossy());
        log!(format!("Checking if {:?} is initialized", &device_path));
        let initialized = device_initialized(&device_path).unwrap_or(false);

        brick_devices.push(Device {
            is_block_device: true,
            initialized: initialized,
            dev_path: device_path.clone(),
            mount_path: mount_path,
        });
    }
    log!(format!("storage devices: {:?}", brick_devices));

    // Format all drives in parallel
    brick_devices.par_iter_mut().weight_max().for_each(|device| match device.initialized {
        false => {
            log!(format!("Calling initialize_storage for {:?}", device.dev_path));
            match initialize_storage(&device.dev_path) {
                Ok(_) => {
                    log!(format!("{:?} is not initialized", &device.dev_path));
                    device.initialized = true;
                }
                Err(e) => {
                    log!(format!("Failed to initialize device: {:?}. Error: {}",
                                 &device.dev_path,
                                 e),
                         Error);
                }
            }
        }
        true => {
            log!(format!("{:?} is already initialized", &device.dev_path));
        }
    });

    let brick_paths: Vec<String> = brick_devices.iter()
        // Only operate on initialized devices
        .filter(|device| device.initialized)
        // Find their mount path
        .map(|device| device.mount_path.clone())
        .collect();
    log!(format!("Usable brick paths: {:?}", brick_paths));

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
// Add all the peers in the gluster cluster to the ctdb cluster
fn setup_ctdb() -> Result<(), String> {
    if juju::config_get("virtual_ip_addresses").map_err(|e| e.to_string())?.is_empty() {
        // virtual_ip_addresses isn't set.  Skip setting ctdb up
        return Ok(());
    }
    log!("setting up ctdb");
    let peers = peer_list().map_err(|e| e.to_string())?;
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

fn shrink_volume(peer: Peer, volume_info: Option<Volume>) -> Result<i32, String> {
    let volume_name = get_config_value("volume_name")?;

    log!(format!("Shrinking volume named  {}", volume_name), Info);

    let peers: Vec<Peer> = vec![peer];

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
    match gluster::volume::volume_remove_brick(&volume_name, brick_list, true) {
        Ok(o) => Ok(o),
        Err(e) => Err(e.to_string()),
    }
}
fn start_gluster_volume(volume_name: &str) -> Result<(), String> {
    match gluster::volume::volume_start(&volume_name, false) {
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
            settings.push(GlusterOption::DiagnosticsFopSampleInterval(5));
            // Dump FOP stats every 5 seconds.
            // NOTE: On slow main drives this can severely impact them
            settings.push(GlusterOption::DiagnosticsStatsDumpInterval(30));
            // 1HR DNS timeout
            settings.push(GlusterOption::DiagnosticsStatsDnscacheTtlSec(3600));

            // Set parallel-readdir on.  This has a very nice performance benefit
            // as the number of bricks/directories grows
            settings.push(GlusterOption::PerformanceParallelReadDir(Toggle::On));

            settings.push(GlusterOption::PerformanceReadDirAhead(Toggle::On));
            // Start with 20MB and go from there
            settings.push(GlusterOption::PerformanceReadDirAheadCacheLimit(1024 * 1024 * 20));

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
            let _ = volume_set_options(&volume_name, settings).map_err(|e| e.to_string())?;

            return Ok(());
        }
        Err(e) => {
            log!(format!("Start volume failed with output: {:?}", e), Error);
            status_set!(Blocked "Start volume failed.  Please check juju debug-log.");
            return Err(e.to_string());
        }
    };
}