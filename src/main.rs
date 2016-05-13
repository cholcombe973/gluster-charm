mod block;

extern crate gluster;
extern crate itertools;
extern crate juju;

use itertools::Itertools;
use std::env;
use std::fs;
use std::fs::File;
use std::io::prelude::Read;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

// A gluster server has either joined or left this cluster
//

// #[derive(Debug)]
// struct Config{
// volume_name: String,
// brick_paths: Vec<String>,
// cluster_type: gluster::VolumeType,
// replicas: usize,
// filesystem_type: block::FilesystemType,
// }
//

#[cfg(test)]
mod tests {
    extern crate uuid;
    use std::fs::File;
    use std::io::prelude::Read;
    use self::uuid::Uuid;

    // #[test]
    // fn generate_test_peers(amount: usize)->Vec<gluster::Peer>{
    // let mut peers: Vec<gluster::Peer> = Vec::with_capacity(amount);
    // let mut count = 0;
    // loop{
    // let p = gluster::Peer {
    // uuid: Uuid::new_v4(),
    // hostname: format!("host-{}",Uuid::new_v4()),
    // status: gluster::State::Connected,
    // };
    // peers.push(p);
    // count+=1;
    // if count == amount{
    // break;
    // }
    // }
    // return peers;
    // }
    //
    // #[test]
    // fn generate_test_bricks(peers: &Vec<gluster::Peer>)->Vec<gluster::Brick>{
    // let mut bricks: Vec<gluster::Brick> = Vec::with_capacity(peers.len());
    // let mut count = 0;
    // for peer in peers{
    // let b = gluster::Brick{
    // peer: peer.clone(),
    // path: PathBuf::from(&format!("/mnt/{}",count)),
    // };
    // bricks.push(b);
    // count+=1;
    // }
    // return bricks;
    // }
    //

    #[test]
    fn test_block_device_usage() {}

    // #[test]
    // fn test_brick_generation(){
    // let mut test_peers = generate_test_peers(3);
    // let data: Value = json::from_str("[\"/mnt/sda\", \"/mnt/sdb\"]").unwrap();
    // let brick_path_array = data.as_array().unwrap();
    //
    // let c = Config{
    // volume_name: "test".to_string(),
    // brick_paths: brick_path_array.clone(),
    // cluster_type: gluster::VolumeType::Replicate,
    // replicas: 3,
    // };
    //
    // Case 1: New volume and perfectly matched peer number to replica number
    // let b1 = get_brick_list(&c, &test_peers, None).unwrap();
    // println!("get_brick_list 1: {:?}", b1);
    // assert!(b1.len() == 6);
    //
    // Case 2: New volume and we're short 1 Peer
    //
    // Drop a peer off the end
    // test_peers.pop();
    // let b2 = get_brick_list(&c, &test_peers, None);
    // println!("get_brick_list 2: {:?}", b2);
    // assert!(b2.is_none());
    //
    // Now add a peer and try again
    // test_peers.push(gluster::Peer{
    // uuid: Uuid::new_v4(),
    // hostname: "host-x".to_string(),
    // status: gluster::State::Connected,
    // });
    // let b3 = get_brick_list(&c, &test_peers, None);
    // println!("get_brick_list 3: {:?}", b3);
    // assert!(b1.len() == 6);
    //
    //
    // Case 3: Existing volume with 2 peers and we're adding 2 Peers
    // let test_peers2 = generate_test_peers(2);
    // let v = gluster::Volume {
    // name: "test".to_string(),
    // vol_type: gluster::VolumeType::Replicate,
    // id: Uuid::new_v4(),
    // status: "normal".to_string(),
    // transport: gluster::Transport::Tcp,
    // bricks: generate_test_bricks(&test_peers),
    // };
    // let b4 = get_brick_list(&c, &test_peers2, Some(v));
    // println!("get_brick_list 4: {:?}", b4);
    // assert!(b4.is_none());
    //
    //
    // Case 4: Mismatch of new volume and too many peers
    // let test_peers3 = generate_test_peers(4);
    // let b5 = get_brick_list(&c, &test_peers3, None).unwrap();
    // println!("get_brick_list 5: {:?}", b5);
    // assert!(b5.len() == 6);
    // }
    //
}

//Need more expressive return values so we can wait on peers
#[derive(Debug)]
enum Status{
    Created,
    WaitForMorePeers,
    InvalidConfig(String),
    FailedToCreate(String),
    FailedToStart(String),
}

fn get_config_value(name: &str) -> Result<String, String> {
    match juju::config_get(&name.to_string()){
        Ok(v) => Ok(v),
        Err(e) => {
            return Err(e.to_string());
        }
    }
}

fn config_changed() -> Result<(), String> {
    // load the config again
    // let new_config = parse_config(&s);

    // how do we figure out what changed?
    return Ok(());
}

fn peers_are_ready(peers: Result<Vec<gluster::Peer>, gluster::GlusterError>) -> bool {
    if peers.is_err() {
        return false;
    }

    juju::log(&format!("Got peer status: {:?}", peers));
    let result = match peers {
        Ok(result) => result,
        Err(err) => {
            juju::log(&format!("peers_are_ready failed to get peer status: {:?}", err));
            return false;
        }
    };
    for peer in result {
        if peer.status != gluster::State::PeerInCluster {
            return false;
        }
    }
    return true;
}


// HDD's are so slow that sometimes the peers take long to join the cluster.
// This will loop and wait for them ie spinlock
fn wait_for_peers() -> Result<(), String> {
    juju::log(&"Waiting for all peers to enter the Peer in Cluster status".to_string());
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
// 1. If you probe in units by their IP address it works.  The CLI will show you their resolved hostnames however
// 2. If you probe in units by their hostname instead it'll still work but gluster client mount
// commands will fail if it can not resolve the hostname.
// For example: Probing in containers by hostname will cause the glusterfs client to fail to mount
// on the container host.  :(
// 3. To get around this I'm converting hostnames to ip addresses in the gluster library to mask this from
// the callers.
//
fn probe_in_units(existing_peers: &Vec<gluster::Peer>,
                  related_units: Vec<juju::Relation>)
                  -> Result<(), String> {

    juju::log(&format!("Adding in related_units: {:?}", related_units));
    for unit in related_units {
        let address = try!(juju::relation_get_by_unit(&"private-address".to_string(), &unit)
                               .map_err(|e| e.to_string()));
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
            juju::log(&format!("Adding {} to cluster", &address_trimmed));
            match gluster::peer_probe(&address_trimmed) {
                Ok(_) => juju::log(&"Gluster peer probe was successful".to_string()),
                Err(why) => {
                    juju::log(&format!("Gluster peer probe failed: {:?}", why));
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
// If insufficient hosts exist to satisfy this replication level this will return no new bricks to add
fn get_brick_list(peers: &Vec<gluster::Peer>,
                  volume: Option<gluster::Volume>)
                  -> Result<Vec<gluster::Brick>, Status> {

    // Default to 3 replicas if the parsing fails
    let replica_config = get_config_value("replication_level").unwrap_or("3".to_string());
    let replicas = replica_config.parse().unwrap_or(3);

    let brick_path_config = match get_config_value("brick_paths") {
        Ok(b) => b,
        Err(e) => {
            return Err(Status::InvalidConfig(format!("Unable to get brick_paths config value. {}", e)));
        }
    };

    let brick_paths: Vec<String> = brick_path_config.split(" ")
                                                        .map(|s| s.to_string())
                                                        .collect();

    if volume.is_none() {
        juju::log(&"Volume is none".to_string());
        // number of bricks % replicas == 0 then we're ok to proceed
        if peers.len() < replicas {
            // Not enough peers to replicate across
            juju::log(&"Not enough peers to satisfy the replication level for the Gluster \
                        volume.  Waiting for more peers to join."
                           .to_string());
            return Err(Status::WaitForMorePeers);
        } else if peers.len() == replicas {
            // Case 1: A perfect marriage of peers and number of replicas
            juju::log(&"Number of peers and number of replicas match".to_string());
            return Ok(brick_and_server_cartesian_product(peers, &brick_paths));
        } else {
            // Case 2: We have a mismatch of replicas and hosts
            // Take as many as we can and leave the rest for a later time
            let count = peers.len() - (peers.len() % replicas);
            let mut new_peers = peers.clone();

            // Drop these peers off the end of the list
            new_peers.truncate(count);
            juju::log(&format!("Too many new peers.  Dropping {} peers off the list", count));
            return Ok(brick_and_server_cartesian_product(&new_peers, &brick_paths));
        }
    } else {
        // Existing volume.  Build a differential list.
        juju::log(&"Existing volume.  Building differential brick list".to_string());
        let mut new_peers = find_new_peers(peers, &volume.unwrap());

        if new_peers.len() < replicas {
            juju::log(&"New peers found are less than needed by the replica count".to_string());
            return Err(Status::WaitForMorePeers);
        } else if new_peers.len() == replicas {
            juju::log(&"New peers and number of replicas match".to_string());
            return Ok(brick_and_server_cartesian_product(&new_peers, &brick_paths));
        } else {
            let count = new_peers.len() - (new_peers.len() % replicas);
            // Drop these peers off the end of the list
            juju::log(&format!("Too many new peers.  Dropping {} peers off the list", count));
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
                    juju::log(&format!("Creating dir {}", path));
                    try!(fs::create_dir(&path).map_err(|e| e.to_string()));
                    return Ok(());
                }
                _ => {
                    return Err(format!("Error searching for directory {:?} {:?}", &path, e.kind()));
                }
            }
        }
    }
}

// TODO: This blindly formats block devices and ignores what's on them.
fn check_brick_list(bricks: &Vec<gluster::Brick>) -> Result<Vec<gluster::Brick>, String> {
    let filesystem_config_value = try!(get_config_value("filesystem_type"));
    let filesystem_type = block::FilesystemType::from_str(&filesystem_config_value);
    let mut clean_bricks: Vec<gluster::Brick> = Vec::new();
    for brick in bricks {
        let block_check = block::is_block_device(&brick.path);

        if block_check.is_ok() {

            let dev_name = try!(brick.path
                                     .file_name()
                                     .ok_or(format!("Failed to get device name from {:?}",
                                                    &brick.path)));
            let dev_name_str = try!(dev_name.to_str()
                                            .ok_or(format!("Failed to transform device name to \
                                                            string {:?}",
                                                           &dev_name)));

            let mount_path = format!("/mnt/{}", dev_name_str);

            juju::log(&format!("Gathering info on block device: {:?}", &brick.path));

            // Format with the default XFS unless told otherwise
            match filesystem_type {
                block::FilesystemType::Xfs => {
                    juju::log(&format!("Formatting block device with XFS: {:?}", &brick.path));
                    try!(juju::status_set(juju::Status{
                        status_type: juju::StatusType::Maintenance,
                        message: format!("Formatting block device with XFS: {:?}", &brick.path),
                    }).map_err(|e| e.to_string()));

                    let filesystem_type = block::Filesystem::Xfs {
                        inode_size: None,
                        force: true,
                    };
                    try!(block::format_block_device(&brick.path, &filesystem_type)
                             .map_err(|e| e.to_string()));
                }
                block::FilesystemType::Ext4 => {
                    juju::log(&format!("Formatting block device with Ext4: {:?}", &brick.path));
                    try!(juju::status_set(juju::Status {
                             status_type: juju::StatusType::Maintenance,
                             message: format!("Formatting block device with Ext4: {:?}",
                                              &brick.path),
                         })
                             .map_err(|e| e.to_string()));

                    let filesystem_type = block::Filesystem::Ext4 {
                        inode_size: 0,
                        reserved_blocks_percentage: 0,
                    };
                    try!(block::format_block_device(&brick.path, &filesystem_type)
                             .map_err(|e| e.to_string()));
                }
                block::FilesystemType::Btrfs => {
                    juju::log(&format!("Formatting block device with Btrfs: {:?}", &brick.path));
                    try!(juju::status_set(juju::Status {
                             status_type: juju::StatusType::Maintenance,
                             message: format!("Formatting block device with Btrfs: {:?}",
                                              &brick.path),
                         })
                             .map_err(|e| e.to_string()));

                    let filesystem_type = block::Filesystem::Btrfs {
                        leaf_size: 0,
                        node_size: 0,
                        metadata_profile: block::MetadataProfile::Single,
                    };
                    try!(block::format_block_device(&brick.path, &filesystem_type)
                             .map_err(|e| e.to_string()));
                }
                _ => {
                    juju::log(&format!("Formatting block device with XFS: {:?}", &brick.path));
                    try!(juju::status_set(juju::Status{
                        status_type: juju::StatusType::Maintenance,
                        message: format!("Formatting block device with XFS: {:?}", &brick.path),
                    }).map_err(|e| e.to_string()));

                    let filesystem_type = block::Filesystem::Xfs {
                        inode_size: None,
                        force: true,
                    };
                    try!(block::format_block_device(&brick.path, &filesystem_type)
                             .map_err(|e| e.to_string()));
                }
            }

            // Update our block device info to reflect formatting
            let device_info = try!(block::get_device_info(&brick.path).map_err(|e| e.to_string()));
            juju::log(&format!("device_info: {:?}", device_info));
            try!(check_and_create_dir(&mount_path));

            juju::log(&format!("Mounting block device {:?} at {}", &brick.path, mount_path));
            try!(juju::status_set(juju::Status {
                     status_type: juju::StatusType::Maintenance,
                     message: format!("Mounting block device {:?} at {}", &brick.path, mount_path),
                 })
                     .map_err(|e| e.to_string()));

            try!(block::mount_device(&device_info, &mount_path).map_err(|e| e.to_string()));

            // Modify the brick to point at the mnt point and add it to the clean list
            clean_bricks.push(gluster::Brick {
                peer: brick.peer.clone(),
                path: PathBuf::from(mount_path),
            });
        } else {
            let brick_path_str = try!(brick.path
                                           .to_str()
                                           .ok_or(format!("Failed to transform path to string \
                                                           from {:?}",
                                                          &brick.path)));

            try!(check_and_create_dir(&brick_path_str));

            clean_bricks.push(gluster::Brick {
                peer: brick.peer.clone(),
                path: brick.path.clone(),
            });
        }
    }
    return Ok(clean_bricks);
}

// Create a new volume if enough peers are available
fn create_volume(peers: &Vec<gluster::Peer>,
                 volume_info: Option<gluster::Volume>)
                 -> Result<i32, String> {
    let cluster_type_config = try!(get_config_value("cluster_type"));
    let cluster_type = gluster::VolumeType::from_str(&cluster_type_config);
    let volume_name = try!(get_config_value("volume_name"));
    let replicas = match try!(get_config_value("replication_level")).parse() {
        Ok(r) => r,
        Err(e) => {
            juju::log(&format!("Invalid config value for replicas.  Defaulting to 3. Error was {}", e));
            3
        }
    };

    // Make sure all peers are in the cluster
    // spinlock
    try!(wait_for_peers());

    // Build the brick list
    let brick_list = match get_brick_list(&peers, volume_info) {
        Ok(list) => list,
        Err(e) => {
            match e{
                Status::WaitForMorePeers => {
                    juju::log(&"Waiting for more peers".to_string());
                    return Ok(0);
                },
                Status::InvalidConfig(config_err) => {
                    return Err(config_err);
                },
                _ => {
                    //Some other error
                    return Err(format!("Unknown error in create volume: {:?}", e));
                }
            }
        }
    };
    juju::log(&format!("Got brick list: {:?}", brick_list));

    // Check to make sure the bricks are formatted and mounted
    let clean_bricks = try!(check_brick_list(&brick_list).map_err(|e| e.to_string()));

    juju::log(&format!("Creating volume of type {:?} with brick list {:?}",
                       cluster_type,
                       clean_bricks));

    match cluster_type {
        gluster::VolumeType::Distribute => {
            gluster::volume_create_distributed(&volume_name,
                                               gluster::Transport::Tcp,
                                               clean_bricks,
                                               true)
                .map_err(|e| e.to_string())
        }
        gluster::VolumeType::Stripe => {
            gluster::volume_create_striped(&volume_name,
                                           3,
                                           gluster::Transport::Tcp,
                                           clean_bricks,
                                           true)
                .map_err(|e| e.to_string())
        }
        gluster::VolumeType::Replicate => {
            gluster::volume_create_replicated(&volume_name,
                                              replicas,
                                              gluster::Transport::Tcp,
                                              clean_bricks,
                                              true)
                .map_err(|e| e.to_string())
        }
        gluster::VolumeType::StripedAndReplicate => {
            gluster::volume_create_striped_replicated(&volume_name,
                                                      3,
                                                      3,
                                                      gluster::Transport::Tcp,
                                                      clean_bricks,
                                                      true)
                .map_err(|e| e.to_string())
        }
        gluster::VolumeType::Disperse => {
            gluster::volume_create_erasure(&volume_name,
                                           3,
                                           1,
                                           gluster::Transport::Tcp,
                                           clean_bricks,
                                           true)
                .map_err(|e| e.to_string())
        }
        // gluster::VolumeType::Tier => {},
        gluster::VolumeType::DistributedAndStripe => {
            gluster::volume_create_striped(&volume_name,
                                           3,
                                           gluster::Transport::Tcp,
                                           clean_bricks,
                                           true)
                .map_err(|e| e.to_string())
        }
        gluster::VolumeType::DistributedAndReplicate => {
            gluster::volume_create_replicated(&volume_name,
                                              3,
                                              gluster::Transport::Tcp,
                                              clean_bricks,
                                              true)
                .map_err(|e| e.to_string())
        }
        gluster::VolumeType::DistributedAndStripedAndReplicate => {
            gluster::volume_create_striped_replicated(&volume_name,
                                                      3,
                                                      3,
                                                      gluster::Transport::Tcp,
                                                      clean_bricks,
                                                      true)
                .map_err(|e| e.to_string())
        }
        gluster::VolumeType::DistributedAndDisperse =>
            gluster::volume_create_erasure(
                &volume_name,
                brick_list.len()-1, //TODO: This number has to be lower than the brick length
                1,
                gluster::Transport::Tcp,
                clean_bricks,
                true).map_err(|e| e.to_string()),
    }
}

// Expands the volume by X servers+bricks
// Adds bricks and then runs a rebalance
fn expand_volume(peers: Vec<gluster::Peer>,
                 volume_info: Option<gluster::Volume>)
                 -> Result<i32, String> {
    let volume_name = try!(get_config_value("volume_name"));

    // Are there new peers?
    juju::log(&format!("Checking for new peers to expand the volume named {}",
                       volume_name));

    // Build the brick list
    let brick_list = match get_brick_list(&peers, volume_info) {
        Ok(list) => list,
        Err(e) => {
            match e{
                Status::WaitForMorePeers => {
                    juju::log(&"Waiting for more peers".to_string());
                    return Ok(0);
                },
                Status::InvalidConfig(config_err) => {
                    return Err(config_err);
                },
                _ => {
                    //Some other error
                    return Err(format!("Unknown error in expand volume: {:?}", e));
                }
            }
        }
    };

    // Check to make sure the bricks are formatted and mounted
    let clean_bricks = try!(check_brick_list(&brick_list).map_err(|e| e.to_string()));

    juju::log(&format!("Expanding volume with brick list: {:?}", clean_bricks));
    match gluster::volume_add_brick(&volume_name, clean_bricks, true) {
        Ok(o) => Ok(o),
        Err(e) => Err(e.to_string()),
    }
}

fn shrink_volume(peer: gluster::Peer, volume_info: Option<gluster::Volume>) -> Result<i32, String> {
    let volume_name = try!(get_config_value("volume_name"));

    juju::log(&format!("Shrinking volume named  {}", volume_name));

    let peers: Vec<gluster::Peer> = vec![peer];

    // Build the brick list
    let brick_list = match get_brick_list(&peers, volume_info) {
        Ok(list) => list,
        Err(e) => {
            match e{
                Status::WaitForMorePeers => {
                    juju::log(&"Waiting for more peers".to_string());
                    return Ok(0);
                },
                Status::InvalidConfig(config_err) => {
                    return Err(config_err);
                },
                _ => {
                    //Some other error
                    return Err(format!("Unknown error in shrink volume: {:?}", e));
                }
            }
        }
    };

    juju::log(&format!("Shrinking volume with brick list: {:?}", brick_list));
    match gluster::volume_remove_brick(&volume_name, brick_list, true) {
        Ok(o) => Ok(o),
        Err(e) => Err(e.to_string()),
    }
}

fn server_changed() -> Result<(), String> {
    let context = juju::Context::new_from_env();
    let leader = try!(juju::is_leader().map_err(|e| e.to_string()));
    let volume_name = try!(get_config_value("volume_name"));

    if leader {
        juju::log(&format!("I am the leader: {}", context.relation_id));
        juju::log(&"Loading config".to_string());

        let mut f = try!(File::open("config.yaml").map_err(|e| e.to_string()));
        let mut s = String::new();
        try!(f.read_to_string(&mut s).map_err(|e| e.to_string()));

        try!(juju::status_set(juju::Status {
                 status_type: juju::StatusType::Maintenance,
                 message: "Checking for new peers to probe".to_string(),
             })
                 .map_err(|e| e.to_string()));

        let mut peers = try!(gluster::peer_list().map_err(|e| e.to_string()));
        juju::log(&format!("peer list: {:?}", peers));
        let related_units = try!(juju::relation_list().map_err(|e| e.to_string()));
        try!(probe_in_units(&peers, related_units));
        // Update our peer list
        peers = try!(gluster::peer_list().map_err(|e| e.to_string()));

        // Everyone is in.  Lets see if a volume exists
        let volume_info = gluster::volume_info(&volume_name);
        // juju::log(&format!("volume info for create/expand volume {:?}", volume_info));
        let existing_volume: bool;
        match volume_info {
            Ok(_) => {
                juju::log(&format!("Expading volume {}", volume_name));
                try!(juju::status_set(juju::Status {
                         status_type: juju::StatusType::Maintenance,
                         message: format!("Expanding volume {}", volume_name),
                     })
                         .map_err(|e| e.to_string()));

                match expand_volume(peers, volume_info.ok()) {
                    Ok(v) => {
                        juju::log(&format!("Expand volume succeeded.  Return code: {}", v));
                        try!(juju::status_set(juju::Status {
                                 status_type: juju::StatusType::Active,
                                 message: "Expand volume succeeded.".to_string(),
                             })
                                 .map_err(|e| e.to_string()));
                        return Ok(());
                    }
                    Err(e) => {
                        juju::log(&format!("Expand volume failed with output: {}", e));
                        try!(juju::status_set(juju::Status {
                                 status_type: juju::StatusType::Blocked,
                                 message: "Expand volume failed.  Please check juju debug-log."
                                              .to_string(),
                             })
                                 .map_err(|e| e.to_string()));
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
        // Only set existing_volume to false if we have this specific error
        // gluster::GlusterError::NoVolumesPresent => {
        // existing_volume = false;
        // }
        // Otherwise something failed and lets return that error
        // _ => {
        // return Err(e.to_string());
        // }
        // }
        // }
        //
        if !existing_volume {
            juju::log(&format!("Creating volume {}", volume_name));
            try!(juju::status_set(juju::Status {
                     status_type: juju::StatusType::Maintenance,
                     message: format!("Creating volume {}", volume_name),
                 })
                     .map_err(|e| e.to_string()));
            match create_volume(&peers, None) {
                Ok(_) => {
                    juju::log(&"Create volume succeeded.".to_string());
                    try!(juju::status_set(juju::Status {
                             status_type: juju::StatusType::Maintenance,
                             message: "Create volume succeeded".to_string(),
                         })
                             .map_err(|e| e.to_string()));
                }
                Err(e) => {
                    juju::log(&format!("Create volume failed with output: {}", e));
                    try!(juju::status_set(juju::Status {
                             status_type: juju::StatusType::Blocked,
                             message: "Create volume failed.  Please check juju debug-log."
                                          .to_string(),
                         })
                             .map_err(|e| e.to_string()));
                    return Err(e.to_string());
                }
            }
            match gluster::volume_start(&volume_name, false) {
                Ok(_) => {
                    juju::log(&"Starting volume succeeded.".to_string());
                    try!(juju::status_set(juju::Status {
                             status_type: juju::StatusType::Active,
                             message: "Starting volume succeeded.".to_string(),
                         })
                             .map_err(|e| e.to_string()));
                }
                Err(e) => {
                    juju::log(&format!("Start volume failed with output: {:?}", e));
                    try!(juju::status_set(juju::Status {
                             status_type: juju::StatusType::Blocked,
                             message: "Start volume failed.  Please check juju debug-log."
                                          .to_string(),
                         })
                             .map_err(|e| e.to_string()));
                    return Err(e.to_string());
                }
            };
        }
        try!(juju::status_set(juju::Status {
                 status_type: juju::StatusType::Active,
                 message: "".to_string(),
             })
                 .map_err(|e| e.to_string()));
        return Ok(());
    } else {
        return Ok(());
    }
}

fn server_removed() -> Result<(), String> {
    let private_address = try!(juju::unit_get_private_addr().map_err(|e| e.to_string()));
    juju::log(&format!("Removing server: {}", private_address));
    return Ok(());
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() > 0 {
        let mut hook_registry: Vec<juju::Hook> = Vec::new();

        // Register our hooks with the Juju library
        hook_registry.push(juju::Hook {
            name: "config-changed".to_string(),
            callback: Box::new(config_changed),
        });

        hook_registry.push(juju::Hook {
            name: "server-relation-changed".to_string(),
            callback: Box::new(server_changed),
        });

        hook_registry.push(juju::Hook {
            name: "server-relation-departed".to_string(),
            callback: Box::new(server_removed),
        });

        let result = juju::process_hooks(args, hook_registry);

        if result.is_err() {
            juju::log(&format!("Hook failed with error: {:?}", result.err()));
        }
    }
}
