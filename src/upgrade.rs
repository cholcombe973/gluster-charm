extern crate chrono;
extern crate gluster;
extern crate init_daemon;
extern crate juju;
extern crate rand;
extern crate rustc_serialize;
extern crate uuid;

use std::fs::{create_dir, File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::thread;

use self::chrono::*;
use self::gluster::{volume_info, Peer};
use self::rand::distributions::{IndependentSample, Range};
use self::rustc_serialize::json;
use self::uuid::Uuid;

use super::apt;
use super::debian::version::Version;
use super::get_glusterfs_version;
use log::LogLevel;

fn get_local_uuid() -> Result<Uuid, String> {
    // File looks like this:
    // UUID=30602134-698f-4e53-8503-163e175aea85
    // operating-version=30800
    let f = File::open("/var/lib/glusterd/glusterd.info").map_err(|e| e.to_string())?;
    let mut reader = BufReader::new(f);

    let mut line = String::new();
    reader.read_line(&mut line).map_err(|e| e.to_string())?;
    if line.contains("UUID") {
        let parts: Vec<&str> = line.split("=").collect();
        let uuid = Uuid::parse_str(parts[1].trim()).map_err(|e| e.to_string())?;
        return Ok(uuid);
    }
    Err("Unable to find UUID".to_string())
}

// Edge cases:
// 1. Previous node dies on upgrade, can we retry?
pub fn roll_cluster(new_version: &Version) -> Result<(), String> {
    // This is tricky to get right so here's what we're going to do.
    // :param new_version: str of the version to upgrade to
    // There's 2 possible cases: Either I'm first in line or not.
    // If I'm not first in line I'll wait a random time between 5-30 seconds
    // and test to see if the previous peer is upgraded yet.
    //
    juju::log(&format!("roll_cluster called with {}", new_version),
              Some(LogLevel::Debug));
    let volume_name = juju::config_get(&"volume_name".to_string()).map_err(|e| e.to_string())?;
    let my_uuid = get_local_uuid()?;

    let volume_bricks = volume_info(&volume_name).map_err(|e| e.to_string())?.bricks;
    let mut peer_list: Vec<Peer> = volume_bricks.iter().map(|x| x.peer.clone()).collect();
    juju::log(&format!("peer_list: {:?}", peer_list),
              Some(LogLevel::Debug));

    // Sort by UUID
    peer_list.sort();
    // We find our position by UUID
    let position = match peer_list.iter().position(|x| x.uuid == my_uuid) {
        Some(p) => p,
        None => {
            juju::log(&format!("Unable to determine upgrade position from: {:?}", peer_list),
                      Some(LogLevel::Error));
            return Err("Unable to determine upgrade position".to_string());
        }
    };
    juju::log(&format!("upgrade position: {}", position),
              Some(LogLevel::Debug));
    if position == 0 {
        // I'm first!  Roll
        // First set a key to inform others I'm about to roll
        lock_and_roll(&my_uuid, new_version)?;
    } else {
        // Check if the previous node has finished
        juju::status_set(juju::Status {
                status_type: juju::StatusType::Waiting,
                message: format!("Waiting on {:?} to finish upgrading",
                                 peer_list[position - 1]),
            }).map_err(|e| e.to_string())?;
        wait_on_previous_node(&peer_list[position - 1], new_version)?;
        lock_and_roll(&my_uuid, new_version)?;
    }
    Ok(())
}

pub fn upgrade_peer(new_version: &Version) -> Result<(), String> {
    let current_version = get_glusterfs_version().map_err(|e| e.to_string())?;
    juju::status_set(juju::Status {
            status_type: juju::StatusType::Maintenance,
            message: "Upgrading peer".to_string(),
        }).map_err(|e| e.to_string())?;
    juju::log(&format!("Current ceph version is {}", current_version),
              Some(LogLevel::Debug));
    juju::log(&format!("Upgrading to: {}", new_version),
              Some(LogLevel::Debug));

    apt::service_stop("glusterfs-server")?;
    apt::apt_install(vec!["glusterfs-server", "glusterfs-common", "glusterfs-client"])?;
    apt::service_start("glusterfs-server")?;
    super::update_status()?;
    return Ok(());
}

fn lock_and_roll(my_uuid: &Uuid, version: &Version) -> Result<(), String> {
    let start_timestamp = Local::now();

    juju::log(&format!("gluster_key_set {}_{}_start {}",
                       my_uuid,
                       version,
                       start_timestamp),
              Some(LogLevel::Debug));
    gluster_key_set(&format!("{}_{}_start", &my_uuid, version), start_timestamp)?;
    juju::log("Rolling", Some(LogLevel::Debug));

    // This should be quick
    upgrade_peer(&version)?;
    juju::log("Done", Some(LogLevel::Debug));

    let stop_timestamp = Local::now();
    // Set a key to inform others I am finished
    juju::log(&format!("gluster_key_set {}_{}_done {}",
                       my_uuid,
                       version,
                       stop_timestamp),
              Some(LogLevel::Debug));
    gluster_key_set(&format!("{}_{}_done", &my_uuid, version), stop_timestamp)?;

    return Ok(());
}



fn gluster_key_get(key: &str) -> Option<DateTime<Local>> {
    let mut f = match File::open(&format!("/mnt/glusterfs/.upgrade/{}", key)) {
        Ok(f) => f,
        Err(_) => {
            return None;
        }
    };
    let mut s = String::new();
    match f.read_to_string(&mut s) {
        Ok(bytes) => {
            juju::log(&format!("gluster_key_get read {} bytes", bytes),
                      Some(LogLevel::Debug));
        }
        Err(e) => {
            juju::log(&format!("gluster_key_get failed to read file \
                                /mnt/glusterfs/.upgraded/{}. Error: {}",
                               key,
                               e),
                      Some(LogLevel::Error));
            return None;
        }
    };
    let decoded: DateTime<Local> = match json::decode(&s) {
        Ok(d) => d,
        Err(e) => {
            juju::log(&format!("Failed to decode json file in gluster_key_get(): {}", e),
                      Some(LogLevel::Error));
            return None;
        }
    };
    Some(decoded)
}

fn gluster_key_set(key: &str, timestamp: DateTime<Local>) -> Result<(), String> {
    if !Path::new("/mnt/glusterfs/.upgrade").exists() {
        create_dir("/mnt/glusterfs/.upgrade").map_err(|e| e.to_string())?;
    }
    let mut file = try!(OpenOptions::new()
        .write(true)
        .create(true)
        .open(&format!("/mnt/glusterfs/.upgrade/{}", key))
        .map_err(|e| e.to_string()));
    let encoded = json::encode(&timestamp).map_err(|e| e.to_string())?;
    try!(file.write(&encoded.as_bytes()).map_err(|e| e.to_string()));
    Ok(())
}

fn gluster_key_exists(key: &str) -> bool {
    let location = format!("/mnt/glusterfs/.upgrade/{}", key);
    let p = Path::new(&location);
    return p.exists();
}

pub fn wait_on_previous_node(previous_node: &Peer, version: &Version) -> Result<(), String> {
    juju::log(&format!("Previous node is: {:?}", previous_node),
              Some(LogLevel::Debug));

    let mut previous_node_finished =
        gluster_key_exists(&format!("{}_{}_done", previous_node.uuid, version));

    while !previous_node_finished {
        juju::log(format!("{} is not finished. Waiting", previous_node.uuid),
                  Some(LogLevel::Debug));
        // Has this node been trying to upgrade for longer than
        // 10 minutes?
        // If so then move on and consider that node dead.

        // NOTE: This assumes the clusters clocks are somewhat accurate
        // If the hosts clock is really far off it may cause it to skip
        // the previous node even though it shouldn't.
        let current_timestamp = Local::now();

        let previous_node_start_time =
            gluster_key_get(&format!("{}_{}_start", previous_node.uuid, version));
        match previous_node_start_time {
            Some(previous_start_time) => {
                if (current_timestamp - Duration::minutes(10)) > previous_start_time {
                    // Previous node is probably dead.  Lets move on
                    if previous_node_start_time.is_some() {
                        juju::log(&format!("Waited 10 mins on node {}. current time: {} > \
                                            previous node start time: {} Moving on",
                                           previous_node.uuid,
                                           (current_timestamp - Duration::minutes(10)),
                                           previous_start_time),
                                  Some(LogLevel::Debug));
                        return Ok(());
                    }
                } else {
                    // I have to wait.  Sleep a random amount of time and then
                    // check if I can lock,upgrade and roll.
                    let between = Range::new(5, 30);
                    let mut rng = rand::thread_rng();
                    let wait_time = between.ind_sample(&mut rng);
                    juju::log(format!("waiting for {} seconds", wait_time),
                              Some(LogLevel::Debug));
                    thread::sleep(::std::time::Duration::from_secs(wait_time));
                    previous_node_finished =
                        gluster_key_exists(&format!("{}_{}_done", previous_node.uuid, version));
                }
            }
            None => {
                // There is no previous start time.  What should we do?
            }
        }
    }
    Ok(())
}
