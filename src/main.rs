mod block;

extern crate gluster;
extern crate itertools;
extern crate juju;
extern crate yaml_rust;

use itertools::Itertools;
use std::env;
use std::fs;
use std::fs::File;
use std::io::prelude::Read;
use std::path::Path;
use std::path::PathBuf;
use yaml_rust::{Yaml, YamlLoader};


/*
    A gluster server has either joined or left this cluster
*/

#[derive(Debug)]
struct Config{
    volume_name: String,
    brick_paths: Vec<String>,
    cluster_type: gluster::VolumeType,
    replicas: usize,
}

#[cfg(test)]
mod tests{
    extern crate uuid;
    use uuid::Uuid;

    #[test]
    fn generate_test_peers(amount: usize)->Vec<gluster::Peer>{
        let mut peers: Vec<gluster::Peer> = Vec::with_capacity(amount);
        let mut count = 0;
        loop{
            let p = gluster::Peer {
               uuid: Uuid::new_v4(),
               hostname: format!("host-{}",Uuid::new_v4()),
               status: gluster::State::Connected,
            };
            peers.push(p);
            count+=1;
            if count == amount{
                break;
            }
        }
        return peers;
    }

    #[test]
    fn generate_test_bricks(peers: &Vec<gluster::Peer>)->Vec<gluster::Brick>{
        let mut bricks: Vec<gluster::Brick> = Vec::with_capacity(peers.len());
        let mut count = 0;
        for peer in peers{
            let b = gluster::Brick{
               peer: peer.clone(),
               path: PathBuf::from(&format!("/mnt/{}",count)),
            };
            bricks.push(b);
            count+=1;
        }
        return bricks;
    }

    #[test]
    fn test_block_device_usage(){

    }

    #[test]
    fn test_load_config(){
        let result = load_config();
        println!("Result: {:?}", result);
    }

    /*
    #[test]
    fn test_brick_generation(){
        let mut test_peers = generate_test_peers(3);
        let data: Value = json::from_str("[\"/mnt/sda\", \"/mnt/sdb\"]").unwrap();
        let brick_path_array = data.as_array().unwrap();

        let c = Config{
            volume_name: "test".to_string(),
            brick_paths: brick_path_array.clone(),
            cluster_type: gluster::VolumeType::Replicate,
            replicas: 3,
        };

        //Case 1: New volume and perfectly matched peer number to replica number
        let b1 = get_brick_list(&c, &test_peers, None).unwrap();
        println!("get_brick_list 1: {:?}", b1);
        assert!(b1.len() == 6);

        //Case 2: New volume and we're short 1 Peer

        //Drop a peer off the end
        test_peers.pop();
        let b2 = get_brick_list(&c, &test_peers, None);
        println!("get_brick_list 2: {:?}", b2);
        assert!(b2.is_none());

        //Now add a peer and try again
        test_peers.push(gluster::Peer{
            uuid: Uuid::new_v4(),
            hostname: "host-x".to_string(),
            status: gluster::State::Connected,
        });
        let b3 = get_brick_list(&c, &test_peers, None);
        println!("get_brick_list 3: {:?}", b3);
        assert!(b1.len() == 6);


        //Case 3: Existing volume with 2 peers and we're adding 2 Peers
        let test_peers2 = generate_test_peers(2);
        let v = gluster::Volume {
            name: "test".to_string(),
            vol_type: gluster::VolumeType::Replicate,
            id: Uuid::new_v4(),
            status: "normal".to_string(),
            transport: gluster::Transport::Tcp,
            bricks: generate_test_bricks(&test_peers),
        };
        let b4 = get_brick_list(&c, &test_peers2, Some(v));
        println!("get_brick_list 4: {:?}", b4);
        assert!(b4.is_none());


        //Case 4: Mismatch of new volume and too many peers
        let test_peers3 = generate_test_peers(4);
        let b5 = get_brick_list(&c, &test_peers3, None).unwrap();
        println!("get_brick_list 5: {:?}", b5);
        assert!(b5.len() == 6);
    }
    */
}


//Reduce the code duplication
fn get_config_value<'a>(config_hash: &'a yaml_rust::yaml::Hash, name: String)->Option<&'a Yaml>{
    let field = config_hash.get(&Yaml::String(name)).unwrap();

    //Now we're down into that hash map for the field
    let field_hash = field.as_hash().unwrap();
    let default = field_hash.get(&Yaml::String("default".to_string()));

    return default;
}

fn config_changed()->Result<(), String>{
    //load the config again
    let new_config = load_config();

    //how do we figure out what changed?
    return Ok(());
}
//Parsing this yaml file is hideous
fn load_config() -> Result<Config, String>{
    let mut f = try!(File::open("config.yaml").map_err(|e| e.to_string()));

    let mut s = String::new();
    try!(f.read_to_string(&mut s).map_err(|e| e.to_string()));

    //Remove this hack when the new version of yaml_rust releases to get the real error msg
    let data = match YamlLoader::load_from_str(&s){
        Ok(data) => data,
        Err(_) => {
            return Err("Unable to load yaml data from config file".to_string());
        },
    };

    if data.len() < 1{
        return Err("Missing options: field in yaml configuration file".to_string());
    }

    //This is ugly but it takes several levels to get down to the real data
    let config_options = try!(data.get(0).ok_or(
        "options are not in the form of a dictionary"));

    let config_options_hash = try!(config_options.as_hash().ok_or(
        "Unable to parse options in the form of a dictionary"));

    let options = try!(config_options_hash.get(&Yaml::String("options".to_string())).ok_or(
        "Unable to parse options from config file"));

    //Pass this to our function
    let hash_map = try!(options.as_hash().ok_or(
        "options are not in the form of a dictionary"));

    let brick_paths = try!(get_config_value(hash_map, "brick_paths".to_string()).ok_or(
        "Unable to parse brick_paths from config file"));

    let volume_name = try!(get_config_value(hash_map, "volume_name".to_string()).ok_or(
        "Unable to parse volume_name from config file"));

    let volume_name_str = try!(volume_name.as_str().ok_or(
        "Unable to convert volume_name to a String"));

    let cluster_type_field = try!(get_config_value(hash_map, "cluster_type".to_string()).ok_or(
        "Unable to parse cluster_type from config file"));

    let cluster_type = try!(cluster_type_field.as_str().ok_or(
        "Unable to convert cluster_type to a String"));

    let replica_field = try!(get_config_value(hash_map, "replication_level".to_string()).ok_or(
        "Unable to parse replicas from config file"));

    let replicas = try!(replica_field.as_i64().ok_or(
        "Unable to convert replicas to an integer"));

    let brick_path_pieces: Vec<&str> = try!(brick_paths.as_str().ok_or(
        "Unable to parse convert brick_paths into a String")).split(" ").collect();

    let bricks: Vec<String> = brick_path_pieces.iter().map(|s| s.to_string()).collect();

    //ok we've read the config lets parse it
    let config = Config{
        volume_name: volume_name_str.to_string(),
        brick_paths: bricks,
        cluster_type: gluster::VolumeType::new(&cluster_type.to_string()),
        replicas: (replicas as usize),
    };
    juju::log(&format!("Config yaml file: {:?}", config));
    return Ok(config);
}

//Probe in a unit if they haven't joined yet
fn probe_in_units(existing_peers: &Vec<gluster::Peer>,
    related_units: Vec<juju::Relation>)->Result<(), String>{

    juju::log(&format!("Adding in related_units: {:?}", related_units));
    for unit in related_units{
        let address = try!(juju::relation_get_by_unit(&"private-address".to_string(), &unit));
        let address_trimmed = address.trim().to_string();
        let mut already_probed: bool = false;

        //I think the localhost test is failing
        for peer in existing_peers{
            if peer.hostname == address_trimmed{
                already_probed = true;
            }
        }

        //Probe the peer in
        if !already_probed{
            juju::log(&format!("Adding {} to cluster", address_trimmed));
            match gluster::peer_probe(&address_trimmed){
                Ok(_) => juju::log(&"Gluster peer probe was successful".to_string()),
                Err(why) => {
                    juju::log(&format!("Gluster peer probe failed: {}", why));
                    return Err(why.to_string());
                },
            };
        }
    }
    return Ok(());
}

fn find_new_peers(peers: &Vec<gluster::Peer>, volume_info: &gluster::Volume)->Vec<gluster::Peer>{
    let mut new_peers: Vec<gluster::Peer> = Vec::new();
    for peer in peers{
        //If this peer is already in the volume, skip it
        let mut new_peer: bool = true;

        for brick in volume_info.bricks.iter(){
            if brick.peer.uuid == peer.uuid{
                new_peer = false;
                break;
            }
        }
        if new_peer{
            new_peers.push(peer.clone());
        }
    }
    return new_peers;
}

fn brick_and_server_cartesian_product(peers: &Vec<gluster::Peer>, paths: &Vec<String>)->Vec<gluster::Brick>{
    let mut product: Vec<gluster::Brick> = Vec::new();

    let it = paths.iter().cartesian_product(peers.iter());
    for (path, host) in it{
        let brick = gluster::Brick{
            peer: host.clone(),
            path: PathBuf::from(path),
        };
        product.push(brick);
    }
    return product;
}

//This function will take into account the replication level and
//try its hardest to produce a list of bricks that satisfy this:
//1. Are not already in the volume
//2. Sufficient hosts to satisfy replication level
//3. Stripped across the hosts
//If insufficient hosts exist to satisfy this replication level this will return no new bricks to add
fn get_brick_list(config: &Config, peers: &Vec<gluster::Peer>, volume: Option<gluster::Volume>)
    -> Option<Vec<gluster::Brick>>{

    if volume.is_none(){
        juju::log(&"Volume is none".to_string());
        //number of bricks % replicas == 0 then we're ok to proceed
        if peers.len() < config.replicas {
            //Not enough peers to replicate across
            juju::log(&"Not enough peers to satisfy the replication level for the Gluster volume.  Waiting for more peers to join.".to_string());
            return None;
        }else if peers.len() == config.replicas{
            //Case 1: A perfect marriage of peers and number of replicas
            juju::log(&"Number of peers and number of replicas match".to_string());
            return Some(brick_and_server_cartesian_product(peers, &config.brick_paths));
        }else{
            //Case 2: We have a mismatch of replicas and hosts
            //Take as many as we can and leave the rest for a later time
            let count = peers.len() - (peers.len() % config.replicas);
            let mut new_peers = peers.clone();

            //Drop these peers off the end of the list
            new_peers.truncate(count);
            juju::log(&format!("Too many new peers.  Dropping {} peers off the list", count));
            return Some(brick_and_server_cartesian_product(&new_peers, &config.brick_paths));
        }
    }else{
        //Existing volume.  Build a differential list.
        juju::log(&"Existing volume.  Building differential brick list".to_string());
        let mut new_peers = find_new_peers(peers, &volume.unwrap());

        if new_peers.len() < config.replicas{
            juju::log(&"New peers found are less than needed by the replica count".to_string());
            return None;
        }else if new_peers.len() == config.replicas {
            juju::log(&"New peers and number of replicas match".to_string());
            return Some(brick_and_server_cartesian_product(&new_peers, &config.brick_paths));
        }else{
            let count = new_peers.len() - (new_peers.len() % config.replicas);
            //Drop these peers off the end of the list
            juju::log(&format!("Too many new peers.  Dropping {} peers off the list", count));
            new_peers.truncate(count);
            return Some(brick_and_server_cartesian_product(&new_peers, &config.brick_paths));
        }
    }
}

fn check_and_create_dir(path: &str)->Result<(), String>{
    match fs::metadata(path){
        Ok(_) => {
            return Ok(())
        },
        Err(e) => {
            match e.kind(){
                std::io::ErrorKind::NotFound => {
                    juju::log(&format!("Creating dir {}", path));
                    try!(fs::create_dir(&path).map_err(|e| e.to_string()));
                    return Ok(());
                },
                _ => {
                    return Err(format!("Error searching for directory {:?} {:?}",&path, e.kind()));
                },
            }
        }
    }
}

//TODO: This blindly formats block devices and ignores what's on them.
fn check_brick_list(bricks: &Vec<gluster::Brick>)->Result<Vec<gluster::Brick>, String>{
    let mut clean_bricks: Vec<gluster::Brick> = Vec::new();
    for brick in bricks{
        let block_check = block::is_block_device(&brick.path);

        if block_check.is_ok(){

            let dev_name = try!(brick.path.file_name().ok_or(
                format!("Failed to get device name from {:?}", &brick.path)));
            let dev_name_str = try!(dev_name.to_str().ok_or(
                format!("Failed to transform device name to string {:?}", &dev_name)));

            let mount_path = format!("/mnt/{}", dev_name_str);

            juju::log(&format!("Gathering info on block device: {:?}", &brick.path));

            //Format with the default XFS unless told otherwise
            juju::log(&format!("Formatting block device with XFS: {:?}", &brick.path));
            let filesystem_type = block::Filesystem::Xfs{inode_size: None, force: true};
            try!(block::format_block_device(
                &brick.path,
                &filesystem_type).map_err(|e| e.to_string()));

            //Update our block device info to reflect formatting
            let device_info = try!(
                block::get_device_info(&brick.path).map_err(|e| e.to_string())
            );
            juju::log(&format!("device_info: {:?}", device_info));
            try!(check_and_create_dir(&mount_path));

            juju::log(&format!("Mounting block device {:?} at {}", &brick.path, mount_path));
            try!(block::mount_device(
                &device_info,
                &mount_path).map_err(|e| e.to_string()));

            //Modify the brick to point at the mnt point and add it to the clean list
            clean_bricks.push(gluster::Brick{
                peer: brick.peer.clone(),
                path: PathBuf::from(mount_path),
            });
        }else{
            let brick_path_str = try!(brick.path.to_str().ok_or(
                format!("Failed to transform path to string from {:?}", &brick.path)));

            try!(check_and_create_dir(&brick_path_str));

            clean_bricks.push(gluster::Brick{
                peer: brick.peer.clone(),
                path: brick.path.clone(),
            });
        }
    }
    return Ok(clean_bricks);
}

//Create a new volume if enough peers are available
fn create_volume(
    config: &Config,
    peers: &Vec<gluster::Peer>,
    volume_info: Option<gluster::Volume>)->Result<i32,String>{

    //Build the brick list
    let brick_list = match get_brick_list(&config, &peers, volume_info){
        Some(list) => list,
        None => return Ok(0),
    };
    juju::log(&format!("Got brick list: {:?}", brick_list));

    //Check to make sure the bricks are formatted and mounted
    let clean_bricks = try!(
        check_brick_list(&brick_list).map_err(|e| e.to_string())
    );

    juju::log(&format!("Creating volume of type {:?} with brick list {:?}",
        config.cluster_type,
        clean_bricks));

    match config.cluster_type {
        gluster::VolumeType::Distribute =>
            gluster::volume_create_distributed(
                &config.volume_name,
                gluster::Transport::Tcp,
                clean_bricks,
                true).map_err(|e| e.to_string()),
        gluster::VolumeType::Stripe =>
            gluster::volume_create_striped(
                &config.volume_name,
                3,
                gluster::Transport::Tcp,
                clean_bricks,
                true).map_err(|e| e.to_string()),
        gluster::VolumeType::Replicate =>
            gluster::volume_create_replicated(
                &config.volume_name,
                config.replicas,
                gluster::Transport::Tcp,
                clean_bricks,
                true).map_err(|e| e.to_string()),
        gluster::VolumeType::StripedAndReplicate =>
            gluster::volume_create_striped_replicated(
                &config.volume_name,
                3,
                3,
                gluster::Transport::Tcp,
                clean_bricks,
                true).map_err(|e| e.to_string()),
        gluster::VolumeType::Disperse =>
            gluster::volume_create_erasure(
                &config.volume_name,
                3,
                1,
                gluster::Transport::Tcp,
                clean_bricks,
                true).map_err(|e| e.to_string()),
        //gluster::VolumeType::Tier => {},
        gluster::VolumeType::DistributedAndStripe =>
            gluster::volume_create_striped(
                &config.volume_name,
                3,
                gluster::Transport::Tcp,
                clean_bricks,
                true).map_err(|e| e.to_string()),
        gluster::VolumeType::DistributedAndReplicate =>
            gluster::volume_create_replicated(
                &config.volume_name,
                3,
                gluster::Transport::Tcp,
                clean_bricks,
                true).map_err(|e| e.to_string()),
        gluster::VolumeType::DistributedAndStripedAndReplicate =>
            gluster::volume_create_striped_replicated(
                &config.volume_name,
                3,
                3,
                gluster::Transport::Tcp,
                clean_bricks,
                true).map_err(|e| e.to_string()),
        gluster::VolumeType::DistributedAndDisperse =>
            gluster::volume_create_erasure(
                &config.volume_name,
                brick_list.len()-1, //TODO: This number has to be lower than the brick length
                1,
                gluster::Transport::Tcp,
                clean_bricks,
                true).map_err(|e| e.to_string()),
    }
}

//Expands the volume by X servers+bricks
//Adds bricks and then runs a rebalance
fn expand_volume(
    config: Config,
    peers: Vec<gluster::Peer>,
    volume_info: Option<gluster::Volume>) -> Result<i32, String>{

    //Are there new peers?
    juju::log(&format!("Checking for new peers to expand the volume named {}", config.volume_name));

    //Build the brick list
    let brick_list = match get_brick_list(&config, &peers, volume_info){
        Some(list) => list,
        None => {
            juju::log(&"No new bricks found to be added to the volume".to_string());
            return Ok(0);
        },
    };

    //Check to make sure the bricks are formatted and mounted
    let clean_bricks = try!(
        check_brick_list(&brick_list).map_err(|e| e.to_string())
    );

    juju::log(&format!("Expanding volume with brick list: {:?}", clean_bricks));
    match gluster::volume_add_brick(&config.volume_name, clean_bricks, true){
        Ok(o) => Ok(o),
        Err(e) => Err(e.to_string()),
    }
}

fn shrink_volume(
    config: Config,
    peer: gluster::Peer,
    volume_info: Option<gluster::Volume>) -> Result<i32, String>{

    juju::log(&format!("Shrinking volume named  {}", config.volume_name));

    let peers: Vec<gluster::Peer> = vec![peer];

    //Build the brick list
    let brick_list = match get_brick_list(&config, &peers, volume_info){
        Some(list) => list,
        None => {return Ok(0);},
    };

    juju::log(&format!("Shrinking volume with brick list: {:?}", brick_list));
    match gluster::volume_remove_brick(&config.volume_name, brick_list, true){
        Ok(o) => Ok(o),
        Err(e) => Err(e.to_string()),
    }
}

fn server_joined()->Result<(), String>{
    //This should panic.  Can't recover from this
    let private_address = try!(juju::unit_get_private_addr());
    try!(juju::relation_set("hostname", &private_address));
    return Ok(());
}

fn server_changed()->Result<(),String>{
    let context = juju::Context::new_from_env();
    let leader = try!(juju::is_leader());

    if leader{
        juju::log(&format!("I am the leader: {}", context.relation_id));
        juju::log(&"Loading config".to_string());
        let config = try!(load_config());

        juju::log(&"Checking for new peers to probe".to_string());
        let mut peers = try!(gluster::peer_list().map_err(|e| e.to_string()));
        juju::log(&format!("peer list: {:?}", peers));
        let related_units = try!(juju::relation_list());
        try!(probe_in_units(&peers, related_units));
        //Update our peer list
        peers = try!(gluster::peer_list().map_err(|e| e.to_string()));

        //Everyone is in.  Lets see if a volume exists
        let volume_info = gluster::volume_info(&config.volume_name);
        juju::log(&format!("volume info for create/expand volume {:?}", volume_info));
        let mut existing_volume: bool;
        match volume_info {
            Some(..) => existing_volume = true,
            None => existing_volume = false,
        }
        if existing_volume {
            //We need to add bricks in groups of the replica count.  how do we do this?
            juju::log(&format!("Expanding volume {}", config.volume_name));
            match expand_volume(config, peers, volume_info){
                Ok(v) => {
                    juju::log(&format!("Expand volume succeeded.  Return code: {}", v));
                    return Ok(());
                },
                Err(e) => {
                    juju::log(&format!("Expand volume failed with output: {}", e));
                    return Err(e);
                },
            }
        }else{
            juju::log(&format!("Creating volume {}", config.volume_name));
            match create_volume(&config, &peers, volume_info){
                Ok(_) => juju::log(&"Create volume succeeded.".to_string()),
                Err(e) => {
                    juju::log(&format!("Create volume failed with output: {}", e));
                    return Err(e.to_string());
                },
            }
            match gluster::volume_start(&config.volume_name, false){
                Ok(_) => juju::log(&"Starting volume succeeded.".to_string()),
                Err(e) => {
                    juju::log(&format!("Start volume failed with output: {}", e));
                    return Err(e.to_string());
                },
            };
        }
        return Ok(());
    }else{
        return Ok(());
    }
}

fn server_removed()->Result<(),String>{
    let private_address = try!(juju::unit_get_private_addr().map_err(|e| e.to_string()));
    juju::log(&format!("Removing server: {}", private_address));
    return Ok(());
}


fn main(){
    //REMOTE_ADDRESS=`relation-get private-address $REMOTE`
    //TODO: Move this to the juju crate where it belongs
    //TODO: Expose client relation functionality
    let args: Vec<String> = env::args().collect();
    if args.len() > 0{
        let path = Path::new(args[0].trim());
        let filename = match path.file_name(){
            Some(filename) => filename,
            None => {
                juju::log(&format!("Unable to parse filename from {:?}", path));
                return;
            },
        };
        let match_str = match filename.to_str(){
            Some(filename) => filename,
            None => {
                juju::log(&format!("Failed to transform filename into string {:?}.  Bad symlink name perhaps? Bailing", filename));
                return;
            },
        };
        let result = match match_str {
            //"leader-elected" => {},
            //"leader-settings-changed" => {},
            //"server-relation-broken" => {},
            "config-changed" => config_changed(),
            "server-relation-changed" => server_changed(),
            "server-relation-departed" => server_removed(),
            "server-relation-joined" => server_joined(),
            //if no match call server_changed as a last ditch effort
            _ => server_changed(),
        };
        match result{
            Ok(_) => {},
            Err(e) => juju::log(&format!("Execution failed with error: {}", e)),
        };
    }else{
        juju::log(&"Invalid args.  Could not determine which hook to call.".to_string());
    }
}
