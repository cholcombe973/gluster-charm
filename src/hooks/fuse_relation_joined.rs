extern crate gluster;
extern crate juju;

use gluster::volume::volume_list;

pub fn fuse_relation_joined() -> Result<(), String> {
    // Fuse clients only need one ip address and they can discover the rest
    let public_addr = try!(juju::unit_get_public_addr().map_err(|e| e.to_string())).to_string();
    let volumes = volume_list();
    juju::relation_set("gluster-public-address", &public_addr).map_err(|e| e.to_string())?;
    if let Some(vols) = volumes {
        juju::relation_set("volumes", &vols.join(" ")).map_err(|e| e.to_string())?;
    }

    Ok(())
}
