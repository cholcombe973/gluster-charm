extern crate gluster;
extern crate juju;

use gluster::volume::volume_list;
use super::super::resolve_first_vip_to_dns;


pub fn nfs_relation_joined() -> Result<(), String> {
    let config_value = juju::config_get("virtual_ip_addresses").map_err(|e| e.to_string())?;
    let volumes = volume_list();
    if let Some(vols) = volumes {
        juju::relation_set("volumes", &vols.join(" ")).map_err(|e| e.to_string())?;
    }

    // virtual_ip_addresses isn't set.  Handing back my public address
    if !config_value.is_some() {
        let public_addr = try!(juju::unit_get_public_addr().map_err(|e| e.to_string())).to_string();
        juju::relation_set("gluster-public-address", &public_addr).map_err(|e| e.to_string())?;
    } else {
        // virtual_ip_addresses is set.  Handing back the DNS resolved address
        let dns_name = resolve_first_vip_to_dns()?;
        juju::relation_set("gluster-public-address", &dns_name).map_err(|e| e.to_string())?;
    }
    Ok(())
}
