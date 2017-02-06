use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

use super::apt::{apt_install, service_start};
use super::juju;

/// Write the samba configuration file out to disk
pub fn render_samba_configuration<T: Write>(f: &mut T,
                                            volume_name: &str)
                                            -> Result<usize, ::std::io::Error> {
    let mut bytes_written = 0;
    bytes_written += f.write(&format!("[{}]\n", volume_name).as_bytes())?;
    bytes_written += f.write(b"path = /mnt/glusterfs\n")?;
    bytes_written += f.write(b"read only = no\n")?;
    bytes_written += f.write(b"guest ok = yes\n")?;
    bytes_written += f.write(b"kernel share modes = no\n")?;
    bytes_written += f.write(b"kernel oplocks = no\n")?;
    bytes_written += f.write(b"map archive = no\n")?;
    bytes_written += f.write(b"map hidden = no\n")?;
    bytes_written += f.write(b"map read only = no\n")?;
    bytes_written += f.write(b"map system = no\n")?;
    bytes_written += f.write(b"store dos attributes = yes\n")?;
    Ok(bytes_written)
}

fn samba_config_changed(volume_name: &str) -> Result<bool, ::std::io::Error> {
    if Path::new("/etc/samba/smb.conf").exists() {
        // Lets check if the smb.conf matches what we're going to write.  If so then
        // it was already setup and there's nothing to do
        let mut f = File::open("/etc/samba/smb.conf")?;
        let mut existing_config: Vec<u8> = Vec::new();
        f.read_to_end(&mut existing_config)?;
        let mut new_config: Vec<u8> = Vec::new();
        let _ = render_samba_configuration(&mut new_config, volume_name)?;
        if new_config == existing_config {
            // configs are identical
            return Ok(false);
        } else {
            return Ok(true);
        }
    }
    // Config doesn't exist.
    return Ok(true);
}

pub fn setup_samba(volume_name: &str) -> Result<(), String> {
    let cifs_config = juju::config_get("cifs").map_err(|e| e.to_string())?;
    if cifs_config != "True" {
        // Samba isn't enabled
        log!("Samba is not enabled");
        return Ok(());
    }
    if !samba_config_changed(volume_name).map_err(|e| e.to_string())? {
        log!("Samba is already setup.  Not reinstalling");
        return Ok(());
    }

    status_set!(Maintenance "Installing Samba");
    apt_install(vec!["samba"])?;
    status_set!(Maintenance "Configuring Samba");
    log!("Setting up Samba");
    let mut samba_conf = File::create("/etc/samba/smb.conf").map_err(|e| e.to_string())?;
    let bytes_written =
        render_samba_configuration(&mut samba_conf, volume_name).map_err(|e| e.to_string())?;
    log!(format!("Wrote {} bytes to /etc/samba/smb.conf", bytes_written));
    log!("Starting Samba service");
    status_set!(Maintenance "Starting Samba");
    service_start("smbd")?;
    Ok(())
}
