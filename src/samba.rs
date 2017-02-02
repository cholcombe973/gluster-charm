use std::fs::File;
use std::io::Write;

use super::apt::{apt_install, service_start};
use super::juju;
use super::log::LogLevel;

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

pub fn setup_samba(volume_name: &str) -> Result<(), String> {
    let cifs_config = juju::config_get("cifs").map_err(|e| e.to_string())?;
    if cifs_config != "True" {
        // Samba isn't enabled
        juju::log("Samba is not enabled", Some(LogLevel::Debug));
        return Ok(());
    }
    status_set!(Maintenance "Installing Samba");
    apt_install(vec!["samba"])?;
    status_set!(Maintenance "Configuring Samba");
    juju::log("Setting up Samba", Some(LogLevel::Debug));
    let mut samba_conf = File::create("/etc/samba/smb.conf").map_err(|e| e.to_string())?;
    let bytes_written =
        render_samba_configuration(&mut samba_conf, volume_name).map_err(|e| e.to_string())?;
    juju::log(&format!("Wrote {} bytes to /etc/samba/smb.conf", bytes_written),
              Some(LogLevel::Debug));
    juju::log("Starting Samba service", Some(LogLevel::Debug));
    status_set!(Maintenance "Starting Samba");
    service_start("smbd")?;
    Ok(())
}
