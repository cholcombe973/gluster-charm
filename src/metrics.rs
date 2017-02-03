extern crate juju;
extern crate nix;

use std::path::Path;
use self::nix::sys::statvfs::vfs::Statvfs;
use super::log::LogLevel;

pub fn collect_metrics() -> Result<(), String> {
    let p = Path::new("/mnt/glusterfs");
    let mount_stats = Statvfs::for_path(p).map_err(|e| e.to_string())?;
    // block size * total blocks
    let total_space = mount_stats.f_blocks * mount_stats.f_bsize;
    let free_space = mount_stats.f_bfree * mount_stats.f_bsize;
    let used_space = total_space - free_space;

    juju::log(&format!("Collecting metric gb-used {}", used_space),
              Some(LogLevel::Info));
    juju::add_metric("gb-used", &format!("{}", used_space)).map_err(|e| e.to_string())?;
    Ok(())
}
