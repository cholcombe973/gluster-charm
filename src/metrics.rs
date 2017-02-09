extern crate juju;
extern crate nix;

use std::path::Path;
use self::nix::sys::statvfs::vfs::Statvfs;

pub fn collect_metrics() -> Result<(), String> {
    let p = Path::new("/mnt/glusterfs");
    let mount_stats = Statvfs::for_path(p).map_err(|e| e.to_string())?;
    // block size * total blocks
    let total_space = mount_stats.f_blocks * mount_stats.f_bsize;
    let free_space = mount_stats.f_bfree * mount_stats.f_bsize;
    // capsize only operates on i64 values
    let used_space = total_space - free_space;
    let gb_used = used_space / 1024 / 1024 / 1024;

    log!(format!("Collecting metric gb-used {}", gb_used), Info);
    juju::add_metric("gb-used", &format!("{}", gb_used)).map_err(|e| e.to_string())?;
    Ok(())
}
