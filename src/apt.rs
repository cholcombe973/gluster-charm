extern crate init_daemon;
extern crate juju;


use std::process::Command;

use log::LogLevel;
use self::init_daemon::{detect_daemon, Daemon};
use super::debian::version::Version;

// Add a ppa source to apt
pub fn add_source(source_string: &str) -> Result<(), String> {
    let mut cmd = Command::new("add-apt-repository");
    cmd.arg("-y");
    cmd.arg(source_string);
    juju::log(&format!("add-apt-repository cmd: {:?}", cmd),
              Some(LogLevel::Debug));
    let output = cmd.output().map_err(|e| e.to_string())?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned());
    }
    return Ok(());
}

// Update the apt database to get the latest packages
pub fn apt_update() -> Result<(), String> {
    let mut cmd = Command::new("apt-get");
    cmd.arg("update");
    cmd.arg("-q");
    let output = cmd.output().map_err(|e| e.to_string())?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned());
    }
    return Ok(());
}

// Install a list of packages
pub fn apt_install(packages: Vec<&str>) -> Result<(), String> {
    let mut cmd = Command::new("apt-get");
    cmd.arg("install");
    cmd.arg("-q");
    cmd.arg("-y");
    for package in packages {
        cmd.arg(package);
    }
    let output = cmd.output().map_err(|e| e.to_string())?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned());
    }
    return Ok(());
}

pub fn service_stop(name: &str) -> Result<(), String> {
    let init_daemon = try!(detect_daemon());
    match init_daemon {
        Daemon::Systemd => {
            let mut cmd = Command::new("systemctl");
            cmd.arg("stop");
            cmd.arg(name);
            let output = cmd.output().map_err(|e| e.to_string())?;
            if !output.status.success() {
                return Err(String::from_utf8_lossy(&output.stderr).into_owned());
            }
            return Ok(());
        }
        Daemon::Upstart => {
            let mut cmd = Command::new("service");
            cmd.arg("stop");
            cmd.arg(name);
            let output = cmd.output().map_err(|e| e.to_string())?;
            if !output.status.success() {
                return Err(String::from_utf8_lossy(&output.stderr).into_owned());
            }
            return Ok(());
        }
        Daemon::Unknown => {
            return Err("Unknown init system.  Cannot stop service".to_string());
        }
    };
}
pub fn service_start(name: &str) -> Result<(), String> {
    let init_daemon = try!(detect_daemon());
    match init_daemon {
        Daemon::Systemd => {
            let mut cmd = Command::new("systemctl");
            cmd.arg("start");
            cmd.arg(name);
            let output = cmd.output().map_err(|e| e.to_string())?;
            if !output.status.success() {
                return Err(String::from_utf8_lossy(&output.stderr).into_owned());
            }
            return Ok(());
        }
        Daemon::Upstart => {
            let mut cmd = Command::new("service");
            cmd.arg("start");
            cmd.arg(name);
            let output = cmd.output().map_err(|e| e.to_string())?;
            if !output.status.success() {
                return Err(String::from_utf8_lossy(&output.stderr).into_owned());
            }
            return Ok(());
        }
        Daemon::Unknown => {
            return Err("Unknown init system.  Cannot start service".to_string());
        }
    };
}
/// Ask apt-cache for the new candidate package that is available
pub fn get_candidate_package_version(package_name: &str) -> Result<Version, String> {
    let mut cmd = Command::new("apt-cache");
    cmd.arg("policy");
    cmd.arg(package_name);
    let output = cmd.output().map_err(|e| e.to_string())?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned());
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    for line in stdout.lines() {
        if line.contains("Candidate") {
            let parts: Vec<&str> = line.split(' ').collect();
            match parts.last() {
                Some(p) => {
                    let version: Version = Version::parse(p).map_err(|e| e.msg)?;
                    return Ok(version);
                }
                None => {
                    return Err(format!("Unknown candidate line format: {:?}", parts));
                }
            }
        }
    }
    Err(format!("Unable to find candidate upgrade package from stdout: {}",
                stdout))
}
