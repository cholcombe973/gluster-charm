use gluster;
use gluster::{BitrotOption, ScrubAggression, ScrubSchedule, ScrubControl};
use gluster::volume::{quota_list, volume_add_quota, volume_disable_bitrot, volume_enable_bitrot,
                      volume_enable_quotas, volume_quotas_enabled, volume_remove_quota,
                      volume_set_bitrot_option, volume_set_options};
use juju;

use std::path::PathBuf;
use std::str::FromStr;

fn action_get(key: &str) -> Result<String, String> {
    let value = match juju::action_get(key) {
            Ok(v) => v,
            Err(e) => {
                // Notify the user of the failure and then return the error up the stack
                juju::action_fail(&e.to_string()).map_err(|e| e.to_string())?;
                return Err(e.to_string());
            }
        }
        .unwrap();
    Ok(value)
}

pub fn enable_bitrot_scan() -> Result<(), String> {
    let vol = action_get("volume")?;
    match volume_enable_bitrot(&vol) {
        Err(e) => {
            juju::action_fail(&e.to_string()).map_err(|e| e.to_string())?;
            Err(e.to_string())
        }
        Ok(_) => Ok(()),
    }
}
pub fn disable_bitrot_scan() -> Result<(), String> {
    let vol = action_get("volume")?;
    match volume_disable_bitrot(&vol) {
        Err(e) => {
            juju::action_fail(&e.to_string()).map_err(|e| e.to_string())?;
            Err(e.to_string())
        }
        Ok(_) => Ok(()),
    }
}
pub fn pause_bitrot_scan() -> Result<(), String> {
    let vol = action_get("volume")?;
    let option = BitrotOption::Scrub(ScrubControl::Pause);
    match volume_set_bitrot_option(&vol, option) {
        Err(e) => {
            juju::action_fail(&e.to_string()).map_err(|e| e.to_string())?;
            Err(e.to_string())
        }
        Ok(_) => Ok(()),
    }
}
pub fn resume_bitrot_scan() -> Result<(), String> {
    let vol = action_get("volume")?;
    let option = BitrotOption::Scrub(ScrubControl::Resume);
    match volume_set_bitrot_option(&vol, option) {
        Err(e) => {
            juju::action_fail(&e.to_string()).map_err(|e| e.to_string())?;
            Err(e.to_string())
        }
        Ok(_) => Ok(()),
    }
}
pub fn set_bitrot_scan_frequency() -> Result<(), String> {
    let vol = action_get("volume")?;
    let frequency = action_get("frequency")?;
    let option = ScrubSchedule::from_str(&frequency);
    match volume_set_bitrot_option(&vol, BitrotOption::ScrubFrequency(option)) {
        Err(e) => {
            juju::action_fail(&e.to_string()).map_err(|e| e.to_string())?;
            Err(e.to_string())
        }
        Ok(_) => Ok(()),
    }
}
pub fn set_bitrot_throttle() -> Result<(), String> {
    let vol = action_get("volume")?;
    let throttle = action_get("throttle")?;
    let option = ScrubAggression::from_str(&throttle);
    match volume_set_bitrot_option(&vol, BitrotOption::ScrubThrottle(option)) {
        Err(e) => {
            juju::action_fail(&e.to_string()).map_err(|e| e.to_string())?;
            Err(e.to_string())
        }
        Ok(_) => Ok(()),
    }
}

pub fn enable_volume_quota() -> Result<(), String> {
    // Gather our action parameters
    let volume = action_get("volume")?;
    let usage_limit = action_get("usage-limit")?;
    let parsed_usage_limit = u64::from_str(&usage_limit).map_err(|e| e.to_string())?;
    let path = action_get("path")?;
    // Turn quotas on if not already enabled
    let quotas_enabled = volume_quotas_enabled(&volume).map_err(|e| e.to_string())?;
    if !quotas_enabled {
        volume_enable_quotas(&volume).map_err(|e| e.to_string())?;
    }

    volume_add_quota(&volume, PathBuf::from(path), parsed_usage_limit).map_err(|e| e.to_string())?;
    Ok(())
}

pub fn disable_volume_quota() -> Result<(), String> {
    // Gather our action parameters
    let volume = action_get("volume")?;
    let path = action_get("path")?;

    let quotas_enabled = volume_quotas_enabled(&volume).map_err(|e| e.to_string())?;
    if quotas_enabled {
        match volume_remove_quota(&volume, PathBuf::from(path)) {
            Ok(_) => return Ok(()),
            Err(e) => {
                // Notify the user of the failure and then return the error up the stack
                juju::action_fail(&e.to_string()).map_err(|e| e.to_string())?;
                return Err(e.to_string());
            }
        }
    } else {
        return Ok(());
    }
}

pub fn list_volume_quotas() -> Result<(), String> {
    // Gather our action parameters
    let volume = action_get("volume")?;
    let quotas_enabled = volume_quotas_enabled(&volume).map_err(|e| e.to_string())?;
    if quotas_enabled {
        match quota_list(&volume) {
            Ok(quotas) => {
                let quota_string: Vec<String> = quotas.iter()
                    .map(|quota| {
                             format!("path: {:?} limit: {} used: {}",
                                     quota.path,
                                     quota.limit,
                                     quota.used)
                         })
                    .collect();
                juju::action_set("quotas", &quota_string.join("\n")).map_err(|e| e.to_string())?;
                return Ok(());
            }
            Err(e) => {
                log!(&format!("Quota list failed: {:?}", e), Error);
                return Err(e.to_string());
            }
        }
    } else {
        log!(format!("Quotas are disabled on volume: {}", volume));
        return Ok(());
    }
}

pub fn set_volume_options() -> Result<(), String> {
    // volume is a required parameter so this should be safe
    let mut volume: String = String::new();

    // Gather all of the action parameters up at once.  We don't know what
    // the user wants to change.
    let options = juju::action_get_all().map_err(|e| e.to_string())?;
    let mut settings: Vec<gluster::GlusterOption> = Vec::new();
    for (key, value) in options {
        if key != "volume" {
            settings.push(try!(gluster::GlusterOption::from_str(&key, value)
                .map_err(|e| e.to_string())));
        } else {
            volume = value;
        }
    }
    volume_set_options(&volume, settings).map_err(|e| e.to_string())?;
    return Ok(());
}
