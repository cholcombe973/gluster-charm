extern crate juju;

use std::path::PathBuf;
use super::super::initialize_storage;

pub fn brick_attached() -> Result<(), String> {
    //let brick_location = juju::storage_get_location().map_err(|e| e.to_string())?;
    //let brick_path = PathBuf::from(brick_location.trim());

    // Format our bricks and mount them
    //initialize_storage(&brick_path)?;
    //Ok(())

    // Do nothing for now.  We will initialize this block device later in parallel
    return Ok(());
}
