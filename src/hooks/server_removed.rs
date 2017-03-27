extern crate juju;

pub fn server_removed() -> Result<(), String> {
    let private_address = juju::unit_get_private_addr().map_err(|e| e.to_string())?;
    log!(format!("Removing server: {}", private_address), Info);
    return Ok(());
}
