extern crate gluster;
extern crate juju;
extern crate log;
extern crate openssl;

use std::fs::File;
use std::io::{Read, Write};

use log::LogLevel;
use super::gluster::{GlusterError, GlusterOption, volume_set_options};
// use self::openssl::rsa::Rsa;
use self::openssl::error::ErrorStack;

use self::openssl::hash::MessageDigest;
use self::openssl::pkey::PKey;
use self::openssl::rsa::Rsa;
use self::openssl::x509::X509Generator;
use self::openssl::x509::extension::{Extension, KeyUsageOption};

// save keys to:
//  /etc/ssl/glusterfs.pem X's own certificate
//  /etc/ssl/glusterfs.key X's private key
//  /etc/ssl/glusterfs.ca concatenation of others' certificates
//
// Enable TLS on the IO path
// gluster volume set MYVOLUME client.ssl on
// gluster volume set MYVOLUME server.ssl on


// Generate the public/private key pair
// Returns a (public key, private key) tuple
pub fn generate_keypair(keysize: u32) -> Result<(Vec<u8>, Vec<u8>), ErrorStack> {
    let rsa = Rsa::generate(keysize)?;
    let pkey = PKey::from_rsa(rsa)?;

    let mut f = File::open("/etc/hostname").unwrap();
    let mut hostname = String::new();
    f.read_to_string(&mut hostname).unwrap();

    // `openssl req -new -x509 -key /etc/ssl/glusterfs.key -subj "/CN=<server-hostname>"
    // `-out /etc/ssl/glusterfs.pem`
    //
    let gen = X509Generator::new()
        .set_valid_period(365 * 2)
        .add_name("CN".to_owned(), hostname)
        .set_sign_hash(MessageDigest::sha256())
        .add_extension(Extension::KeyUsage(vec![KeyUsageOption::DigitalSignature]));

    let cert = gen.sign(&pkey)?;
    let cert_pem = cert.to_pem()?;
    let pkey_pem = pkey.private_key_to_pem()?;

    // let keypair = Rsa::generate(keysize)?;
    // let private = keypair.private_key_to_pem()?;
    // let public = keypair.public_key_to_pem()?;

    Ok((cert_pem, pkey_pem))
}

// Take the public and private keys and save them to disk where gluster can find them
pub fn save_keys(public_key: &[u8], private_key: &[u8]) -> Result<(), ::std::io::Error> {
    juju::log("Creating /etc/ssl/glusterfs.pem file",
              Some(LogLevel::Debug));
    let mut pem = File::create("/etc/ssl/glusterfs.pem")?;
    pem.write(&public_key)?;

    juju::log("Creating /etc/ssl/glusterfs.key file",
              Some(LogLevel::Debug));
    let mut key = File::create("/etc/ssl/glusterfs.key")?;
    key.write(&private_key)?;

    juju::log("Creating /etc/ssl/glusterfs.ca file", Some(LogLevel::Debug));
    let mut ca = File::create("/etc/ssl/glusterfs.ca")?;
    ca.write(&public_key)?;

    Ok(())
}

// Enable client and server side encryption
pub fn enable_io_encryption(volume: &str) -> Result<(), GlusterError> {
    let mut settings: Vec<GlusterOption> = Vec::new();
    settings.push(GlusterOption::from_str("client.ssl", "on".to_string())?);
    settings.push(GlusterOption::from_str("server.ssl", "on".to_string())?);
    volume_set_options(&volume, settings)?;
    Ok(())
}
