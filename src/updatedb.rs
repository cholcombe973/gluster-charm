use std::fs::File;
use std::io::{Error, Read, Write};
use std::path::Path;

/*
NOTE: Remove any bricks and also the fuse mountpoint from updatedb

 */
// Ported from host.py in charmhelpers.core.host
pub fn add_to_prunepath(path: &String, updatedb_path: &Path) -> Result<(), Error> {
    let mut f = File::open(updatedb_path)?;
    let mut buff = String::new();
    f.read_to_string(&mut buff)?;
    let output = updatedb(buff, path);

    // Truncate the file
    let mut f = File::create(updatedb_path)?;
    f.write_all(&output.as_bytes())?;
    Ok(())
}

#[test]
fn test_updatedb() {
    let expected_result = r#"PRUNE_BIND_MOUNTS="yes"
# PRUNENAMES=".git .bzr .hg .svn"
PRUNEPATHS="/tmp /var/spool /media /home/.ecryptfs /var/lib/schroot /mnt/xvdb"
PRUNEFS="NFS nfs nfs4 rpc_pipefs afs binfmt_misc proc smbfs autofs"
"#;
    let update_conf = r#"PRUNE_BIND_MOUNTS="yes"
# PRUNENAMES=".git .bzr .hg .svn"
PRUNEPATHS="/tmp /var/spool /media /home/.ecryptfs /var/lib/schroot"
PRUNEFS="NFS nfs nfs4 rpc_pipefs afs binfmt_misc proc smbfs autofs"
"#;
    let new_path = "/mnt/xvdb";
    let result = updatedb(update_conf.to_string(), &new_path.to_string());

    println!("Result: {}", result);
    assert_eq!(result, expected_result);

    // Test that it isn't added again
    let new_path = "/mnt/xvdb";
    let result = updatedb(expected_result.to_string(), &new_path.to_string());
    assert_eq!(result, expected_result);
}

// Ported from host.py in charmhelpers.core.host
fn updatedb(updatedb: String, new_path: &String) -> String {
    // PRUNEPATHS="/tmp /var/spool /media /home/.ecryptfs /var/lib/schroot"
    let mut output = String::new();
    for line in updatedb.lines() {
        if line.starts_with("PRUNEPATHS=") {
            let mut paths: Vec<String> = line.replace("\"", "")
                .replace("PRUNEPATHS=", "")
                .split(" ")
                .map(|e| e.to_string())
                .collect();
            if !paths.contains(&new_path) {
                paths.push(new_path.clone());
            }
            output.push_str(&format!("PRUNEPATHS=\"{}\"\n", paths.join(" ")));
        } else {
            output.push_str(&format!("{}\n", line));
        }
    }
    output
}
