// Setup ctdb for high availability NFSv3
extern crate ipnetwork;
extern crate pnet;

use std::io::{Read, Write};
use std::net::IpAddr;
use std::str::FromStr;

use self::ipnetwork::{IpNetworkError, IpNetwork, Ipv4Network, Ipv6Network};
use self::pnet::datalink::{interfaces, NetworkInterface};

#[derive(Debug, Eq, PartialEq)]
pub struct VirtualIp {
    pub cidr: IpNetwork,
    pub interface: String,
}

impl ToString for VirtualIp {
    fn to_string(&self) -> String {
        match self.cidr {
            IpNetwork::V4(v4) => format!("{} {}", v4, self.interface),
            IpNetwork::V6(v6) => format!("{} {}", v6, self.interface),
        }
    }
}

/// Write the ctdb configuration file out to disk
pub fn render_ctdb_configuration<T: Write>(f: &mut T) -> Result<usize, ::std::io::Error> {
    let mut bytes_written = 0;
    bytes_written += f.write(b"CTDB_LOGGING=file:/var/log/ctdb/ctdb.log\n")?;
    bytes_written += f.write(b"CTDB_NODES=/etc/ctdb/nodes\n")?;
    bytes_written += f.write(b"CTDB_PUBLIC_ADDRESSES=/etc/ctdb/public_addresses\n")?;
    bytes_written += f.write(b"CTDB_RECOVERY_LOCK=/mnt/glusterfs/.CTDB-lockfile\n")?;
    Ok(bytes_written)
}

/// Create the public nodes file for ctdb cluster to find all the other peers
/// the cluster Vec should contain all nodes that are participating in the cluster
pub fn render_ctdb_cluster_nodes<T: Write>(f: &mut T,
                                           cluster: &Vec<IpAddr>)
                                           -> Result<usize, ::std::io::Error> {
    let mut bytes_written = 0;
    for node in cluster {
        bytes_written += f.write(&format!("{}\n", node).as_bytes())?;
    }
    Ok(bytes_written)
}

/// Create the public addresses file for ctdb cluster to find all the virtual
/// ip addresses to float across the cluster.
pub fn render_ctdb_public_addresses<T: Write>(f: &mut T,
                                              cluster_networks: &Vec<VirtualIp>)
                                              -> Result<usize, ::std::io::Error> {
    let mut bytes_written = 0;
    for node in cluster_networks {
        bytes_written += f.write(&format!("{}\n", node.to_string()).as_bytes())?;
    }
    Ok(bytes_written)
}
#[test]
fn test_render_ctdb_cluster_nodes() {
    // Test IPV5
    let ctdb_cluster = vec![::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(192, 168, 1, 2)),
                            ::std::net::IpAddr::V4(::std::net::Ipv4Addr::new(192, 168, 1, 3))];
    let expected_result = "192.168.1.2\n192.168.1.3\n";
    let mut buff = ::std::io::Cursor::new(vec![0; 24]);
    render_ctdb_cluster_nodes(&mut buff, &ctdb_cluster).unwrap();
    let result = String::from_utf8_lossy(&buff.into_inner()).into_owned();
    println!("test_render_ctdb_cluster_nodes: \"{}\"", result);
    assert_eq!(expected_result, result);

    // Test IPV6
    let addr1 = ::std::net::Ipv6Addr::from_str("2001:0db8:85a3:0000:0000:8a2e:0370:7334").unwrap();
    let addr2 = ::std::net::Ipv6Addr::from_str("2001:cdba:0000:0000:0000:0000:3257:9652").unwrap();
    let ctdb_cluster = vec![::std::net::IpAddr::V6(addr1), ::std::net::IpAddr::V6(addr2)];
    let expected_result = "2001:db8:85a3::8a2e:370:7334\n2001:cdba::3257:9652\n";
    let mut buff = ::std::io::Cursor::new(vec![0; 49]);
    render_ctdb_cluster_nodes(&mut buff, &ctdb_cluster).unwrap();
    let result = String::from_utf8_lossy(&buff.into_inner()).into_owned();
    println!("test_render_ctdb_cluster_nodes ipv6: \"{}\"", result);
    assert_eq!(expected_result, result);
}

pub fn ipnetwork_from_str(s: &str) -> Result<IpNetwork, String> {
    let v4addr: Result<Ipv4Network, IpNetworkError> = s.parse();
    let v6addr: Result<Ipv6Network, IpNetworkError> = s.parse();

    if v4addr.is_ok() {
        Ok(IpNetwork::V4(v4addr.unwrap()))
    } else if v6addr.is_ok() {
        Ok(IpNetwork::V6(v6addr.unwrap()))
    } else {
        // Failed to parse ip address
        Err(format!("Unable to parse network cidr: {}", s))
    }
}

/// Return all virtual ip cidr networks that are being managed by ctdb
/// located at file f. /etc/ctdb/public_addresses is the usual location
#[allow(dead_code)]
pub fn get_virtual_addrs<T: Read>(f: &mut T) -> Result<Vec<VirtualIp>, String> {
    let mut networks: Vec<VirtualIp> = Vec::new();
    let mut buf = String::new();
    f.read_to_string(&mut buf).map_err(|e| e.to_string())?;
    for line in buf.lines() {
        let parts: Vec<&str> = line.split(" ").collect();
        if parts.len() < 2 {
            return Err(format!("Unable to parse network: {}", line));
        }
        let addr: IpNetwork = ipnetwork_from_str(parts[0])?;
        let interface = parts[1].trim().to_string();

        networks.push(VirtualIp {
            cidr: addr,
            interface: interface,
        });
    }
    Ok(networks)
}

fn get_interface_for_ipv4_address(cidr_address: Ipv4Network,
                                  interfaces: Vec<NetworkInterface>)
                                  -> Option<String> {
    // Loop through every interface
    for iface in interfaces {
        // Loop through every ip address the interface is serving
        if let Some(ip_addrs) = iface.ips {
            for iface_ip in ip_addrs {
                match iface_ip {
                    IpAddr::V4(v4_addr) => {
                        if cidr_address.contains(v4_addr) {
                            return Some(iface.name);
                        } else {
                            // No match
                            continue;
                        }
                    }
                    _ => {
                        // It's a ipv6 address.  Can't match against ipv4
                        continue;
                    }
                };
            }
        }
    }
    None
}
#[test]
fn test_get_interfaces_for_ipv4_address() {
    let addr: Ipv4Network = "192.168.1.200/24".parse().unwrap();
    let addr1 = ::std::net::Ipv4Addr::from_str("192.168.1.2").unwrap();
    let addr2 = ::std::net::Ipv4Addr::from_str("192.168.2.2").unwrap();
    let interfaces = vec![NetworkInterface {
                              name: "eth0".to_string(),
                              index: 0,
                              mac: None,
                              ips: Some(vec![::std::net::IpAddr::V4(addr1)]),
                              flags: 0,
                          },
                          NetworkInterface {
                              name: "eth1".to_string(),
                              index: 1,
                              mac: None,
                              ips: Some(vec![::std::net::IpAddr::V4(addr2)]),
                              flags: 0,
                          }];
    let result = get_interface_for_ipv4_address(addr, interfaces);
    println!("get_interface_for_ipv4_address: {:?}", result);
    assert_eq!(Some("eth0".to_string()), result);
}

fn get_interface_for_ipv6_address(cidr_address: Ipv6Network,
                                  interfaces: Vec<NetworkInterface>)
                                  -> Option<String> {
    // Loop through every interface
    for iface in interfaces {
        // Loop through every ip address the interface is serving
        if let Some(ip_addrs) = iface.ips {
            for iface_ip in ip_addrs {
                match iface_ip {
                    IpAddr::V6(v6_addr) => {
                        if cidr_address.contains(v6_addr) {
                            return Some(iface.name);
                        } else {
                            // No match
                            continue;
                        }
                    }
                    _ => {
                        // It's a ipv4 address.  Can't match against ipv6
                        continue;
                    }
                };
            }
        }
    }
    None
}

#[test]
fn test_get_interfaces_for_ipv6_address() {
    let addr: Ipv6Network = "2001:0db8:85a3:0000:0000:8a2e:0370:7334/120".parse().unwrap();
    let addr1 = ::std::net::Ipv6Addr::from_str("2001:db8:85a3:0:0:8a2e:370:7300").unwrap();
    let addr2 = ::std::net::Ipv6Addr::from_str("fd36:d456:3a78::").unwrap();
    let interfaces = vec![NetworkInterface {
                              name: "eth0".to_string(),
                              index: 0,
                              mac: None,
                              ips: Some(vec![::std::net::IpAddr::V6(addr1)]),
                              flags: 0,
                          },
                          NetworkInterface {
                              name: "eth1".to_string(),
                              index: 1,
                              mac: None,
                              ips: Some(vec![::std::net::IpAddr::V6(addr2)]),
                              flags: 0,
                          }];
    let result = get_interface_for_ipv6_address(addr, interfaces);
    println!("get_interface_for_ipv6_address: {:?}", result);
    assert_eq!(Some("eth0".to_string()), result);
}

/// Return the network interface that serves the subnet for this ip address
pub fn get_interface_for_address(cidr_address: IpNetwork) -> Option<String> {
    let interfaces = interfaces();
    match cidr_address {
        IpNetwork::V4(v4_addr) => get_interface_for_ipv4_address(v4_addr, interfaces),
        IpNetwork::V6(v6_addr) => get_interface_for_ipv6_address(v6_addr, interfaces),
    }
}

/// Constructs a new `IpNetwork` from a given &str with a prefix denoting the
/// network size.  If the prefix is larger than 32 (for IPv4) or 128 (for IPv6), this
/// will raise an `IpNetworkError::InvalidPrefix` error.
#[allow(dead_code)]
pub fn parse_ipnetwork(s: &str) -> Result<IpNetwork, IpNetworkError> {
    let v4addr: Result<Ipv4Network, IpNetworkError> = s.parse();
    let v6addr: Result<Ipv6Network, IpNetworkError> = s.parse();

    if v4addr.is_ok() {
        Ok(IpNetwork::V4(v4addr.unwrap()))
    } else if v6addr.is_ok() {
        Ok(IpNetwork::V6(v6addr.unwrap()))
    } else {
        Err(IpNetworkError::InvalidAddr(s.to_string()))
    }
}

#[test]
fn test_parse_virtual_addrs() {
    let test_str = "10.0.0.6/24 eth2\n10.0.0.7/24 eth2".as_bytes();
    let mut c = ::std::io::Cursor::new(&test_str);
    let result = get_virtual_addrs(&mut c).unwrap();
    println!("test_parse_virtual_addrs: {:?}", result);
    let expected =
        vec![VirtualIp {
                 cidr: IpNetwork::V4(Ipv4Network::new(::std::net::Ipv4Addr::new(10, 0, 0, 6), 24)
                     .unwrap()),
                 interface: "eth2".to_string(),
             },
             VirtualIp {
                 cidr: IpNetwork::V4(Ipv4Network::new(::std::net::Ipv4Addr::new(10, 0, 0, 7), 24)
                     .unwrap()),
                 interface: "eth2".to_string(),
             }];
    assert_eq!(expected, result);
}

#[test]
fn test_parse_virtual_addrs_v6() {
    let test_str = "2001:0db8:85a3:0000:0000:8a2e:0370:7334/24 \
                    eth2\n2001:cdba:0000:0000:0000:0000:3257:9652/24 eth2"
        .as_bytes();
    let mut c = ::std::io::Cursor::new(&test_str);
    let result = get_virtual_addrs(&mut c).unwrap();
    println!("test_get_virtual_addrs: {:?}", result);
    let addr1 = ::std::net::Ipv6Addr::from_str("2001:0db8:85a3:0000:0000:8a2e:0370:7334").unwrap();
    let addr2 = ::std::net::Ipv6Addr::from_str("2001:cdba:0000:0000:0000:0000:3257:9652").unwrap();
    let expected = vec![VirtualIp {
                            cidr: IpNetwork::V6(Ipv6Network::new(addr1, 24).unwrap()),
                            interface: "eth2".to_string(),
                        },
                        VirtualIp {
                            cidr: IpNetwork::V6(Ipv6Network::new(addr2, 24).unwrap()),
                            interface: "eth2".to_string(),
                        }];
    assert_eq!(expected, result);
}

/// Return all ctdb nodes that are contained in the file f
/// /etc/ctdb/nodes is the usual location
#[allow(dead_code)]
pub fn get_ctdb_nodes<T: Read>(f: &mut T) -> Result<Vec<IpAddr>, String> {
    let mut addrs: Vec<IpAddr> = Vec::new();
    let mut buf = String::new();
    f.read_to_string(&mut buf).map_err(|e| e.to_string())?;
    for line in buf.lines() {
        let addr = IpAddr::from_str(line).map_err(|e| e.to_string())?;
        addrs.push(addr);
    }
    Ok(addrs)
}

#[test]
fn test_get_ctdb_nodes() {
    let test_str = "10.0.0.1\n10.0.0.2".as_bytes();
    let mut c = ::std::io::Cursor::new(&test_str);
    let result = get_ctdb_nodes(&mut c).unwrap();
    println!("test_get_ctdb_nodes: {:?}", result);
    let addr1 = ::std::net::Ipv4Addr::new(10, 0, 0, 1);
    let addr2 = ::std::net::Ipv4Addr::new(10, 0, 0, 2);
    let expected = vec![IpAddr::V4(addr1), IpAddr::V4(addr2)];
    assert_eq!(expected, result);
}

#[test]
fn test_get_ctdb_nodes_v6() {
    let test_str = "2001:0db8:85a3:0000:0000:8a2e:0370:7334\n2001:cdba:0000:0000:0000:0000:3257:\
                    9652"
        .as_bytes();
    let mut c = ::std::io::Cursor::new(&test_str);
    let result = get_ctdb_nodes(&mut c).unwrap();
    println!("test_get_ctdb_nodes_v6: {:?}", result);
    let addr1 = ::std::net::Ipv6Addr::from_str("2001:0db8:85a3:0000:0000:8a2e:0370:7334").unwrap();
    let addr2 = ::std::net::Ipv6Addr::from_str("2001:cdba:0000:0000:0000:0000:3257:9652").unwrap();
    let expected = vec![IpAddr::V6(addr1), IpAddr::V6(addr2)];
    assert_eq!(expected, result);
}
