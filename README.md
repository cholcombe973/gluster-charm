# Gluster charm[![Build Status](https://travis-ci.org/cholcombe973/gluster-charm.svg?branch=master)](https://travis-ci.org/cholcombe973/gluster-charm)

GlusterFS is an open source, distributed file system capable of scaling
to several petabytes (actually, 72 brontobytes!) and handling thousands
of clients. GlusterFS clusters together storage building blocks over
Infiniband RDMA or TCP/IP interconnect, aggregating disk and memory
resources and managing data in a single global namespace. GlusterFS
is based on a stackable user space design and can deliver exceptional
performance for diverse workloads.

# Usage

The gluster charm has defaults in the config.yaml that you will want to change for production.
Please note that volume_name, cluster_type, and replication_level are immutable options.  Changing them post
deployment will have no effect.  
This charm makes use of [juju storage](https://jujucharms.com/docs/1.25/storage).  Please read the docs to learn about adding block storage to your units.

    volume_name:
        Whatever name you would like to call your gluster volume.
    cluster_type:
        The default here is Replicate but you can also set it to
         * Distribute
         * Stripe
         * Replicate
         * StripedAndReplicate
         * Disperse
         * DistributedAndStripe
         * DistributedAndReplicate
         * DistributedAndStripedAndReplicate
         * DistributedAndDisperse
    replication_level:
        The default here is 2
        If you don't know what any of these mean don't worry about it. The defaults are sane.

# Actions
This charm several actions to help manage your Gluster cluster.

1. Creating volume quotes. Example:
`juju action do --unit gluster/0 create-volume-quota volume=test usage-limit=1000MB`
2. Deleting volume quotas. Example:
`juju action do --unit gluster/0 delete-volume-quota volume=test`
3. Listing the current volume quotas.  Example:
`juju action do --unit gluster/0 list-volume-quotas volume=test`
4. Setting volume options.  This can be used to set several volume options at
once.  Example:
`juju action do --unit gluster/0 set-volume-options volume=test performance-cache-size=1GB performance-write-behind-window-size=1MB`

# Building from Source
The charm comes packaged with an already built binary in ./hooks/main which is built for x86-64.
A rebuild would be required for other architectures.

1. Install [rust](http://www.rust-lang.org/install.html) stable for your platform
2. Install [cargo](https://crates.io/install)
3. Install libudev-dev as a dependency.
4. cd into the charm directory and run:

        cargo build --release

5. Copy the built target:

        cp target/release/main hooks/main

If you would like debug flags enabled rebuild with: cargo build and cp target/debug/main hooks/main

That should provide you with a binary.  

# Configure
Create a config.yaml file to set any options you would like to change from the defaults.

# Deploy
This charm requires juju storage. It requires at least 1 block device.
For more information please check out the [docs](https://jujucharms.com/docs/1.25/storage)

    Example EC2 deployment on Juju 1.25:
    juju deploy cs:~xfactor973/xenial/gluster-3 -n 3 --config=~/gluster.yaml --storage brick=ebs,10G,2

    To scale out the service use this command:
    juju add-unit gluster

(keep adding units to keep adding more bricks and storage)

# Scale Out
Note that during scale out operation if your cluster has existing files on there they will not
be migrated to the new bricks until a gluster volume rebalance start operation is performed.
This operation can slow client traffic so it is left up to the administrator to perform
at the appropriate time.

# Rolling Upgrades
The config.yaml source option is used to kick off a rolling upgrade of your cluster.
The current behavior is to install the new packages on the server and upgrade it one by
one.  A UUID sorted order is used to define the upgrade order.  Please note that replica 3
is required to use rolling upgrades.  With replica 2 it's possible to have split brain issues.

# Testing
For a simple test deploy 4 gluster units like so

    juju deploy gluster -n 4 --config=~/gluster.yaml --storage brick=local,10G

Once the status is started the charm will bring both units together into a cluster and create a volume.  
You will know the cluster is ready when you see a status of active.

Now you can mount the exported GlusterFS filesystem with either fuse or NFS.  Fuse has the advantage of
knowing how to talk to all replicas in your Gluster cluster so it will not need other high availablity
software.  NFSv3 is point to point so it will need something like virtual IP's, DNS round robin or
something else to ensure availability if a unit should die or go away suddenly.
Install the glusterfs-client package on your host.  You can reference the ./hooks/install file to
show you how to install the glusterfs packages.

On your juju host you can mount Gluster with fuse like so:

    mount -t glusterfs <ip or hostname of unit>:/<volume_name> mount_point/

## High Availability
There's 3 ways you can achieve high availability with Gluster.  

1. The first an easiest method is to simply use the glusterfs fuse mount on all
clients.  This has the advantage of knowing where all servers in the cluster
are at and will reconnect as needed and failover gracefully.
2. Using virtual ip addresses with a DNS round robin A record.  This solution
applies to NFSv3.  This method is more complicated but has the advantage of
being usable on clients that only support NFSv3.  NFSv3 is stateless and
this can be used to your advantage by floating virtual ip addresses that
failover quickly.  To use this setting please set the virtual_ip_addresses
config.yaml setting after reading the usage.
3. Using the [Gluster coreutils](https://github.com/gluster/glusterfs-coreutils).  
If you do not need a mount point then this is a viable option.  
glusterfs-coreutils provides a set of basic utilities such as cat, cp, flock,
ls, mkdir, rm, stat and tail that are implemented specifically using the
GlusterFS API commonly known as libgfapi. These utilities can be used either
inside a gluster remote shell or as standalone commands with 'gf' prepended to
their respective base names. Example usage is shown here:
[Docs](https://gluster.readthedocs.io/en/latest/Administrator%20Guide/GlusterFS%20Coreutils/)

## MultiTenancy

Gluster provides a few easy ways to have multiple clients in the same volume
without them knowing about one another.  
1. Deep Mounting.  Gluster NFS supports deep mounting which allows the sysadmin
to create a top level directory for each client.  Then instead of mounting the
volume you mount the volume + the directory name.  Now the client only sees
their files.  This doesn't stop a malacious client from remounting the top
level directory.  
  * This can be combined with [posix acl's](https://gluster.readthedocs.io/en/latest/Administrator%20Guide/Access%20Control%20Lists/) if your tenants are not trustworthy.
  * Another option is combining with [Netgroups](https://gluster.readthedocs.io/en/latest/Administrator%20Guide/Export%20And%20Netgroup%20Authentication/).
  This feature allows users to restrict access specific IPs
  (exports authentication) or a netgroup (netgroups authentication),
  or a combination of both for both Gluster volumes and subdirectories within
  Gluster volumes.

## Filesystem Support:
The charm supports several filesystems currently.  Btrfs, Ext4, Xfs and ZFS. The
default filesystem can be set in the config.yaml.  The charm currently defaults
to XFS but ZFS would likely be a safe choice and enable advanced functionality
such as bcache backed gluster bricks. 
**Note: The ZFS filesystem requires Ubuntu 16.04 or greater**

## Notes:
If you're using containers to test Gluster you might need to edit /etc/default/lxc-net
and read the last section about if you want lxcbr0's dnsmasq to resolve the .lxc domain

Now to show that your cluster can handle failure you can:

    juju destroy-machine n;

This will remove one of the units from your cluster and simulate a hard failure.  List your files
on the mount point to show that they are still available.  

# Reference
For more information about Gluster and operation of a cluster please see: https://gluster.readthedocs.org/en/latest/
For more immediate and interactive help please join IRC channel #gluster on Freenode.
Gluster also has a users mailing list: https://www.gluster.org/mailman/listinfo/gluster-users
For bugs concerning the Juju charm please file them on [Github](https://github.com/cholcombe973/gluster-charm/tree/master)
