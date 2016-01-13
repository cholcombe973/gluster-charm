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

    brick_paths:
        The directories that will be used for a storage device.  Does not have to
        be a real hard drive but probably should be for anything production related.
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

# Building from Source
The charm comes packaged with an already built binary in ./hooks/main which is built for x86-64.
A rebuild would be required for other architectures.

1. Install [rust](http://www.rust-lang.org/install.html) stable for your platform
2. Install [cargo](https://crates.io/install)
3. cd into the charm directory and run:

        cargo build --release

4. cp target/release/main hooks/main

If you would like debug flags enabled rebuild with: cargo build and cp target/debug/main hooks/main

That should provide you with a binary.  

# Configure
Edit the config.yaml file in the charm's root directory if needed.

# Deploy

    juju deploy gluster
    juju add-unit gluster

(keep adding units to keep adding more bricks and storage)

# Testing
For a simple test deploy 4 gluster units like so

    juju deploy gluster -n 4

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
For bugs concerning the Juju charm please file them on launchpad: https://launchpad.net/
