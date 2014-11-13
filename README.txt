-----------------------------------------------------------------------------
HttpFS backport for httpfs-mapr - Hadoop HDFS over HTTP


HttpFS is a server that provides a REST HTTP gateway to HDFS with full
filesystem read & write capabilities.

HttpFS can be used to transfer data between clusters running different
versions of Hadoop (overcoming RPC versioning issues), for example using
Hadoop DistCP.

HttpFS can be used to access data in HDFS on a cluster behind of a firewall
(the HttpFS server acts as a gateway and is the only system that is allowed
to cross the firewall into the cluster).

HttpFS can be used to access data in HDFS using HTTP utilities (such as curl
and wget) and HTTP libraries Perl from other languages than Java.

Requirements:

  * Unix OS
  * JDK 1.6.*
  * Maven 3.*

How to build:

  Clone this Git repository. Checkout the httpfs-mapr branch.

  Run 'mvn package -Pdist'.

  The resulting TARBALL will under the 'target/' directory.

How to install:

  Expand the build TARBALL.

  Follow the setup instructions:

    http://hadoop.apache.org/docs/r2.2.0/hadoop-hdfs-httpfs/index.html

-----------------------------------------------------------------------------