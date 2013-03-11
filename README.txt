-----------------------------------------------------------------------------
HttpFS backport for cdh3u6 - Hadoop HDFS over HTTP

The HttpFS source for this backport has been taken from the following
Apache Hadoop Subversion branch@revision:

  https://svn.apache.org/repos/asf/hadoop/common/trunk@1363175

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

  Clone this Git repository. Checkout the cdh3u5 branch.

  Run 'mvn package -Pdist'.

  The resulting TARBALL will under the 'target/' directory.

How to install:

  Expand the build TARBALL.

  Follow the setup instructions:

    http://cloudera.github.com/httpfs/

-----------------------------------------------------------------------------
