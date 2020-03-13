#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

#file client impersonation
MAPR_IMPERSONATION_ENABLED="True"
export MAPR_IMPERSONATION_ENABLED

#gives access to MAPR_ECOSYSTEM_LOGIN_OPTS
export BASEMAPR=${MAPR_HOME:-/opt/mapr}
env=${BASEMAPR}/conf/env.sh
[ -f $env ] && . $env

# resolve links - $0 may be a softlink
PRG="${0}"

while [ -h "${PRG}" ]; do
  ls=`ls -ld "${PRG}"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "${PRG}"`/"$link"
  fi
done

BASEDIR=`dirname ${PRG}`
BASEDIR=`cd ${BASEDIR}/..;pwd`

source ${HADOOP_LIBEXEC_DIR:-${BASEDIR}/libexec}/httpfs-config.sh

# The Java System property 'httpfs.http.port' it is not used by HttpFS,
# it is used in Tomcat's server.xml configuration file
#
export CATALINA_OPTS="${CATALINA_OPTS} -Djava.library.path=/opt/mapr/lib -Dhttpfs.proxyuser.mapred.skip.reduce.max.skip.hosts=0 -Dhttpfs.proxyuser.mapred.skip.reduce.max.skip.groups=0"

catalina_opts="-Dhttpfs.home.dir=${HTTPFS_HOME}";
catalina_opts="${catalina_opts} -Dhttpfs.config.dir=${HTTPFS_CONFIG}";
catalina_opts="${catalina_opts} -Dhttpfs.log.dir=${HTTPFS_LOG}";
catalina_opts="${catalina_opts} -Dhttpfs.temp.dir=${HTTPFS_TEMP}";
catalina_opts="${catalina_opts} -Dhttpfs.admin.port=${HTTPFS_ADMIN_PORT}";
catalina_opts="${catalina_opts} -Dhttpfs.http.port=${HTTPFS_HTTP_PORT}";
catalina_opts="${catalina_opts} -Dhttpfs.http.hostname=${HTTPFS_HTTP_HOSTNAME}";
catalina_opts="${catalina_opts} -Dhttpfs.sslEnabledProtocols=${HTTPFS_SSL_ENABLED_PROTOCOL}";

if [ -f "${HTTPFS_HOME}"/etc/hadoop/isSecure ] ; then
  if grep --quiet  secure=true "${HTTPFS_HOME}"/etc/hadoop/isSecure; then
    catalina_opts="${catalina_opts} -Dhttpfs.hadoop.authentication.type=multiauth";
    catalina_opts="${catalina_opts} -Dhttpfs.authentication.type=multiauth";
  fi
else
  if grep --quiet  secure=true $MAPR_HOME/conf/mapr-clusters.conf; then
    catalina_opts="${catalina_opts} -Dhttpfs.hadoop.authentication.type=multiauth";
    catalina_opts="${catalina_opts} -Dhttpfs.authentication.type=multiauth";
  fi
fi

print "Adding to CATALINA_OPTS:     ${catalina_opts}"
#DEBUG_OPTS="-Xdebug -Xrunjdwp:server=y,transport=dt_socket,address=14001,suspend=y"

export CATALINA_OPTS="${CATALINA_OPTS} ${catalina_opts} ${MAPR_ECOSYSTEM_LOGIN_OPTS}"
print "Using   CATALINA_OPTS:       ${CATALINA_OPTS}"

# A bug in catalina.sh script does not use CATALINA_OPTS for stopping the server
#
if [ "${1}" = "stop" ]; then
  export MAPR_HOME
  export JAVA_OPTS=${CATALINA_OPTS}
fi

if [ "${HTTPFS_SILENT}" != "true" ]; then
  exec ${HTTPFS_CATALINA_HOME}/bin/catalina.sh "$@"
else
  exec ${HTTPFS_CATALINA_HOME}/bin/catalina.sh "$@" > /dev/null
fi

