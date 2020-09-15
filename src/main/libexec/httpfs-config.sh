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


function print() {
  if [ "${HTTPFS_SILENT}" != "true" ]; then
    echo "$@"
  fi
}

# if HTTPFS_HOME is already set warn it will be ignored
#
if [ "${HTTPFS_HOME}" != "" ]; then
  echo "WARNING: current setting of HTTPFS_HOME ignored"
fi

print

# setting HTTPFS_HOME to the installation dir, it cannot be changed
#
export HTTPFS_HOME=${BASEDIR}
httpfs_home=${HTTPFS_HOME}
print "Setting HTTPFS_HOME:          ${HTTPFS_HOME}"

# if the installation has a env file, source it
# this is for native packages installations
#
if [ -e "${HTTPFS_HOME}/bin/httpfs-env.sh" ]; then
  print "Sourcing:                    ${HTTPFS_HOME}/bin/httpfs-env.sh"
  source ${HTTPFS_HOME}/bin/HTTPFS-env.sh
  grep "^ *export " ${HTTPFS_HOME}/bin/httpfs-env.sh | sed 's/ *export/  setting/'
fi

# verify that the sourced env file didn't change HTTPFS_HOME
# if so, warn and revert
#
if [ "${HTTPFS_HOME}" != "${httpfs_home}" ]; then
  print "WARN: HTTPFS_HOME resetting to ''${HTTPFS_HOME}'' ignored"
  export HTTPFS_HOME=${httpfs_home}
  print "  using HTTPFS_HOME:        ${HTTPFS_HOME}"
fi

if [ "${HTTPFS_CONFIG}" = "" ]; then
  export HTTPFS_CONFIG=${HTTPFS_HOME}/etc/hadoop
  print "Setting HTTPFS_CONFIG:        ${HTTPFS_CONFIG}"
else
  print "Using   HTTPFS_CONFIG:        ${HTTPFS_CONFIG}"
fi
httpfs_config=${HTTPFS_CONFIG}

# verify that the sourced env file didn't change HTTPFS_HOME
# if so, warn and revert
#
if [ "${HTTPFS_HOME}" != "${httpfs_home}" ]; then
  echo "WARN: HTTPFS_HOME resetting to ''${HTTPFS_HOME}'' ignored"
  export HTTPFS_HOME=${httpfs_home}
fi

# verify that the sourced env file didn't change HTTPFS_CONFIG
# if so, warn and revert
#
if [ "${HTTPFS_CONFIG}" != "${httpfs_config}" ]; then
  echo "WARN: HTTPFS_CONFIG resetting to ''${HTTPFS_CONFIG}'' ignored"
  export HTTPFS_CONFIG=${httpfs_config}
fi

if [ "${HTTPFS_LOG}" = "" ]; then
  export HTTPFS_LOG=${HTTPFS_HOME}/logs
  print "Setting HTTPFS_LOG:           ${HTTPFS_LOG}"
else
  print "Using   HTTPFS_LOG:           ${HTTPFS_LOG}"
fi

if [ ! -f ${HTTPFS_LOG} ]; then
  mkdir -p ${HTTPFS_LOG}
fi

if [ "${HTTPFS_TEMP}" = "" ]; then
  export HTTPFS_TEMP=${HTTPFS_HOME}/temp
  print "Setting HTTPFS_TEMP:           ${HTTPFS_TEMP}"
else
  print "Using   HTTPFS_TEMP:           ${HTTPFS_TEMP}"
fi

if [ ! -f ${HTTPFS_TEMP} ]; then
  mkdir -p ${HTTPFS_TEMP}
fi

httpfs_opts="${httpfs_opts} -Dhttpfs.config.dir=${HTTPFS_CONFIG}";
httpfs_opts="${httpfs_opts} -Dhttpfs.home.dir=${HTTPFS_HOME}";
httpfs_opts="${httpfs_opts} -Dhttpfs.log.dir=${HTTPFS_LOG}";
httpfs_opts="${httpfs_opts} -Dhttpfs.temp.dir=${HTTPFS_TEMP}";
httpfs_opts="${httpfs_opts} -Dhttpfs.http.hostname=${HTTPFS_HOST_NAME:-$(hostname -f)}";
if [[ -n "${HTTPFS_SSL_ENABLED}" ]]; then
    httpfs_opts="${httpfs_opts} -Dhttpfs.ssl.enabled=${HTTPFS_SSL_ENABLED}";
fi

export HTTPFS_OPTS=$httpfs_opts

mapr_home_dir=${MAPR_HOME:-/opt/mapr}
hadoop_version=`cat /opt/mapr/hadoop/hadoopversion`
hadoop_home_dir=${mapr_home_dir}/hadoop/hadoop-${hadoop_version}

hadoop_common_file=`find ${hadoop_home_dir}/share/hadoop/common/* -type f -name hadoop-common* | sort -nrz | head -1`
httpfs_classpath="${hadoop_common_file}:${hadoop_home_dir}/share/hadoop/common/lib/*"
httpfs_classpath="${HTTPFS_HOME}/share/hadoop/hdfs/*:${HTTPFS_HOME}/share/hadoop/hdfs/lib/*:${httpfs_classpath}"

export HTTPFS_CLASSPATH=$httpfs_classpath

export HTTPFS_CLASSNAME=org.apache.hadoop.fs.http.server.HttpFSServerWebServer

print

