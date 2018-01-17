#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


RETURN_SUCCESS=0
RETURN_ERR=1



MAPR_HOME=${MAPR_HOME:-/opt/mapr}
HTTPFS_VERSION="1.0"
HTTPFS_HOME="$MAPR_HOME"/httpfs/httpfs-"$HTTPFS_VERSION"
MAPR_CONF_DIR="$MAPR_HOME"/conf
MAPR_CONFD_DIR="$MAPR_HOME"/conf/conf.d
HTTPFS_CONF_DIR="$HTTPFS_HOME"/etc/hadoop/
HTTPFS_SECURE="$HTTPFS_CONF_DIR"/isSecure
DAEMON_CONF="$MAPR_HOME/conf/daemon.conf"
WARDEN_HTTPFS_CONF="$HTTPFS_HOME"/etc/hadoop/warden.httpfs.conf
HTTPFS_SHARE_CONF="$HTTPFS_HOME"/share/hadoop/httpfs/tomcat/conf/

isSecure=${isSecure:-0}
isOnlyRoles=${isOnlyRoles:-0}
customSec=0

# isSecure from server/configure.sh


#
# Setup MAPR_USER
#
if [ -z "$MAPR_USER" ] ; then
  if [ -f "$DAEMON_CONF" ] ; then
    MAPR_USER=$( awk -F = '$1 == "mapr.daemon.user" { print $2 }' "$DAEMON_CONF")
  else
    MAPR_USER=mapr
  fi
fi

#
# Setup MAPR_GROUP
#
if [ -z "$MAPR_GROUP" ] ; then
  if [ -f "$DAEMON_CONF" ] ; then
    MAPR_GROUP=$( awk -F = '$1 == "mapr.daemon.group" { print $2 }' "$DAEMON_CONF" )
  else
    MAPR_GROUP="$MAPR_USER"
  fi
fi



USAGE="usage: $0 [-h] [-R] [--secure] [--unsecure]"


while [ ${#} -gt 0 ] ; do
  case "$1" in
    --secure)
      isSecure=1
    shift ;;

    --unsecure)
      isSecure=0
    shift ;;

    --custom)
    # ignoring
    shift ;;

     --customSecure)
    customSec=1
    shift ;;

    -R)
      isOnlyRoles=1;
    shift ;;

    -EC)
    # ignoring
      shift 2
    ;;

    -h)
      echo "$USAGE"
      return $RETURN_SUCCESS 2>/dev/null || exit $RETURN_SUCCESS
    ;;

    *)
      echo "$USAGE"
      return $RETURN_ERR  2>/dev/null || exit $RETURN_ERR
    ;;
  esac
done



change_permissions() {
#
# Setup MAPR_USER
#
if [ -z "$MAPR_USER" ] ; then
  if [ -f "$DAEMON_CONF" ] ; then
    MAPR_USER=$( awk -F = '$1 == "mapr.daemon.user" { print $2 }' "$DAEMON_CONF")
  else
    MAPR_USER=mapr
  fi
fi

#
# Setup MAPR_GROUP
#
if [ -z "$MAPR_GROUP" ] ; then
  if [ -f "$DAEMON_CONF" ] ; then
    MAPR_GROUP=$( awk -F = '$1 == "mapr.daemon.group" { print $2 }' "$DAEMON_CONF" )
  else
    MAPR_GROUP="$MAPR_USER"
  fi
fi


chown -R "$MAPR_USER":"$MAPR_GROUP" "$HTTPFS_HOME"
chmod 600 "$HTTPFS_CONF_DIR"/httpfs-signature.secret
}

if ! [ -f "$HTTPFS_SECURE" ] ; then
    touch ${HTTPFS_SECURE}
fi

if [ "$isSecure" == 1 ] ; then
   if grep --quiet  secure=true "$HTTPFS_SECURE"; then
     doRestart=0
   else
     echo "secure=true" > ${HTTPFS_SECURE}
     if [ "$customSec" == 0 ] ; then
         cp "$HTTPFS_SHARE_CONF"/server.xml.https "$HTTPFS_SHARE_CONF"/server.xml
     fi
     doRestart=1
   fi

else
   if grep --quiet  secure=false ${HTTPFS_SECURE}; then
     doRestart=0
   else
     echo "secure=false" > ${HTTPFS_SECURE}
     if [ "$customSec" == 0 ] ; then
        cp "$HTTPFS_SHARE_CONF"/server.xml.orig "$HTTPFS_SHARE_CONF"/server.xml
     fi
     doRestart=1
   fi
fi


if ! [ -f ${MAPR_CONFD_DIR}/warden.httpfs.conf ] ; then
  cp ${WARDEN_HTTPFS_CONF} ${MAPR_CONFD_DIR}
  chown $MAPR_USER:$MAPR_GROUP ${MAPR_CONFD_DIR}/warden.httpfs.conf
fi


if ! [ -f "$HTTPFS_CONF_DIR"/.not_configured_yet ] ; then
  if [ "$doRestart" == 1 ] ; then
    if ! [ -d "$MAPR_CONF_DIR"/restart ] ; then
        mkdir "$MAPR_CONF_DIR"/restart
    fi
    cat <<EOF > "${MAPR_CONF_DIR}/restart/httpfs-${HTTPFS_VERSION}.restart"
      #!/bin/bash
      isSecured=\$(head -1 ${MAPR_HOME}/conf/mapr-clusters.conf | grep -o 'secure=\w*' | cut -d= -f2)
      if [ "\${isSecured}" = "true" ] && [ -f "${MAPR_HOME}/conf/mapruserticket" ]; then
        export MAPR_TICKETFILE_LOCATION="${MAPR_HOME}/conf/mapruserticket"
        maprcli node services -action restart -name httpfs -nodes $(hostname)
      else
        sudo -u ${MAPR_USER} maprcli node services -action restart -name httpfs -nodes $(hostname)
      fi
EOF
    chmod +x "${MAPR_CONF_DIR}/restart/httpfs-${HTTPFS_VERSION}.restart"
    chown $MAPR_USER:$MAPR_GROUP "${MAPR_CONF_DIR}/restart/httpfs-${HTTPFS_VERSION}.restart"
  fi
fi


#if [ "$isOnlyRoles" == 1 ] ; then
#  if ! [ -f "$MAPR_CONFD_DIR" ] ; then
#    # Configure network
#    if checkNetworkPortAvailability 14000 ; then
#        # Register port for HttpFS
#        registerNetworkPort httpfs 14000
#
#        # Copy HttpFS Warden conf into Warden conf directory
#        cp "${WARDEN_HTTPFS_CONF}" "${MAPR_CONFD_DIR}"
#        logInfo 'Warden conf for HttpFS copied.'
#        change_permissions
#    else
#        logErr 'ERROR: HttpFS cannot be registered because its ports already has been taken.'
#        exit $RETURN_ERR
#    fi
#  else
#    change_permissions
#  fi
#fi


#echo "$HTTPFS_VERSION" > "$MAPR_HOME"/httpfs/httpfsversion

change_permissions

if [ -f "$HTTPFS_CONF_DIR"/.not_configured_yet ] ; then
  rm "$HTTPFS_CONF_DIR"/.not_configured_yet
fi

exit $RETURN_SUCCESS



