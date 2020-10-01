#!/bin/bash
#
MYNAME="${0##*/}"

#file client impersonation
MAPR_IMPERSONATION_ENABLED="True"
export MAPR_IMPERSONATION_ENABLED

#gives access to MAPR_ECOSYSTEM_LOGIN_OPTS
MAPR_HOME="/opt/mapr"
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

## @description  Print usage
## @audience     private
## @stability    stable
## @replaceable  no
function print_usage
{
  cat <<EOF
Usage: ${MYNAME} start|status|stop
commands:
  start   Start HttpFS server as a daemon
  status  Return the status of the HttpFS server daemon
  stop    Stop the HttpFS server daemon
EOF
}

function waitForPid() {
  # allow process time to write pid to the file
  if [ -f $pid ]; then
    cnt=0
    rtry=5
    while [ ! -s $pid -a $cnt -lt $rtry ]; do
        sleep 1
      cnt=`expr $cnt + 1`
    done
    [ -s $pid ] && return 0
  fi
  return 1
}
hadoop_rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
        num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
        while [ $num -gt 1 ]; do
            prev=`expr $num - 1`
            [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
            num=$prev
        done
        mv "$log" "$log.$num";
    fi
}

run_httpfs()
{
  # some Java parameters
  if [ "$JAVA_HOME" != "" ]; then
    #echo "run java in $JAVA_HOME"
    JAVA_HOME=$JAVA_HOME
  fi

  if [ "$JAVA_HOME" = "" ]; then
    echo "Error: JAVA_HOME is not set."
    exit 1
  fi
  JAVA=$JAVA_HOME/bin/java

  exec "$JAVA" -Dproc_httpfs $HTTPFS_OPTS $MAPR_ECOSYSTEM_LOGIN_OPTS -classpath "$HTTPFS_CLASSPATH" $HTTPFS_CLASSNAME "$@"
}

if [ "$HTTPFS_PID_DIR" = "" ]; then
  HTTPFS_PID_DIR=$MAPR_HOME/pid
fi
if [ "$HTTPFS_PID_DIR" = "" ]; then
  HTTPFS_PID_DIR=$MAPR_HOME/pid
fi

# some variables
pid=$HTTPFS_PID_DIR/httpfs.pid
log=$HTTPFS_LOG/httpfs.out

# Set default scheduling priority
if [ "$HTTPFS_NICENESS" = "" ]; then
  export HTTPFS_NICENESS=0
fi
if [[ $# = 0 ]]; then
  print_usage
  exit
fi

case $1 in
  (start)

    [ -w "$HTTPFS_PID_DIR" ] || mkdir -p "$HTTPFS_PID_DIR"
    if waitForPid ; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo httpfs running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi
    hadoop_rotate_log $log
    echo starting httpfs, logging to $log
    cd "$HTTPFS_HOME"
    run_httpfs > "$log" 2>&1 < /dev/null &
    echo $! > $pid
    sleep 1; head "$log"
    ;;
  (stop)
    if waitForPid ; then
      TARGET_PID=`cat $pid`
      if kill -0 $TARGET_PID > /dev/null 2>&1; then
        echo stopping httpfs
        kill $TARGET_PID
        sleep 5
        if kill -0 $TARGET_PID > /dev/null 2>&1; then
          echo "httfs did not stop gracefully after 5 seconds: killing with kill -9"
          kill -9 $TARGET_PID
        fi
      else
        echo no httpfs to stop
      fi
      rm -f $pid
    else
      echo no httpfs to stop
    fi
  ;;
  (status)
    if waitForPid ; then
      PID=`cat $pid`
      if kill -0 $PID > /dev/null 2>&1; then
        echo httpfs is running.
        exit 0
      else
        echo $pid file is present but httpfs not running.
        exit 1
      fi
    else
      echo httpfs not running.
      exit 2
    fi
  ;;
  *)
echo "Unknown command \"$1\"."
    print_usage
    exit 1
  ;;
esac