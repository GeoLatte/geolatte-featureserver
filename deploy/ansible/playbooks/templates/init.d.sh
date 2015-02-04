#! /bin/sh

# Applicatie start/stop script
APPLICATION_NAME={{package_name}}
APPLICATION_HOME={{work_path}}
APPLICATION_JAVA_OPTS="{{app.java_opts}}"
# Er zit een bug in de huidige versie van de SBT native packager, waardoor de java_opts genegeerd worden, vandaar dat we 
# voorlopig werken met de mem optie. In SBT native packager 0.7.0 zal deze bug gefixed zijn (moet nog uitkomen - nu: 14 mei 2014).
APPLICATION_JAVA_MEM="{{app.mem_opts}}"
APPLICATION_USER={{app.user}}
CONF={{conf_path}}
LOGGING={{log_conf_path}}
LOG_DIR={{logs_dir_path}}
STDOUT_LOG_FILE=$LOG_DIR/stdout.log
HOST_ADDRESS={{host.ip}}
HOST_PORT={{host.port}}

export PATH="{{java_home}}/bin:$PATH"
export JAVA_HOME="{{java_home}}"

do_start () {
  echo  "Starting $APPLICATION_NAME"
  su $APPLICATION_USER -c "$APPLICATION_HOME/bin/$APPLICATION_NAME $APPLICATION_JAVA_OPTS -Dfile.encoding=UTF-8 -Dpidfile.path=$APPLICATION_HOME/RUNNING_PID -Dconfig.file=$CONF -Dlogger.file=$LOGGING -Dhttp.port=$HOST_PORT -Dhttp.address=$HOST_ADDRESS &"
}

do_stop () {
  echo "checking if $APPLICATION_NAME is running"
  PID=$(cat $APPLICATION_HOME/RUNNING_PID)
  if [ -z "$PID" ];
      then
      echo "$APPLICATION_NAME isn't running, no need to stop"
      exit
  fi
  echo " it is"
  echo "Stopping $APPLICATION_NAME with-> /bin/kill $PID"
    /bin/kill $PID
}

case "$1" in
  start)
     do_start
     ;;
   stop)
     do_stop
     ;;
   *)
     echo "Usage: $0 start|stop" >&2
     exit 3
     ;;
esac
