#! /bin/bash
# ulimit -n 65536

ROOT=$(realpath $(dirname $0)/..)

if [[ "$1" == "with-agent" ]] 
then
		echo "starting with your-kit agent"
        $ROOT/target/start -Xmx1024m -Dlogger.file=conf/logger.xml -agentpath:/opt/yourkit/yjp-9.5.5/bin/linux-x86-64/libyjpagent.so
elif [[ "$1" == "with-remote-debug" ]]
then
		echo "starting with remote debug config"
        $ROOT/target/start -Xmx1024m -Dlogger.file=conf/logger.xml -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005
else
        $ROOT/target/start -Xmx1024m -Dlogger.file=conf/logger.xml
fi
