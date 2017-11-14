master=$(./sbin/start-master.sh | grep "logging to" | awk 'NF{ print $NF }') &
port=$(cat "$master" | grep "Successfully started service 'sparkMaster' on port" | awk 'NF{ print $NF }' | cut -d. -f1) &
ip=$(hostname -I | xargs) &
sleep 3 &
./bin/spark-class org.apache.spark.deploy.worker.Worker spark://"$ip":"$port" &
sleep 5