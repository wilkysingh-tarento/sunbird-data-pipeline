
max_msg_bytes=1572864
env=dev
kafka_partition_override_size=5242880

while read -r line; do

# skip comment
case $line in \#*)
    echo "Skip comment - $line"
    continue
esac

# skip empty line
if [[ -z "${line// }" ]]; then
    continue
fi

args=("$line")
topic=${args[0]}
partitions=${args[1]}
replicas=${args[2]}
retention=${args[3]}

echo "Creating Topic - $env.$topic:$partitions:$replicas"
if /opt/kafka/bin/kafka-topics.sh --create --topic "$env.$topic" ---zookeeper localhost:2181 --partitions "$partitions" --replication-factor "$replicas" ; then
    echo "Topic created - $env.$topic:$partitions:$replicas"
else
    echo "[WARN] Could not create topic - $env.$topic:$partitions:$replicas, could already exist, altering partitions and replicas"
    /opt/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic "$env.$topic" --partitions "$partitions"
    # /opt/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic "$env.$topic" --replication-factor "$replicas"
fi

/opt/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic "$env.$topic" --config retention.ms="$retention"
/opt/kafka/bin/kafka-topics.sh --zookeeper localhost:2181 --alter --topic "$env.$topic" --config max.message.bytes="$max_msg_bytes"

done


