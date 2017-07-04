# ZkFailoverLogLogger

This tool will print the failover log entries for all vbuckets in Couchbase.

## Build

mvn clean compile assembly:single

## Run

java -jar target/zk-updater-jar-with-dependencies.jar localhost localhost cmbucket cmpasswd couchbase-kafka-connector2
