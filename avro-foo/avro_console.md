#### Start docker container to use AVRO console producers and consumers given by Confluent

` docker run -it --rm --net=host confluentinc/cp-schema-registry:3.3.1 bash `

###### Producer data to kafka

`kafka-avro-console-producer \
--broker-list 127.0.0.1:9092 --topic auto-test-avro \
--property schema.registry.url=http://127.0.0.1:8081 \
--property value.schema='{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}'`

###### Consume data from kafka using AVRO

`kafka-avro-console-consumer --topic auto-test-avro \
--bootstrap-server 127.0.0.1:9092 \
--property schema.registry.url=http://127.0.0.1:8081 \
--from-beginning`

