/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.example.app;

import com.example.domain.TextFileObj;
import com.example.mapper.ExampleStateBackendMapper;
import com.example.mapper.ExampleMySqlDBPoolSink;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 *
 * @author chandanar
 */
public class LocationTemperatureStreamProcessor {

    public static void main(String[] args) {

        final String source_topic = "myKafkaTopic";
        final String schemaReigstryUrl = "http://localhost:8081";
    
        try {
            //exec environment
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            //---------------------------------------------Start Env Configs---------------------------------------------
            //set backend with check-point dir and storage as filesystem - for fault tolerence :: configured using flink-conf.yaml
            //check point configs at every 20 secs
            Long checkpointInterval = 20 * 1000L;   
            Long checkpointTimeout = 10 * 1000L;    
            CheckpointConfig checkpointConfig = env.getCheckpointConfig();
            checkpointConfig.setCheckpointTimeout(checkpointTimeout);
            checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION); // enable externalized checkpoints which are retained after job cancellation
            env.enableCheckpointing(checkpointInterval);
            env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 0));
            env.getConfig().setUseSnapshotCompression(true);           


            //---------------------------------------------Kafka consumers---------------------------------------------
            Properties exampleKafkaConsProp = getConsumerProperties("example.kafka.cons.grp");

            //build consumer using topic, avro class, and schema registry
            FlinkKafkaConsumer<TextFileObj> exampleTxtKafkaSource = new FlinkKafkaConsumer<>(
                    source_topic,
                    ConfluentRegistryAvroDeserializationSchema.forSpecific(TextFileObj.class, schemaReigstryUrl),
                    exampleKafkaConsProp);
            exampleTxtKafkaSource.setStartFromTimestamp(0);
            exampleTxtKafkaSource.setCommitOffsetsOnCheckpoints(true);

            //---------------------------------------------Start Streaming kafka src---------------------------------------
            //get streams from consumer   
            DataStream<TextFileObj> exampleTxtKafkaStream = env.addSource(exampleTxtKafkaSource, "exampleTxtKafkaSource");        

            //---------------------------------------------Start Processing---------------------------------------------

            //key selector
            KeySelector ks = new KeySelector<TextFileObj, Long>() {

                @Override
                public Long getKey(TextFileObj t) {
                    return t.getLocationId();
                }
            };

            //get keyed streams
            KeyedStream<TextFileObj, Tuple> exampleTxtKafkaKeyedStream = exampleTxtKafkaStream.keyBy(ks, Types.LONG);

            //----------Check location id in RocksDB backend & get max temperature for perticular location              
            SingleOutputStreamOperator<TextFileObj> exampleTxtKafkaBackendStream = exampleTxtKafkaKeyedStream.map(new ExampleStateBackendMapper());
            
            //update MySql DB with highest temperature of perticular location - using C3P0 pool
            //filter not null records
            DataStream<TextFileObj> exampleTxtKafkaBackendFilteredStream = exampleTxtKafkaBackendStream.filter(new FilterFunction<TextFileObj>() {
                @Override
                public boolean filter(TextFileObj t) throws Exception {
                    return t != null;
                }
            });
            exampleTxtKafkaBackendFilteredStream.addSink(new ExampleMySqlDBPoolSink());

            //sink to kafka as well
            writeOutputToKafkaSink(exampleTxtKafkaBackendFilteredStream, "exampleTxtKafkaBackendFilteredStream", schemaReigstryUrl);

            //-------------------------------------execute-----------------------------------------------
            exampleTxtKafkaBackendFilteredStream.print().name("exampleTxtKafkaBackendFilteredStream-print").uid("exampleTxtKafkaBackendFilteredStream-print");
            env.execute("LocationTemperatureStreamProcessor");
        }
        catch (Exception ex) {
                Logger.getLogger(LocationTemperatureStreamProcessor.class.getName()).log(Level.SEVERE, null, ex);
        }

}

private static Properties getConsumerProperties(String groupId) {
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProperties.setProperty("group.id", groupId);
//        kafkaProperties.setProperty("security.protocol", "SASL_PLAINTEXT");
//        kafkaProperties.setProperty("sasl.mechanism", "GSSAPI");
//        kafkaProperties.setProperty("sasl.kerberos.service.name", "kafka");
//        kafkaProperties.setProperty("sasl.jaas.config", "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true serviceName=\"kafka\" keyTab=\"/home/<user>/user.keytab\" principal=\"user@EXAMPLE.COM\"; ");
        return kafkaProperties;
    }

    
    private static Properties getProducerProperties(String groupId) {
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProperties.setProperty("group.id", groupId);
//        kafkaProperties.setProperty("security.protocol", "SASL_PLAINTEXT");
//        kafkaProperties.setProperty("sasl.mechanism", "GSSAPI");
//        kafkaProperties.setProperty("sasl.kerberos.service.name", "kafka");
//        kafkaProperties.setProperty("sasl.jaas.config", "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true serviceName=\"kafka\" keyTab=\"/home/<user>/user.keytab\" principal=\"user@EXAMPLE.COM\"; ");
        return kafkaProperties;
    }
    
    public static void writeOutputToKafkaSink(DataStream<TextFileObj> queryResultStream, String streamName, String schemaReigstryUrl) {
        String topic = "myKafkaResultTopic";         
        Properties kafkaProdProp = getProducerProperties("example.kafka.producer.grp");

        FlinkKafkaProducer<TextFileObj> scDistMobNoKafkaProd = new FlinkKafkaProducer<>(
                topic,
                ConfluentRegistryAvroSerializationSchema.forSpecific(TextFileObj.class, topic + "-value", schemaReigstryUrl),
                kafkaProdProp);
        queryResultStream.addSink(scDistMobNoKafkaProd).name(streamName).uid(streamName);                 
                    
    }
}
