package com.jas.heronprocessor.processor.kafka;

import com.jas.heronprocessor.processor.props.KafkaProperties;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author daniccan
 */
public class SyslogKafkaProducer {

    private static final Logger LOGGER = Logger.getLogger("SyslogKafkaProducer");
    private Producer<String, byte[]> producer = null;

    private final Map<String, Integer> partitionAssignmentMap;

    public Producer<String, byte[]> getProducer() {
        return producer;
    }

    public Map<String, Integer> getPartitionAssignmentMap() {
        return partitionAssignmentMap;
    }

    public SyslogKafkaProducer() {
        Properties kafkaProducerProps = new Properties();
        kafkaProducerProps.put("bootstrap.servers", KafkaProperties.BOOTSTRAP_SERVERS);
        kafkaProducerProps.put("acks", KafkaProperties.PRODUCER_ACKS);
        kafkaProducerProps.put("retries", KafkaProperties.PRODUCER_RETRIES);
        kafkaProducerProps.put("batch.size", KafkaProperties.PRODUCER_BATCH_SIZE);
        kafkaProducerProps.put("max.request.size", 5000000);
        kafkaProducerProps.put("linger.ms", KafkaProperties.PRODUCER_LINGER_MS);
        kafkaProducerProps.put("buffer.memory", KafkaProperties.PRODUCER_BUFFER_MEMORY);
        kafkaProducerProps.put("key.serializer", KafkaProperties.KEY_SERIALIZER);
        kafkaProducerProps.put("value.serializer", KafkaProperties.VALUE_SERIALIZER);
        kafkaProducerProps.put("compression.type", KafkaProperties.PRODUCER_COMPRESSION_TYPE);

        producer = new KafkaProducer<>(kafkaProducerProps);

        partitionAssignmentMap = new TreeMap<>();
    }

    public void writeToKafkaTopic(String syslogvo, String topicName, String customerId) {

        if (!syslogvo.isEmpty()) {
            try {
                if (topicName.equals(KafkaProperties.SYSLOG_TOPIC)) {
                    if (partitionAssignmentMap.containsKey(customerId)) {
                        producer.send(new ProducerRecord<>(topicName, partitionAssignmentMap.get(customerId),
                                customerId, CompressionUtils.compressObjectToBytesArray(syslogvo)), (RecordMetadata rm, Exception excptn) -> {
                            if (excptn != null) {
                                LOGGER.log(Level.SEVERE, "Exception Caught: {0}", excptn);
                            }
                        });
                    } else {
                        producer.send(new ProducerRecord<>(topicName, customerId, CompressionUtils.compressObjectToBytesArray(syslogvo)), (RecordMetadata rm, Exception excptn) -> {
                            if (excptn != null) {
                                LOGGER.log(Level.SEVERE, "Exception Caught: {0}", excptn);
                            }
                        });
                    }

                    // Write to Separate topic for ES
//                    producer.send(new ProducerRecord<>(topicName + "_es", CompressionUtils.compressObjectToBytesArray(syslogvo)), (RecordMetadata rm, Exception excptn) -> {
//                        if (excptn != null) {
//                            LOGGER.log(Level.SEVERE, "Exception Caught: {0}", excptn);
//                        }
//                    });
                } else {
                    producer.send(new ProducerRecord<>(topicName, CompressionUtils.compressObjectToBytesArray(syslogvo)), (RecordMetadata rm, Exception excptn) -> {
                        if (excptn != null) {
                            LOGGER.log(Level.SEVERE, "Exception Caught: {0}", excptn);
                        }
                    });
                }
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "Exception Caught: {0}", e);
            }
        } else {
            LOGGER.severe("SyslogVO is NULL!!!");
        }
    }

    public void shutDown() {
        producer.close();
    }
}
