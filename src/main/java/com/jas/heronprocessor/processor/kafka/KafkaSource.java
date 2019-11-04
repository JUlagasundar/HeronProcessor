package com.siem.gatewayprocessor.kafka;

import com.siem.gatewayprocessor.GatewayProcessorTopology;
import com.siem.gatewayprocessor.properties.KafkaProperties;
import com.twitter.heron.streamlet.Context;
import com.twitter.heron.streamlet.Source;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaSource implements Source {

    private Consumer<String, byte[]> kafkaConsumer;
    private List<String> syslogTopic;

    private static final Logger LOGGER = Logger.getLogger("KafkaSource");

    @Override
    public void setup(Context context) {

        GatewayProcessorTopology.loadProperties();

        syslogTopic = Arrays.asList(KafkaProperties.SYSLOG_QUEUE_TOPIC);

        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaProperties.BOOTSTRAP_SERVERS);
        props.put("group.id", KafkaProperties.CONSUMER_GROUP_ID);
        props.put("enable.auto.commit", KafkaProperties.ENABLE_AUTO_COMMIT);
        props.put("auto.commit.interval.ms", KafkaProperties.AUTO_COMMIT_INTERVAL_MS);
        props.put("session.timeout.ms", KafkaProperties.SESSION_TIMEOUT);
        props.put("key.deserializer", KafkaProperties.KEY_DESERIALIZER);
        props.put("value.deserializer", KafkaProperties.VALUE_DESERIALIZER);
        props.put("auto.offset.reset", KafkaProperties.AUTO_OFFSET_RESET);
        props.put("max.poll.records", KafkaProperties.MAX_POLL_RECORDS);
        props.put("max.poll.interval.ms", KafkaProperties.MAX_POLL_INTERVAL_MS);

        kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(syslogTopic);

        LOGGER.log(Level.INFO, "Consumer Group: {0}, Subscribing to Topic: {1}", new Object[]{KafkaProperties.CONSUMER_GROUP_ID, syslogTopic});
    }

    @Override
    public Collection get() {

        List<Map<String, byte[]>> kafkaRecords = new ArrayList<>();

        Map<String, byte[]> kafkaRecord = new HashMap<>();

        ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(Long.MAX_VALUE);

        for (ConsumerRecord<String, byte[]> record : records) {
            kafkaRecord.put(record.key(), record.value());
            kafkaRecords.add(kafkaRecord);
        }

        return kafkaRecords;
    }

    @Override
    public void cleanup() {
        kafkaConsumer.wakeup();
    }
}
