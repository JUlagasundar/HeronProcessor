package com.jas.heronprocessor.processor.props;

/**
 * @author daniccan
 */
public class KafkaProperties {

    // Common Properties
    public static String BOOTSTRAP_SERVERS;

    // Consumer Properties
    public static String SYSLOG_QUEUE_TOPIC;
    public static String CONSUMER_GROUP_ID;
    public static String ENABLE_AUTO_COMMIT;
    public static String AUTO_COMMIT_INTERVAL_MS;
    public static String SESSION_TIMEOUT;
    public static String AUTO_OFFSET_RESET;
    public static String MAX_POLL_RECORDS;
    public static String MAX_POLL_INTERVAL_MS;
    public static String KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    public static String VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.ByteArrayDeserializer";

    // Producer Properties
    public static String SYSLOG_TOPIC;
    public static String EVENTLOG_TOPIC;
    public static String KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    public static String VALUE_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";
    public static String PRODUCER_COMPRESSION_TYPE;
    public static String PRODUCER_ACKS = "all";
    public static int PRODUCER_RETRIES = 0;
    public static int PRODUCER_BATCH_SIZE = 16384;
    public static long PRODUCER_LINGER_MS = 1;
    public static long PRODUCER_BUFFER_MEMORY = 33554432;
    public static int NO_OF_PARTITIONS;
}
