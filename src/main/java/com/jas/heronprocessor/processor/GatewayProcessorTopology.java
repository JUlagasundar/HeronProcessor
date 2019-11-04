package com.jas.heronprocessor.processor;

import com.jas.heronprocessor.processor.normalizer.Normalizer;
import com.jas.heronprocessor.processor.normalizer.PreProcessor;
import com.jas.heronprocessor.processor.props.HeronProperties;
import com.jas.heronprocessor.processor.props.KafkaProperties;
import com.jas.heronprocessor.processor.props.PathProperties;
import com.siem.gatewayprocessor.kafka.KafkaSource;
import com.siem.gatewayprocessor.partitioner.CustomerBasedKafkaSink;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.streamlet.Builder;
import com.twitter.heron.streamlet.Config;
import com.twitter.heron.streamlet.Runner;
import com.twitter.heron.streamlet.Streamlet;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GatewayProcessorTopology {

    private static final Logger LOGGER = Logger.getLogger("GatewayProcessor");

    private GatewayProcessorTopology() {
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 7) {
            throw new Exception("Arguments Missing!!! Should be in the format $TOPOLOGY_NAME$ $NUM_CONTAINERS$ $CPU$ $SOURCE_HEAP$ $PREPROCESSOR_HEAP$ $NORMALIZER_HEAP$ $SINK_HEAP$");
        } else {
            HeronProperties.TOPOLOGY_NAME = args[0];
            HeronProperties.NUM_CONTAINERS = Integer.parseInt(args[1].trim());
            HeronProperties.CPU = Float.parseFloat(args[2].trim());
            HeronProperties.SOURCE_HEAP = Long.parseLong(args[3].trim());
            HeronProperties.PREPROCESSOR_HEAP = Long.parseLong(args[4].trim());
            HeronProperties.NORMALIZER_HEAP = Long.parseLong(args[5].trim());
            HeronProperties.SINK_HEAP = Long.parseLong(args[6].trim());
        }

        Builder builder = Builder.newBuilder();

        Streamlet<Map<String, String>> kafkaSource = builder.newSource(new KafkaSource());

        kafkaSource
                .setNumPartitions(HeronProperties.NUM_CONTAINERS)
                .transform(new PreProcessor())
                .transform(new Normalizer())
                .toSink(new CustomerBasedKafkaSink());

        Config.DeliverySemantics deliverySemantics = applyDeliverySemantics();

        Config config = Config.newBuilder()
                .setNumContainers(HeronProperties.NUM_CONTAINERS)
                .setPerContainerCpu(HeronProperties.CPU)
                .setDeliverySemantics(deliverySemantics)
                .build();

        config.getHeronConfig().setContainerCpuRequested(HeronProperties.CPU);

        config.getHeronConfig().setComponentRam("generator1", ByteAmount.fromMegabytes(HeronProperties.SOURCE_HEAP));
        config.getHeronConfig().setComponentRam("transform1", ByteAmount.fromMegabytes(HeronProperties.PREPROCESSOR_HEAP));
        config.getHeronConfig().setComponentRam("transform2", ByteAmount.fromMegabytes(HeronProperties.NORMALIZER_HEAP));
        config.getHeronConfig().setComponentRam("sink1", ByteAmount.fromMegabytes(HeronProperties.SINK_HEAP));

        config.getHeronConfig().setMaxSpoutPending(10);

        config.getHeronConfig().setComponentJvmOptions("generator1", "-XX:MaxMetaspaceSize=192m");
        config.getHeronConfig().setComponentJvmOptions("sink1", "-XX:MaxMetaspaceSize=192m");

        new Runner().run(HeronProperties.TOPOLOGY_NAME, config, builder);
    }

    private static Config.DeliverySemantics applyDeliverySemantics() throws Exception {
        return Config.DeliverySemantics.ATLEAST_ONCE;
    }

    public static void loadProperties() {
        loadKafkaProperties();

        LOGGER.info("Properties Loading Completed!!!");
    }

    private static void loadKafkaProperties() {
        Properties properties = getPropertiesFromFile("kafka.properties");

        KafkaProperties.BOOTSTRAP_SERVERS = properties.getProperty("BOOTSTRAP_SERVERS").trim();
        KafkaProperties.SYSLOG_QUEUE_TOPIC = properties.getProperty("SYSLOG_QUEUE_TOPIC").trim();
        KafkaProperties.CONSUMER_GROUP_ID = properties.getProperty("CONSUMER_GROUP_ID").trim();
        KafkaProperties.ENABLE_AUTO_COMMIT = properties.getProperty("ENABLE_AUTO_COMMIT", "true").trim();
        KafkaProperties.AUTO_COMMIT_INTERVAL_MS = properties.getProperty("AUTO_COMMIT_INTERVAL_MS", "1000").trim();
        KafkaProperties.SESSION_TIMEOUT = properties.getProperty("SESSION_TIMEOUT", "30000").trim();
        KafkaProperties.AUTO_OFFSET_RESET = properties.getProperty("AUTO_OFFSET_RESET", "earliest").trim();
        KafkaProperties.MAX_POLL_RECORDS = properties.getProperty("MAX_POLL_RECORDS", "2").trim();
        KafkaProperties.MAX_POLL_INTERVAL_MS = properties.getProperty("MAX_POLL_INTERVAL_MS", "300000").trim();
        KafkaProperties.SYSLOG_TOPIC = properties.getProperty("SYSLOG_TOPIC").trim();
        KafkaProperties.EVENTLOG_TOPIC = properties.getProperty("EVENTLOG_TOPIC").trim();
        KafkaProperties.PRODUCER_COMPRESSION_TYPE = properties.getProperty("PRODUCER_COMPRESSION_TYPE", "lz4").trim();
        KafkaProperties.PRODUCER_ACKS = properties.getProperty("PRODUCER_ACKS", "all").trim();
        KafkaProperties.PRODUCER_RETRIES = Integer.parseInt(properties.getProperty("PRODUCER_RETRIES", "0").trim());
        KafkaProperties.PRODUCER_BATCH_SIZE = Integer.parseInt(properties.getProperty("PRODUCER_BATCH_SIZE", "16384").trim());
        KafkaProperties.PRODUCER_LINGER_MS = Long.parseLong(properties.getProperty("PRODUCER_LINGER_MS", "1").trim());
        KafkaProperties.PRODUCER_BUFFER_MEMORY = Long.parseLong(properties.getProperty("PRODUCER_BUFFER_MEMORY", "33554432").trim());
        KafkaProperties.NO_OF_PARTITIONS = Integer.parseInt(properties.getProperty("NO_OF_PARTITIONS", "3").trim());
    }

    private static Properties getPropertiesFromFile(String file) {
        Properties properties = new Properties();
        try (FileInputStream inputStream = new FileInputStream(new File(PathProperties.TOPOLOGY_CONF_PATH + file))) {
            properties.load(inputStream);
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "Exception Caught: {0}", ex);
        }

        return properties;
    }
}
