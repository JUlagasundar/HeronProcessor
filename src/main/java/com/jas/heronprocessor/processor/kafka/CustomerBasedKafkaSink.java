package com.siem.gatewayprocessor.partitioner;

import com.google.gson.Gson;
import com.siem.gatewayprocessor.GatewayProcessorTopology;
import com.siem.gatewayprocessor.database.DatabaseTablesLookup;
import com.siem.eventnormalizer.eventlog.EventLogDO;
import com.siem.gatewayprocessor.kafka.SyslogKafkaProducer;
import com.siem.gatewayprocessor.properties.KafkaProperties;
import com.siem.gatewayprocessor.utils.CompressionUtils;
import com.twitter.heron.streamlet.Context;
import com.twitter.heron.streamlet.Sink;
import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.DataFormatException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * @author daniccan
 */
public class CustomerBasedKafkaSink implements Sink {

    private static final Logger LOGGER = Logger.getLogger("CustomerBasedKafkaSink");

    private SyslogKafkaProducer syslogKafkaProducer;
    private EventLogToSyslogConvertor eventLogToSyslogConvertor;
    private JSONParser jsonParser;
    private Gson gson;

    public static long kafkaPublishedCount;

    private static final long DATABASE_LOOKUP_INTERVAL = 30 * 60 * 1000;
    private static final long SYSLOG_COUNT_TIMER = 1 * 60 * 1000;
    private static long loggingTimeInMilliSeconds = System.currentTimeMillis();
    private static boolean isPartitionScheduled = false;

    @Override
    public void setup(Context context) {

        GatewayProcessorTopology.loadProperties();

        initializeDatabaseLookup();

        this.syslogKafkaProducer = new SyslogKafkaProducer();
        this.eventLogToSyslogConvertor = new EventLogToSyslogConvertor();
        this.jsonParser = new JSONParser();
        this.gson = new Gson();

        initializeSyslogCountTimer();
    }

    @Override
    public void put(Object tuple) {

        Map<String, JSONArray> customerBasedSyslogVO = new HashMap<>();
        Map<String, JSONArray> customerBasedEventlogVO = new HashMap<>();

        List<EventLogDO> eventLogVO = null; 
        try {
            eventLogVO = (List<EventLogDO>) CompressionUtils.decompressObjectFromBytesArray((byte[]) tuple);
        } catch (DataFormatException | IOException | ClassNotFoundException ex) {
            LOGGER.log(Level.SEVERE, "Exception Caught: {0}", ex);
        }

        if (eventLogVO != null && !eventLogVO.isEmpty()) {

            long producerStartTime = System.currentTimeMillis();

            eventLogVO.forEach((eventLogDO) -> {
                String customerId = String.valueOf(eventLogDO.getCustomer().getId());

                JSONArray customerSyslogJsonArray;
                JSONArray customerEventLogJsonArray;
                if (customerBasedSyslogVO.containsKey(customerId)) {
                    customerSyslogJsonArray = customerBasedSyslogVO.get(customerId);
                    customerEventLogJsonArray = customerBasedEventlogVO.get(customerId);
                } else {
                    customerSyslogJsonArray = new JSONArray();
                    customerEventLogJsonArray = new JSONArray();
                }

                try {
                    JSONObject syslogDoJson = (JSONObject) jsonParser.parse(gson.toJson(eventLogToSyslogConvertor.getSyslog(eventLogDO)));
                    customerSyslogJsonArray.add(syslogDoJson);

                    JSONObject eventlogDOJson = (JSONObject) jsonParser.parse(gson.toJson(eventLogDO));
                    customerEventLogJsonArray.add(eventlogDOJson);
                } catch (ParseException e) {
                    LOGGER.log(Level.SEVERE, "Exception Caught: {0}", e);
                }

                customerBasedSyslogVO.put(customerId, customerSyslogJsonArray);
                customerBasedEventlogVO.put(customerId, customerEventLogJsonArray);
            });

            customerBasedSyslogVO.keySet().forEach((customerId) -> {
                JSONArray customerSyslogList = customerBasedSyslogVO.get(customerId);
                kafkaPublishedCount = kafkaPublishedCount + customerSyslogList.size();
                syslogKafkaProducer.writeToKafkaTopic(customerSyslogList.toString(), KafkaProperties.SYSLOG_TOPIC, customerId);
            });

            customerBasedEventlogVO.keySet().forEach((customerId) -> {
                JSONArray customerSyslogList = customerBasedEventlogVO.get(customerId);
                syslogKafkaProducer.writeToKafkaTopic(customerSyslogList.toString(), KafkaProperties.EVENTLOG_TOPIC, customerId);
            });

            EventLogDO firstEventOfVo = eventLogVO.get(0);

            LOGGER.log(Level.INFO, "[CID->{0}, SID->{1}, Date->{2}, Count->{3}, Time->{4}ms]",
                    new Object[]{String.valueOf(firstEventOfVo.getCustomer().getId()),
                        String.valueOf(firstEventOfVo.getSentinel().getId()),
                        firstEventOfVo.getEvent_received_date(),
                        eventLogVO.size(), System.currentTimeMillis() - producerStartTime});
        }
    }

    @Override
    public void cleanup() {
        syslogKafkaProducer.shutDown();
    }

    private void initializeSyslogCountTimer() {
        try {
            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    LOGGER.log(Level.INFO, "Kafka Published Events between {0} and {1} : {2}",
                            new Object[]{new Date(loggingTimeInMilliSeconds), new Date(), kafkaPublishedCount});
                    loggingTimeInMilliSeconds = System.currentTimeMillis();
                    kafkaPublishedCount = 0;

                    Calendar now = Calendar.getInstance();
                    if (now.get(Calendar.HOUR_OF_DAY) == 0 || syslogKafkaProducer.getPartitionAssignmentMap().isEmpty()) {
                        if (!isPartitionScheduled) {
                            assignPartitions();
                            isPartitionScheduled = true;
                        }
                    } else {
                        isPartitionScheduled = false;
                    }
                }

                private void assignPartitions() {
                    try {
                        double totalVolume = 0.0f;
                        for (double customerVolume : DatabaseTablesLookup.CUSTOMER_VOLUME_MAP.values()) {
                            totalVolume += customerVolume;
                        }

                        double avgCapacity = totalVolume / KafkaProperties.NO_OF_PARTITIONS;

                        LOGGER.log(Level.INFO, "Total Volume: {0}, Partitions: {1}, Avg. Capacity: {2}",
                                new Object[]{totalVolume, KafkaProperties.NO_OF_PARTITIONS, avgCapacity});

                        Map<Integer, Double> partitionVolumeMap = new LinkedHashMap<>();

                        int lastUpdatedPartition = -1;

                        for (Integer customerId : DatabaseTablesLookup.CUSTOMER_VOLUME_MAP.keySet()) {
                            double tmpAvgCapacity = avgCapacity;
                            for (int partition = lastUpdatedPartition + 1; partition < KafkaProperties.NO_OF_PARTITIONS; partition++) {
                                double previousPartitionVolume = 0.0;
                                if (partitionVolumeMap.containsKey(partition)) {
                                    previousPartitionVolume = partitionVolumeMap.get(partition);
                                }

                                double customerVolume = DatabaseTablesLookup.CUSTOMER_VOLUME_MAP.get(customerId);

                                double newPartitionVolume = customerVolume + previousPartitionVolume;
                                if (newPartitionVolume <= avgCapacity || previousPartitionVolume == 0.0) {
                                    partitionVolumeMap.put(partition, newPartitionVolume);
                                    syslogKafkaProducer.getPartitionAssignmentMap().put(String.valueOf(customerId), partition);
                                    lastUpdatedPartition = partition;
                                    break;
                                } else {
                                    if (partition == KafkaProperties.NO_OF_PARTITIONS - 1) {
                                        tmpAvgCapacity = tmpAvgCapacity + (0.1 * tmpAvgCapacity); // Increase Average Capacity by 10%
                                        partition = -1; // Reset Partition
                                    }
                                }
                            }

                            if (lastUpdatedPartition == KafkaProperties.NO_OF_PARTITIONS - 1) {
                                lastUpdatedPartition = -1; // Reset Last Updated Partition
                            }
                        }

                        LOGGER.log(Level.INFO, "Assigned Partitions for Customers: {0}", syslogKafkaProducer.getPartitionAssignmentMap().toString());
                        LOGGER.log(Level.INFO, "Approx. Volume for Each Partition: {0}", partitionVolumeMap.toString());
                    } catch (Exception e) {
                        LOGGER.log(Level.SEVERE, "Exception Caught: {0}", e);
                    }
                }
            }, SYSLOG_COUNT_TIMER, SYSLOG_COUNT_TIMER);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Exception Caught: {0}", e);
        }
    }

    private void initializeDatabaseLookup() {
        try {
            DatabaseTablesLookup databaseTablesLookup = DatabaseTablesLookup.getInstance(false, false, true);
            Timer timer = new Timer();
            timer.schedule(databaseTablesLookup, DATABASE_LOOKUP_INTERVAL, DATABASE_LOOKUP_INTERVAL);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Exception Caught: {0}", e);
        }
    }
}
