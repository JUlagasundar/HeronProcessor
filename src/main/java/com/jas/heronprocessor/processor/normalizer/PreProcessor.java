package com.jas.heronprocessor.processor.normalizer;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.jas.heronprocessor.processor.GatewayProcessorTopology;
import com.twitter.heron.streamlet.Context;
import com.twitter.heron.streamlet.SerializableTransformer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.DataFormatException;

public class PreProcessor implements SerializableTransformer {

    private static final Logger LOGGER = Logger.getLogger("PreProcessor");

    private Gson gson;

    public static long errorCount;
    public static long successEventCount;
    public static long preProcessedEventCount;
    public static long invalidSentinelEventCount;
    public static long inactiveDeviceEventCount;

    private static final long DATABASE_LOOKUP_INTERVAL = 10 * 60 * 1000;
    private static final long SYSLOG_COUNT_TIMER = 1 * 60 * 1000;
    private static long loggingTimeInMilliSeconds = System.currentTimeMillis();

    @Override
    public void setup(Context context) {

        GatewayProcessorTopology.loadProperties();

        initializeDatabaseLookup();

        gson = new Gson();
        rawlogProcessor = new RawlogProcessor();

        errorCount = 0;
        successEventCount = 0;
        preProcessedEventCount = 0;
        invalidSentinelEventCount = 0;
        inactiveDeviceEventCount = 0;

        initializeSyslogCountTimer();
    }

    @Override
    public void transform(Object tuple, Consumer consumer) {
        Map<String, byte[]> kafkaRecords = (HashMap<String, byte[]>) tuple;

        kafkaRecords.keySet().forEach((sentinelUUID) -> {

            List<RawlogMessage> preProcessedMessages = new ArrayList<>();

            Map<String, Integer> sentryDetails = rawlogProcessor.validateSentry(sentinelUUID);

            if (sentryDetails.get("SENTINEL_ID") != null) {
                List<String> syslogvo = null;
                try {
                    syslogvo = gson.fromJson((String) CompressionUtils.decompressObjectFromBytesArray(kafkaRecords.get(sentinelUUID)), new TypeToken<ArrayList<String>>() {
                    }.getType());
                } catch (JsonSyntaxException | DataFormatException | IOException | ClassNotFoundException e) {
                    errorCount++;
                }

                if (syslogvo != null) {

                    long preProcessStartTime = System.currentTimeMillis();

                    syslogvo.forEach((syslogdo) -> {
                        try {
                            if (syslogdo != null) {
                                RawlogMessage message = rawlogProcessor.processSyslog(syslogdo, sentryDetails.get("SENTINEL_ID"), sentryDetails.get("CUSTOMER_ID"));
                                if (message != null) {
                                    preProcessedMessages.add(message);
                                    successEventCount++;
                                }
                            } else {
                                errorCount++;
                            }
                        } catch (DeviceNotFoundException ex) {
                            RawlogMessage rawlogMessage = ex.getRawlogMessage();
                            if (!DatabaseTablesLookup.NEW_DEVICE_MAP.containsKey(rawlogMessage.getDeviceIP())) {
                                DatabaseTablesLookup.NEW_DEVICE_MAP.put(rawlogMessage.getDeviceIP(), rawlogMessage);
                                LOGGER.log(Level.INFO, "New Device Detected: {0} for Customer ID: {1}", new Object[]{rawlogMessage.getDeviceIP(), rawlogMessage.getCustomerId()});
                            }
                        } catch (DeviceInactiveException ex) {
                            inactiveDeviceEventCount++;
                            // TODO: Log Inactive Device Messages
                        }
                        preProcessedEventCount++;
                    });

                    if (preProcessedMessages.size() > 0) {
                        LOGGER.log(Level.INFO, "[CID->{0}, SID->{1}, Date->{2}, Count->{3}, Time->{4}ms]",
                                new Object[]{String.valueOf(sentryDetails.get("CUSTOMER_ID")),
                                    String.valueOf(sentryDetails.get("SENTINEL_ID")),
                                    preProcessedMessages.get(0).getEventReceivedDate(),
                                    syslogvo.size(), System.currentTimeMillis() - preProcessStartTime});
                    }
                }
            } else {
                try {
                    List<String> syslogvo = gson.fromJson((String) CompressionUtils.decompressObjectFromBytesArray(kafkaRecords.get(sentinelUUID)), new TypeToken<ArrayList<String>>() {
                    }.getType());
                    invalidSentinelEventCount = invalidSentinelEventCount + syslogvo.size();
                } catch (JsonSyntaxException | DataFormatException | IOException | ClassNotFoundException e) {
                    errorCount++;
                }
                LOGGER.log(Level.SEVERE, "Sentinel validation failed: {0}", sentinelUUID);
            }

            try {
                consumer.accept(CompressionUtils.compressObjectToBytesArray(preProcessedMessages));
            } catch (IOException ex) {
                LOGGER.log(Level.SEVERE, "Exception Caught: {0}", ex);
            }
        });
    }

    @Override
    public void cleanup() {
        errorCount = 0;
        successEventCount = 0;
        preProcessedEventCount = 0;
        invalidSentinelEventCount = 0;
        inactiveDeviceEventCount = 0;
    }

    private void initializeSyslogCountTimer() {
        try {
            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    LOGGER.log(Level.INFO, "Pre-Processed Events between {0} and {1} : [Total: {2}, Success: {3}, Invalid Sentinel: {4}, Inactive Device: {5}, Error: {6}]",
                            new Object[]{new Date(loggingTimeInMilliSeconds), new Date(), preProcessedEventCount, successEventCount, invalidSentinelEventCount, inactiveDeviceEventCount, errorCount});
                    loggingTimeInMilliSeconds = System.currentTimeMillis();
                    errorCount = 0;
                    preProcessedEventCount = 0;
                    successEventCount = 0;
                    invalidSentinelEventCount = 0;
                    inactiveDeviceEventCount = 0;
                }
            }, SYSLOG_COUNT_TIMER, SYSLOG_COUNT_TIMER);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Exception Caught: {0}", e);
        }
    }

    private void initializeDatabaseLookup() {
        try {
            DatabaseTablesLookup databaseTablesLookup = DatabaseTablesLookup.getInstance(true, false, false);
            Timer timer = new Timer();
            timer.schedule(databaseTablesLookup, DATABASE_LOOKUP_INTERVAL, DATABASE_LOOKUP_INTERVAL);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Exception Caught: {0}", e);
        }
    }
}
