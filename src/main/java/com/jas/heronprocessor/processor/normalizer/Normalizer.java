package com.jas.heronprocessor.processor.normalizer;

import com.twitter.heron.streamlet.Context;
import com.twitter.heron.streamlet.SerializableTransformer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.DataFormatException;
public class Normalizer implements SerializableTransformer {

    private static final Logger LOGGER = Logger.getLogger("Normalizer");


    public static long matchedEventsCount;
    public static long unmatchedEventsCount;
    public static long auditEventsCount;

    private static final long DATABASE_LOOKUP_INTERVAL = 15 * 60 * 1000;
    private static final long SYSLOG_COUNT_TIMER = 1 * 60 * 1000;
    private static long loggingTimeInMilliSeconds = System.currentTimeMillis();

    @Override
    public void setup(Context context) {

        initializeDatabaseLookup();

        matchedEventsCount = 0;
        auditEventsCount = 0;
        unmatchedEventsCount = 0;

        initializeSyslogCountTimer();
    }

    @Override
    public void transform(Object tuple, Consumer consumer) {
        List<RawlogMessage> preProcessedMessages = null;
        try {
            preProcessedMessages = (List<RawlogMessage>) CompressionUtils.decompressObjectFromBytesArray((byte[]) tuple);
        } catch (DataFormatException | IOException | ClassNotFoundException ex) {
            LOGGER.log(Level.SEVERE, "Exception Caught: {0}", ex);
        }

        if (preProcessedMessages != null && preProcessedMessages.size() > 0) {

            long normalizationStartTime = System.currentTimeMillis();

            List<EventLogDO> eventLogVO = new ArrayList<>();

            Set<String> usedNormalizers = new HashSet<>();
            Set<Integer> deviceIds = new HashSet<>();
            int matched = 0;
            int unmatched = 0;

            for (RawlogMessage rawlogMessage : preProcessedMessages) {
                if (rawlogMessage != null) {

                    usedNormalizers.add(rawlogMessage.getNormalizerName());
                    deviceIds.add(rawlogMessage.getDeviceId());

                    EventLogDO eventLogDO = rawlogNormalizer.rawMessageProcess(rawlogMessage);

                    if (("unknown").equals(eventLogDO.getEvent_type().getName())) {
                        unmatchedEventsCount++;
                        unmatched++;
                    } else if (("portal-audit").equals(eventLogDO.getEvent_type().getName())) {
                        auditLogUpdater.insertRecord(eventLogDO);
                        auditEventsCount++;
                    } else {
                        matchedEventsCount++;
                        matched++;
                    }

                    if (!("portal-audit").equals(eventLogDO.getEvent_type().getName())) {
                        eventLogVO.add(eventLogDO);
                    }
                }
            }

            int sizeOfSyslogVo = eventLogVO.size();
            if (sizeOfSyslogVo > 0) {
                EventLogDO firstEventOfVo = eventLogVO.get(0);

                long timeTaken = System.currentTimeMillis() - normalizationStartTime;

                if (sizeOfSyslogVo > 2000 && (timeTaken > (sizeOfSyslogVo / 2) || (unmatched >= matched))) {
                    LOGGER.log(Level.INFO, "[CID->{0}, SID->{1}, Date->{2}, Count->{3}, Matched->{4}, Unmatched->{5}, Time->{6}ms, Normalizers->{7}, DID->{8}]",
                            new Object[]{String.valueOf(firstEventOfVo.getCustomer().getId()),
                                String.valueOf(firstEventOfVo.getSentinel().getId()),
                                firstEventOfVo.getEvent_received_date(),
                                preProcessedMessages.size(), matched, unmatched, timeTaken,
                                usedNormalizers, deviceIds});
                } else {
                    LOGGER.log(Level.INFO, "[CID->{0}, SID->{1}, Date->{2}, Count->{3}, Matched->{4}, Unmatched->{5}, Time->{6}ms]",
                            new Object[]{String.valueOf(firstEventOfVo.getCustomer().getId()),
                                String.valueOf(firstEventOfVo.getSentinel().getId()),
                                firstEventOfVo.getEvent_received_date(),
                                preProcessedMessages.size(), matched, unmatched, timeTaken});
                }
            }

            try {
                consumer.accept(CompressionUtils.compressObjectToBytesArray(eventLogVO));
            } catch (IOException ex) {
                LOGGER.log(Level.SEVERE, "Exception Caught: {0}", ex);
            }
        }
    }

    @Override
    public void cleanup() {
        matchedEventsCount = 0;
        auditEventsCount = 0;
        unmatchedEventsCount = 0;
    }

    private void initializeSyslogCountTimer() {
        try {
            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    LOGGER.log(Level.INFO, "Normalized Events between {0} and {1} : [Matched: {2}, Unmatched: {3}, Audit: {4}]",
                            new Object[]{new Date(loggingTimeInMilliSeconds), new Date(), matchedEventsCount, unmatchedEventsCount, auditEventsCount});
                    loggingTimeInMilliSeconds = System.currentTimeMillis();
                    matchedEventsCount = 0;
                    auditEventsCount = 0;
                    unmatchedEventsCount = 0;
                }
            }, SYSLOG_COUNT_TIMER, SYSLOG_COUNT_TIMER);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Exception Caught: {0}", e);
        }
    }

    private void initializeDatabaseLookup() {
        try {
            DatabaseTablesLookup databaseTablesLookup = DatabaseTablesLookup.getInstance(false, true, false);
            Timer timer = new Timer();
            timer.schedule(databaseTablesLookup, DATABASE_LOOKUP_INTERVAL, DATABASE_LOOKUP_INTERVAL);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Exception Caught: {0}", e);
        }
    }
}
