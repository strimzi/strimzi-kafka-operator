/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A source connection used for testing which can be configured to be slow at doing things.
 */
public class TestingConnector extends SourceConnector {

    private static final Logger LOGGER = LogManager.getLogger(TestingConnector.class);
    public static final String START_TIME_MS = "start.time.ms";
    public static final String STOP_TIME_MS = "stop.time.ms";
    public static final String TASK_START_TIME_MS = "task.start.time.ms";
    public static final String TASK_STOP_TIME_MS = "task.stop.time.ms";
    public static final String TASK_POLL_TIME_MS = "task.poll.time.ms";
    public static final String TASK_POLL_RECORDS = "task.poll.records";
    public static final String TOPIC_NAME = "topic.name";
    public static final String NUM_PARTITIONS = "num.partitions";

    private long stopTime;
    private long taskStartTime;
    private long taskStopTime;
    private long taskPollTime;
    private long taskPollRecords;
    private String topicName;
    private int numPartitions;

    public static class TestingTask extends SourceTask {

        private long taskStopTime;
        private long taskPollTime;
        private long record;
        private long taskPollRecords;
        private String topicName;
        private int numPartitions;

        @Override
        public String version() {
            return "0.0.1";
        }

        @Override
        public void start(Map<String, String> map) {
            LOGGER.info("Starting task {}", this);
            long taskStartTime = getLong(map, "task.start.time.ms");
            taskStopTime = getLong(map, "task.stop.time.ms");
            taskPollTime = getLong(map, "task.poll.time.ms");
            taskPollRecords = getLong(map, "task.poll.records");
            topicName = map.get("topic.name");
            numPartitions = Integer.parseInt(map.get("num.partitions"));
            LOGGER.info("Sleeping for {}ms", taskStartTime);
            sleep(taskStartTime);
            LOGGER.info("Started task {}", this);
        }

        @Override
        public List<SourceRecord> poll() throws InterruptedException {
            LOGGER.info("Poll {}", this);
            LOGGER.info("Sleeping for {}ms in poll", taskPollTime);
            sleep(taskPollTime);
            Schema valueSchema = new SchemaBuilder(Schema.Type.INT64).valueSchema();
            List<SourceRecord> records = new ArrayList<>();
            for (int i = 0; i < taskPollRecords; i++) {
                records.add(new SourceRecord(Collections.singletonMap("", ""),
                        Collections.singletonMap("", ""),
                        topicName, (int) record % numPartitions,
                        null, null,
                        valueSchema, record++));
            }
            LOGGER.warn("Returning {} records for topic {} from poll", taskPollRecords, topicName);
            return records;
        }

        @Override
        public void stop() {
            LOGGER.info("Stopping task {}", this);
            sleep(taskStopTime);
        }
    }

    @Override
    public void start(Map<String, String> map) {
        LOGGER.info("Starting connector {}", this);
        long startTime = getLong(map, START_TIME_MS);
        stopTime = getLong(map, STOP_TIME_MS);
        taskStartTime = getLong(map, TASK_START_TIME_MS);
        taskStopTime = getLong(map, TASK_STOP_TIME_MS);
        taskPollTime = getLong(map, TASK_POLL_TIME_MS);
        taskPollRecords = getLong(map, TASK_POLL_RECORDS);
        topicName = map.get(TOPIC_NAME);
        numPartitions = Integer.parseInt(map.get(NUM_PARTITIONS));
        sleep(startTime);
        LOGGER.info("Started connector {}", this);
    }

    private static void sleep(long ms) {
        if (ms > 0) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                LOGGER.warn("Interrupted during sleep", e);
            }
        }
    }

    private static long getLong(Map<String, String> map, String configName) {
        return Long.parseLong(map.get(configName));
    }

    @Override
    public Class<? extends Task> taskClass() {
        return TestingTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int count) {
        Map<String, String> taskConfig = new HashMap<>();
        taskConfig.put(TASK_START_TIME_MS, Long.toString(taskStartTime));
        taskConfig.put(TASK_STOP_TIME_MS, Long.toString(taskStopTime));
        taskConfig.put(TASK_POLL_TIME_MS, Long.toString(taskPollTime));
        taskConfig.put(TASK_POLL_RECORDS, Long.toString(taskPollRecords));
        taskConfig.put(TOPIC_NAME, topicName);
        taskConfig.put(NUM_PARTITIONS, Integer.toString(numPartitions));
        List<Map<String, String>> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            result.add(taskConfig);
        }
        return result;
    }

    @Override
    public void stop() {
        LOGGER.info("Stopping connector {}", this);
        sleep(stopTime);
        LOGGER.info("Stopped connector {}", this);
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(START_TIME_MS, ConfigDef.Type.LONG, 5_000L, new ConfigDef.NonNullValidator(),
                        ConfigDef.Importance.MEDIUM, "The time that start() should take to return")
                .define(STOP_TIME_MS, ConfigDef.Type.LONG, 5_000L, new ConfigDef.NonNullValidator(),
                        ConfigDef.Importance.MEDIUM, "The time that stop() should take to return")
                .define(TASK_START_TIME_MS, ConfigDef.Type.LONG, 5_000L, new ConfigDef.NonNullValidator(),
                        ConfigDef.Importance.MEDIUM, "The time that the task start() should take to return")
                .define(TASK_STOP_TIME_MS, ConfigDef.Type.LONG, 5_000L, new ConfigDef.NonNullValidator(),
                        ConfigDef.Importance.MEDIUM, "The time that the task stop() should take to return")
                .define(TASK_POLL_TIME_MS, ConfigDef.Type.LONG, 5_000L, new ConfigDef.NonNullValidator(),
                        ConfigDef.Importance.MEDIUM, "The time that the task poll() should take to return")
                .define(TASK_POLL_RECORDS, ConfigDef.Type.LONG, 1_000L, new ConfigDef.NonNullValidator(),
                        ConfigDef.Importance.MEDIUM, "The number of records that the task poll() should return")
                .define(TOPIC_NAME, ConfigDef.Type.STRING, "my-topic", new ConfigDef.NonNullValidator(),
                        ConfigDef.Importance.MEDIUM, "The name of the topic which the records are sent to")
                .define(NUM_PARTITIONS, ConfigDef.Type.INT, 1, new ConfigDef.NonNullValidator(),
                        ConfigDef.Importance.MEDIUM, "The number of partitions which the records are send to");
    }

    @Override
    public String version() {
        return "0.0.1";
    }
}
