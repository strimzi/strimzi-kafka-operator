/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.operator.common.ReconciliationLogger;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A source connection used for testing which can be configured to be slow at doing things or fail.
 */
public class TestingConnector extends SourceConnector {

    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(TestingConnector.class);
    public static final String FAIL_ON_START = "fail.on.start";
    public static final String TASK_FAIL_ON_START = "task.fail.on.start";
    public static final String START_TIME_MS = "start.time.ms";
    public static final String STOP_TIME_MS = "stop.time.ms";
    public static final String TASK_START_TIME_MS = "task.start.time.ms";
    public static final String TASK_STOP_TIME_MS = "task.stop.time.ms";
    public static final String TASK_POLL_TIME_MS = "task.poll.time.ms";
    public static final String TASK_POLL_RECORDS = "task.poll.records";
    public static final String TOPIC_NAME = "topic.name";
    public static final String NUM_PARTITIONS = "num.partitions";

    private boolean taskFailOnStart;
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
            LOGGER.infoOp("Starting task {}", this);
            long taskStartTime = getLong(map, TASK_START_TIME_MS);
            taskStopTime = getLong(map, TASK_STOP_TIME_MS);
            taskPollTime = getLong(map, TASK_POLL_TIME_MS);
            taskPollRecords = getLong(map, TASK_POLL_RECORDS);
            topicName = map.get(TOPIC_NAME);
            numPartitions = Integer.parseInt(map.get(NUM_PARTITIONS));
            LOGGER.infoOp("Sleeping for {}ms", taskStartTime);
            sleep(taskStartTime);
            if (getBoolean(map, TASK_FAIL_ON_START)) {
                LOGGER.infoOp("Failing task {}", this);
                throw new ConnectException("Task failed to start");
            }
            LOGGER.infoOp("Started task {}", this);
        }

        @Override
        public List<SourceRecord> poll() throws InterruptedException {
            LOGGER.infoOp("Poll {}", this);
            LOGGER.infoOp("Sleeping for {}ms in poll", taskPollTime);
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
            LOGGER.warnOp("Returning {} records for topic {} from poll", taskPollRecords, topicName);
            return records;
        }

        @Override
        public void stop() {
            LOGGER.infoOp("Stopping task {}", this);
            sleep(taskStopTime);
        }
    }

    @Override
    public void start(Map<String, String> map) {
        LOGGER.infoOp("Starting connector {}", this);
        taskFailOnStart = getBoolean(map, TASK_FAIL_ON_START);
        long startTime = getLong(map, START_TIME_MS);
        stopTime = getLong(map, STOP_TIME_MS);
        taskStartTime = getLong(map, TASK_START_TIME_MS);
        taskStopTime = getLong(map, TASK_STOP_TIME_MS);
        taskPollTime = getLong(map, TASK_POLL_TIME_MS);
        taskPollRecords = getLong(map, TASK_POLL_RECORDS);
        topicName = map.get(TOPIC_NAME);
        numPartitions = Integer.parseInt(map.get(NUM_PARTITIONS));
        sleep(startTime);
        if (getBoolean(map, FAIL_ON_START)) {
            LOGGER.infoOp("Failing connector {}", this);
            throw new RuntimeException("Failed to start connector");
        }
        LOGGER.infoOp("Started connector {}", this);
    }

    private static void sleep(long ms) {
        if (ms > 0) {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                LOGGER.warnOp("Interrupted during sleep", e);
            }
        }
    }

    private static boolean getBoolean(Map<String, String> map, String configName) {
        return Boolean.parseBoolean(map.get(configName));
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
        taskConfig.put(TASK_FAIL_ON_START, Boolean.toString(taskFailOnStart));
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
        LOGGER.infoOp("Stopping connector {}", this);
        sleep(stopTime);
        LOGGER.infoOp("Stopped connector {}", this);
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(FAIL_ON_START, ConfigDef.Type.BOOLEAN, false, new ConfigDef.NonNullValidator(),
                        ConfigDef.Importance.MEDIUM, "Whether start() should fail")
                .define(TASK_FAIL_ON_START, ConfigDef.Type.BOOLEAN, false, new ConfigDef.NonNullValidator(),
                        ConfigDef.Importance.MEDIUM, "Whether task start() should fail")
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
