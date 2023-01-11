/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.agent;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.MetricsRegistryListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * A very simple Java agent which polls the value of the {@code kafka.server:type=KafkaServer,name=BrokerState}
 * Yammer Metric and once it reaches the value 3 (meaning "running as broker", see {@code kafka.server.BrokerState}),
 * creates a given file.
 * The presence of this file is tested via a Kube "exec" readiness probe to determine when the broker is ready.
 */
public class KafkaAgent {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAgent.class);

    // KafkaYammerMetrics class in Kafka 3.3+
    private static final String YAMMER_METRICS_IN_KAFKA_3_3_AND_LATER = "org.apache.kafka.server.metrics.KafkaYammerMetrics";
    // KafkaYammerMetrics class in Kafka 3.2-
    private static final String YAMMER_METRICS_IN_KAFKA_3_2_AND_EARLIER = "kafka.metrics.KafkaYammerMetrics";

    private static final byte BROKER_RUNNING_STATE = 3;
    private static final byte BROKER_UNKNOWN_STATE = 127;

    private final File sessionConnectedFile;
    private final File brokerReadyFile;
    private MetricName brokerStateName;
    private Gauge brokerState;
    private MetricName sessionStateName;
    private Gauge sessionState;

    /**
     * Constructor of the KafkaAgent
     *
     * @param brokerReadyFile       File which is touched (created) when the broker is ready
     * @param sessionConnectedFile  File which is touched (created) when the Kafka broker connects successfully to ZooKeeper
     */
    public KafkaAgent(File brokerReadyFile, File sessionConnectedFile) {
        this.brokerReadyFile = brokerReadyFile;
        this.sessionConnectedFile = sessionConnectedFile;
    }

    private void run() {
        LOGGER.info("Starting metrics registry");
        MetricsRegistry metricsRegistry = metricsRegistry();

        metricsRegistry.addListener(new MetricsRegistryListener() {
            @Override
            public void onMetricRemoved(MetricName metricName) {
            }

            @Override
            public synchronized void onMetricAdded(MetricName metricName, Metric metric) {
                LOGGER.trace("Metric added {}", metricName);
                if ("kafka.server".equals(metricName.getGroup())) {
                    if ("KafkaServer".equals(metricName.getType())
                            && "BrokerState".equals(metricName.getName())
                            && metric instanceof Gauge) {
                        LOGGER.debug("Metric {} added ", metricName);
                        brokerStateName = metricName;
                        brokerState = (Gauge) metric;
                    } else if ("SessionExpireListener".equals(metricName.getType())
                            && "SessionState".equals(metricName.getName())
                            && metric instanceof Gauge) {
                        sessionStateName = metricName;
                        sessionState = (Gauge) metric;
                    }
                }
                if (brokerState != null
                        && sessionState != null) {
                    metricsRegistry.removeListener(this);
                    LOGGER.info("Starting poller");
                    Thread pollerThread = new Thread(poller(),
                            "KafkaAgentPoller");
                    pollerThread.setDaemon(true);
                    pollerThread.start();
                }
            }
        });
    }

    /**
     * Acquires the MetricsRegistry from the KafkaYammerMetrics class. Depending on the Kafka version we are on, it will
     * use reflection to use the right class to get it.
     *
     * @return  Metrics Registry object
     */
    private MetricsRegistry metricsRegistry()   {
        Object metricsRegistry;
        Class<?> yammerMetrics;

        try {
            // First we try to get the KafkaYammerMetrics class for Kafka 3.3+
            yammerMetrics = Class.forName(YAMMER_METRICS_IN_KAFKA_3_3_AND_LATER);
            LOGGER.info("Found class {} for Kafka 3.3 and newer.", YAMMER_METRICS_IN_KAFKA_3_3_AND_LATER);
        } catch (ClassNotFoundException e)    {
            LOGGER.info("Class {} not found. We are probably on Kafka 3.2 or older.", YAMMER_METRICS_IN_KAFKA_3_3_AND_LATER);

            // We did not find the KafkaYammerMetrics class from Kafka 3.3+. So we are probably on older Kafka version
            //     => we will try the older class for KAfka 3.2-.
            try {
                yammerMetrics = Class.forName(YAMMER_METRICS_IN_KAFKA_3_2_AND_EARLIER);
                LOGGER.info("Found class {} for Kafka 3.2 and older.", YAMMER_METRICS_IN_KAFKA_3_2_AND_EARLIER);
            } catch (ClassNotFoundException e2) {
                // No class was found for any Kafka version => we should fail
                LOGGER.error("Class {} not found. We are not on Kafka 3.2 or earlier either.", YAMMER_METRICS_IN_KAFKA_3_2_AND_EARLIER);
                throw new RuntimeException("Failed to find Yammer Metrics class", e2);
            }
        }

        try {
            Method method = yammerMetrics.getMethod("defaultRegistry");
            metricsRegistry = method.invoke(null);
        } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException("Failed to get metrics registry", e);
        }

        if (metricsRegistry instanceof MetricsRegistry) {
            return (MetricsRegistry) metricsRegistry;
        } else {
            throw new RuntimeException("Metrics registry does not have the expected type");
        }
    }

    private Runnable poller() {
        return new Runnable() {
            int i = 0;

            @Override
            public void run() {
                while (true) {
                    handleSessionState();

                    if (handleBrokerState()) {
                        break;
                    }

                    try {
                        Thread.sleep(1000L);
                    } catch (InterruptedException e) {
                        // In theory this should never normally happen
                        LOGGER.warn("Unexpectedly interrupted");
                        break;
                    }
                }
                LOGGER.debug("Exiting thread");
            }

            boolean handleBrokerState() {
                LOGGER.trace("Polling {}", brokerStateName);
                boolean ready = false;
                byte observedState = (byte) brokerState.value();

                boolean stateIsRunning = BROKER_RUNNING_STATE <= observedState && BROKER_UNKNOWN_STATE != observedState;
                if (stateIsRunning) {
                    try {
                        LOGGER.trace("Running as server according to {} => ready", brokerStateName);
                        touch(brokerReadyFile);
                    } catch (IOException e) {
                        LOGGER.error("Could not write readiness file {}", brokerReadyFile, e);
                    }
                    ready = true;
                } else if (i++ % 60 == 0) {
                    LOGGER.debug("Metric {} = {}", brokerStateName, observedState);
                }
                return ready;
            }

            void handleSessionState() {
                LOGGER.trace("Polling {}", sessionStateName);
                String sessionStateStr = String.valueOf(sessionState.value());
                if ("CONNECTED".equals(sessionStateStr)) {
                    if (!sessionConnectedFile.exists()) {
                        try {
                            touch(sessionConnectedFile);
                        } catch (IOException e) {
                            LOGGER.error("Could not write session connected file {}", sessionConnectedFile, e);
                        }
                    }
                } else {
                    if (sessionConnectedFile.exists() && !sessionConnectedFile.delete()) {
                        LOGGER.error("Could not delete session connected file {}", sessionConnectedFile);
                    }
                    if (i++ % 60 == 0) {
                        LOGGER.debug("Metric {} = {}", sessionStateName, sessionStateStr);
                    }
                }
            }
        };
    }

    private void touch(File file) throws IOException {
        try (FileOutputStream out = new FileOutputStream(file)) {
            file.deleteOnExit();
        }
    }

    /**
     * Agent entry point
     * @param agentArgs The agent arguments
     */
    public static void premain(String agentArgs) {
        int index = agentArgs.indexOf(':');
        if (index == -1) {
            LOGGER.error("Unable to parse arguments {}", agentArgs);
            System.exit(1);
        } else {
            File brokerReadyFile = new File(agentArgs.substring(0, index));
            File sessionConnectedFile = new File(agentArgs.substring(index + 1));
            if (brokerReadyFile.exists() && !brokerReadyFile.delete()) {
                LOGGER.error("Broker readiness file already exists and could not be deleted: {}", brokerReadyFile);
                System.exit(1);
            } else if (sessionConnectedFile.exists() && !sessionConnectedFile.delete()) {
                LOGGER.error("Session connected file already exists and could not be deleted: {}", sessionConnectedFile);
                System.exit(1);
            } else {
                LOGGER.info("Starting KafkaAgent with brokerReadyFile={} and sessionConnectedFile={}", brokerReadyFile, sessionConnectedFile);
                new KafkaAgent(brokerReadyFile, sessionConnectedFile).run();
            }
        }
    }
}
