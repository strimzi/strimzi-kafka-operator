/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.agent;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.MetricsRegistryListener;
import kafka.metrics.KafkaYammerMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * A very simple Java agent which polls the value of the {@code kafka.server:type=KafkaServer,name=BrokerState}
 * Yammer Metric and once it reaches the value 3 (meaning "running as broker", see {@code kafka.server.BrokerState}),
 * creates a given file.
 * The presence of this file is tested via a Kube "exec" readiness probe to determine when the broker is ready.
 */
public class KafkaAgent {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAgent.class);

    private final File sessionConnectedFile;
    private File brokerReadyFile;
    private MetricName brokerStateName;
    private Gauge brokerState;
    private MetricName sessionStateName;
    private Gauge sessionState;

    public KafkaAgent(File brokerReadyFile, File sessionConnectedFile) {
        this.brokerReadyFile = brokerReadyFile;
        this.sessionConnectedFile = sessionConnectedFile;
    }

    /**
     * Since Kafka 2.6.0, a new class KafkaYammerMetrics exists which has the default Metrics Registry. The old default
     * registry does not work there anymore. So if the new class exists, we use it and if it doesn't exist we use the
     * old one. More details can be found here: https://github.com/apache/kafka/blob/2.6.0/core/src/main/java/kafka/metrics/KafkaYammerMetrics.java
     *
     * Once we support only 2.6.0 and newer, we can clean this up and use only KafkaYammerMetrics all the time.
     *
     * @return  MetricsRegistry with Kafka metrics
     */
    private MetricsRegistry metricsRegistry()   {
        try {
            Class.forName("kafka.metrics.KafkaYammerMetrics");
            LOGGER.info("KafkaYammerMetrics found and will be used.");
            return KafkaYammerMetrics.defaultRegistry();
        } catch (ClassNotFoundException e) {
            LOGGER.info("KafkaYammerMetrics not found. Metrics will be used.");
            return Metrics.defaultRegistry();
        }
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
                Integer running = Integer.valueOf(3);
                Object value = brokerState.value();

                if ((value instanceof Integer && running.equals(value))
                        || (value instanceof Byte && running.equals(((Byte) value).intValue()))) {
                    try {
                        LOGGER.trace("Running as server according to {} => ready", brokerStateName);
                        touch(brokerReadyFile);
                    } catch (IOException e) {
                        LOGGER.error("Could not write readiness file {}", brokerReadyFile, e);
                    }
                    ready = true;

                } else if (i++ % 60 == 0) {
                    LOGGER.debug("Metric {} = {} (type: {})", brokerStateName, value, value.getClass());
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
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(file);
            file.deleteOnExit();
        } finally {
            if (out != null) {
                out.close();
            }
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
