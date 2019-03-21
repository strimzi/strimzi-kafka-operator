/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.agent;

import com.yammer.metrics.Metrics;
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

/**
 * A very simple Java agent which polls the value of the {@code kafka.server:type=KafkaServer,name=BrokerState}
 * Yammer Metric and once it reaches the value 3 (meaning "running as broker", see {@code kafka.server.BrokerState}),
 * creates a given file.
 * The presence of this file is tested via a Kube "exec" readiness probe to determine when the broker is ready.
 */
public class KafkaAgent {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAgent.class);

    public static void premain(String agentArgs) {

        File filename = new File(agentArgs != null && !agentArgs.isEmpty() ? agentArgs : "/tmp/kafka-ready");
        if (filename.exists()) {
            LOGGER.error(KafkaAgent.class.getName() + ": File already exists: " + filename);
        } else {
            MetricsRegistry metricsRegistry = Metrics.defaultRegistry();
            metricsRegistry.addListener(new MetricsRegistryListener() {
                @Override
                public void onMetricRemoved(MetricName metricName) {
                }

                @Override
                public void onMetricAdded(MetricName metricName, Metric metric) {
                    LOGGER.trace("Metric added {}", metricName);
                    if ("kafka.server".equals(metricName.getGroup())
                            && "KafkaServer".equals(metricName.getType())
                            && "BrokerState".equals(metricName.getName())) {
                        LOGGER.debug("Metric {} added ", metricName);
                        new Thread(poller(metricName, (Gauge) metric, filename),
                                "KubernetesReadinessPoller").start();
                    }
                }
            });
        }
    }

    private static Runnable poller(MetricName metricName, Gauge metric, File filename) {
        return () -> {
            int i = 0;
            while (true) {
                Integer running = Integer.valueOf(3);
                Object value = metric.value();
                if (running.equals(value)) {
                    try {
                        LOGGER.info("Running as server according to {} => ready", metricName);
                        new FileOutputStream(filename).close();
                        break;
                    } catch (IOException e) {
                        LOGGER.error("Could not write readiness file {}", filename, e);
                        break;
                    }
                } else if (i++ % 60 == 0) {
                    LOGGER.debug("Metric {} = {}", metricName, value);
                }
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    // In theory this should never normally happen
                    LOGGER.warn("Unexepctedly interrupted");
                    break;
                }
            }
            LOGGER.debug("Exiting thread");
        };
    }
}
