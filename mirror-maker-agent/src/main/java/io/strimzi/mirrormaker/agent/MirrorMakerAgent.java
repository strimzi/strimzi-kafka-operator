/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.mirrormaker.agent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.JMException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Set;

/**
 * A Java agent which helps with the Readiness and Liveness check in Kafka Mirror Maker.
 *
 * Liveness:
 *   In every loop it touches a liveness file if it doesn't exist. The file is expected to tbe deleted by the Kubernetes
 *   liveness probe. So it should be periodically deleted and recreated.
 *
 * Readiness:
 *   Readiness checks the number of connections to the source and target Kafka clusters. If at least one connection
 *   exists to each of the clusters, the readiness file will be created. If not it will be deleted.
 */
public class MirrorMakerAgent {
    private static final Logger LOGGER = LoggerFactory.getLogger(MirrorMakerAgent.class);

    private final File livenessFile;
    private final File readinessFile;
    private final long readinessSleepInterval;
    private final long livenessSleepInterval;

    /**
     * Constructor of the MirrorMakerAgent
     *
     * @param readinessFile             File which is touched (created) when Mirror Maker 1 is ready
     * @param livenessFile              File which is touched (created) when Mirror Maker 1 is alive
     * @param readinessSleepInterval    Sleep interval of the readiness check
     * @param livenessSleepInterval     Sleep interval of the liveness check
     */
    public MirrorMakerAgent(File readinessFile, File livenessFile, long readinessSleepInterval, long livenessSleepInterval) {
        this.readinessFile = readinessFile;
        this.livenessFile = livenessFile;
        this.readinessSleepInterval = readinessSleepInterval;
        this.livenessSleepInterval = livenessSleepInterval;
    }

    /**
     * Starts two poller threads - one for readiness and one for liveness.
     */
    private void run() {
        LOGGER.info("Starting readiness poller");
        Thread readinessThread = new Thread(readinessPoller(), "ReadinessPoller");
        readinessThread.setDaemon(true);
        readinessThread.start();

        LOGGER.info("Starting liveness poller");
        Thread livenessThread = new Thread(livenessPoller(), "LivenessPoller");
        livenessThread.setDaemon(true);
        livenessThread.start();
    }

    /**
     * Creates the poller thread for the liveness check
     *
     * @return Runable for liveness check
     */
    private Runnable livenessPoller() {
        return () -> {
            while (true) {
                if (!livenessFile.exists()) {
                    try {
                        LOGGER.debug("Mirror Maker is alive");
                        touch(livenessFile);
                    } catch (IOException e) {
                        LOGGER.error("Could not write liveness file {}", livenessFile, e);
                    }
                }

                try {
                    Thread.sleep(livenessSleepInterval);
                } catch (InterruptedException e) {
                    // In theory this should never normally happen
                    LOGGER.warn("Unexpectedly interrupted");
                    break;
                }
            }
            LOGGER.debug("Exiting thread");
        };
    }

    /**
     * Creates the poller thread for the readiness check
     *
     * @return Runable for readiness check
     */
    private Runnable readinessPoller() {
        return new Runnable() {
            private final MBeanServerConnection beanConn = ManagementFactory.getPlatformMBeanServer();

            @Override
            public void run() {
                while (true) {
                    if (handleProducerConnected() && handleConsumerConnected()) {
                        try {
                            LOGGER.debug("Mirror Maker is ready");
                            touch(readinessFile);
                        } catch (IOException e) {
                            LOGGER.error("Could not write readiness file {}", readinessFile, e);
                        }
                    } else {
                        LOGGER.debug("Mirror Maker is not ready");

                        if (readinessFile.exists() && !readinessFile.delete()) {
                            LOGGER.error("Could not delete readiness indicator file {}", readinessFile);
                        }
                    }

                    try {
                        Thread.sleep(readinessSleepInterval);
                    } catch (InterruptedException e) {
                        // In theory this should never normally happen
                        LOGGER.warn("Unexpectedly interrupted");
                        break;
                    }
                }
                LOGGER.debug("Exiting thread");
            }

            /**
             * Gets the producer connections from JMX and counts them.
             *
             * @return True if at least one producer connections exists. False otherwise.
             */
            boolean handleProducerConnected() {
                LOGGER.debug("Polling for producer connections");
                Double connectionCount = 0.0D;

                try {
                    Set<ObjectName> mbeans = beanConn.queryNames(new ObjectName("kafka.producer:type=producer-metrics,client-id=*"), null);

                    for (ObjectName oName : mbeans) {
                        Double attr = (Double) beanConn.getAttribute(oName, "connection-count");
                        connectionCount += attr;
                        LOGGER.trace("Found connection metric with name {} and value: {}", oName, attr);
                    }
                }   catch (IOException | JMException e) {
                    LOGGER.error("Failed to query JMX metrics", e);
                }   finally {
                    LOGGER.trace("Total producer connections {}", connectionCount);
                }
                return connectionCount > 0;
            }

            /**
             * Gets the consumer connections from JMX and counts them.
             *
             * @return True if at least one consumer connections exists. False otherwise.
             */
            boolean handleConsumerConnected() {
                LOGGER.debug("Polling for consumer connections");
                Double connectionCount = 0.0D;

                try {
                    Set<ObjectName> mbeans = beanConn.queryNames(new ObjectName("kafka.consumer:type=consumer-metrics,client-id=*"), null);

                    for (ObjectName oName : mbeans) {
                        Double attr = (Double) beanConn.getAttribute(oName, "connection-count");
                        connectionCount += attr;
                        LOGGER.trace("Found connection metric with name {} and value: {}", oName, attr);
                    }
                }   catch (IOException | JMException e) {
                    LOGGER.error("Failed to query JMX metrics", e);
                }   finally {
                    LOGGER.trace("Total consumer connections {}", connectionCount);
                }
                return connectionCount > 0;
            }
        };
    }

    /**
     * Creates the file which indicates readiness or liveness.
     *
     * @param file  File which should be created
     *
     * @throws IOException if the file can't be created
     */
    private void touch(File file) throws IOException {
        try (FileOutputStream ignored = new FileOutputStream(file)) {
            file.deleteOnExit();
        }
    }

    /**
     * Agent entry point
     *
     * @param agentArgs The agent arguments
     */
    public static void premain(String agentArgs) {
        String[] args = agentArgs.split(":");

        if (args.length != 4) {
            LOGGER.error("Unexpected number of arguments ({}): {}", args.length, agentArgs);
            System.exit(1);
        } else {
            File mirrorMakerReadyFile = new File(args[0]);
            File livenessFile = new File(args[1]);

            if (mirrorMakerReadyFile.exists() && !mirrorMakerReadyFile.delete()) {
                LOGGER.error("Mirror Maker readiness file already exists and could not be deleted: {}", mirrorMakerReadyFile);
                System.exit(1);
            } else if (livenessFile.exists() && !livenessFile.delete()) {
                LOGGER.error("Liveness file already exists and could not be deleted: {}", livenessFile);
                System.exit(1);
            } else {
                long readinessSleepInterval = Long.parseLong(args[2]) / 2L * 1000L;
                long livenessSleepInterval = Long.parseLong(args[3]) / 2L * 1000L;

                new MirrorMakerAgent(mirrorMakerReadyFile, livenessFile, readinessSleepInterval, livenessSleepInterval).run();
            }
        }
    }
}
