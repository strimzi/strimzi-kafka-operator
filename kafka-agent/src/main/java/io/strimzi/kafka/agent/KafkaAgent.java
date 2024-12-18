/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.agent;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.MetricsRegistryListener;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * A very simple Java agent which polls the value of the {@code kafka.server:type=KafkaServer,name=BrokerState}
 * Yammer Metric and once it reaches the value 3 (meaning "running as broker", see {@code kafka.server.BrokerState}),
 * creates a given file. It exposes a REST endpoint for broker metrics and readiness check used by KRaft mode.
 * <dl>
 *     <dt>{@code GET /v1/broker-state}</dt>
 *     <dd>Reflects the BrokerState metric, returning a JSON response e.g. {"brokerState": 3}.
 *      If broker state is RECOVERY(2), it includes remainingLogsToRecover and remainingLogsToRecover in the response e.g.
 *      {"brokerState": 2,
 *       "recovery": {
 *          "remainingLogsToRecover": 123,
 *          "remainingSegmentsToRecover": 456
 *        }
 *      }</dd>
 *     <dt>{@code GET /v1/ready}</dt>
 *     <dd>Returns HTTP code 204 if broker state is RUNNING(3). Otherwise returns non successful HTTP code.
 *     </dd>
 * </dl>
 */
public class KafkaAgent {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAgent.class);
    private static final String BROKER_STATE_PATH = "/v1/broker-state";
    private static final String READINESS_ENDPOINT_PATH = "/v1/ready";
    private static final int HTTPS_PORT = 8443;
    private static final int HTTP_PORT = 8080;
    private static final long GRACEFUL_SHUTDOWN_TIMEOUT_MS = 30 * 1000;

    // KafkaYammerMetrics class in Kafka 3.3+
    private static final String YAMMER_METRICS_IN_KAFKA_3_3_AND_LATER = "org.apache.kafka.server.metrics.KafkaYammerMetrics";

    private static final byte BROKER_RUNNING_STATE = 3;
    private static final byte BROKER_RECOVERY_STATE = 2;
    private static final byte BROKER_UNKNOWN_STATE = 127;
    private String sslKeyStorePath;
    private String sslKeyStorePassword;
    private String sslTruststorePath;
    private String sslTruststorePassword;
    private MetricName brokerStateName;
    private Gauge brokerState;
    private Gauge remainingLogsToRecover;
    private Gauge remainingSegmentsToRecover;

    /**
     * Constructor of the KafkaAgent
     *
     * @param sslKeyStorePath       Keystore containing the broker certificate
     * @param sslKeyStorePass       Password for keystore
     * @param sslTruststorePath     Truststore containing CA certs for authenticating clients
     * @param sslTruststorePass     Password for truststore
     */
    /* test */ KafkaAgent(String sslKeyStorePath, String sslKeyStorePass, String sslTruststorePath, String sslTruststorePass) {
        this.sslKeyStorePath = sslKeyStorePath;
        this.sslKeyStorePassword = sslKeyStorePass;
        this.sslTruststorePath = sslTruststorePath;
        this.sslTruststorePassword = sslTruststorePass;
    }

    /**
     * Constructor of the KafkaAgent
     *
     * @param brokerState                   Current state of the broker
     * @param remainingLogsToRecover        Number of remaining logs to recover
     * @param remainingSegmentsToRecover    Number of remaining segments to recover
     */
    /* test */ KafkaAgent(Gauge brokerState, Gauge remainingLogsToRecover, Gauge remainingSegmentsToRecover) {
        this.brokerState = brokerState;
        this.remainingLogsToRecover = remainingLogsToRecover;
        this.remainingSegmentsToRecover = remainingSegmentsToRecover;
    }

    private void run() {
        try {
            startHttpServer();
        } catch (Exception e) {
            LOGGER.error("Could not start the server for broker state: ", e);
            throw new RuntimeException(e);
        }

        LOGGER.info("Starting metrics registry");
        MetricsRegistry metricsRegistry = metricsRegistry();

        metricsRegistry.addListener(new MetricsRegistryListener() {
            @Override
            public void onMetricRemoved(MetricName metricName) {
            }

            @Override
            public synchronized void onMetricAdded(MetricName metricName, Metric metric) {
                LOGGER.debug("Metric added {}", metricName);
                if (isBrokerState(metricName) && metric instanceof Gauge) {
                    brokerStateName = metricName;
                    brokerState = (Gauge) metric;
                } else if (isRemainingLogsToRecover(metricName) && metric instanceof Gauge) {
                    remainingLogsToRecover = (Gauge) metric;
                } else if (isRemainingSegmentsToRecover(metricName) && metric instanceof Gauge) {
                    remainingSegmentsToRecover = (Gauge) metric;
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
            throw new RuntimeException("Failed to find Yammer Metrics class", e);
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

    private boolean isBrokerState(MetricName name) {
        return "BrokerState".equals(name.getName())
                && "kafka.server".equals(name.getGroup())
                && "KafkaServer".equals(name.getType());
    }
    private boolean isRemainingLogsToRecover(MetricName name) {
        return "remainingLogsToRecover".equals(name.getName())
                && "kafka.log".equals(name.getGroup())
                && "LogManager".equals(name.getType());
    }
    private boolean isRemainingSegmentsToRecover(MetricName name) {
        return "remainingSegmentsToRecover".equals(name.getName())
                && "kafka.log".equals(name.getGroup())
                && "LogManager".equals(name.getType());
    }

    private void startHttpServer() throws Exception {
        Server server = new Server();

        HttpConfiguration https = new HttpConfiguration();
        https.addCustomizer(new SecureRequestCustomizer());
        ServerConnector httpsConn = new ServerConnector(server,
                new SslConnectionFactory(getSSLContextFactory(), "http/1.1"),
                new HttpConnectionFactory(https));
        httpsConn.setHost("0.0.0.0");
        httpsConn.setPort(HTTPS_PORT);

        ContextHandler brokerStateContext = new ContextHandler(BROKER_STATE_PATH);
        brokerStateContext.setHandler(getBrokerStateHandler());

        ServerConnector httpConn  = new ServerConnector(server);
        // The HTTP port should not be exposed outside the Pod, so it listens only on localhost
        httpConn.setHost("localhost");
        httpConn.setPort(HTTP_PORT);

        ContextHandler readinessContext = new ContextHandler(READINESS_ENDPOINT_PATH);
        readinessContext.setHandler(getReadinessHandler());

        server.setConnectors(new Connector[] {httpsConn, httpConn});
        server.setHandler(new ContextHandlerCollection(brokerStateContext, readinessContext));

        server.setStopTimeout(GRACEFUL_SHUTDOWN_TIMEOUT_MS);
        server.setStopAtShutdown(true);
        server.start();
    }

    /**
     * Creates a Handler instance to handle incoming HTTP requests for the broker state
     *
     * @return Handler
     */
    /* test */ Handler getBrokerStateHandler() {
        return new AbstractHandler() {
            @Override
            public void handle(String s, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException {
                response.setContentType("application/json");
                response.setCharacterEncoding("UTF-8");
                baseRequest.setHandled(true);

                Map<String, Object> brokerStateResponse = new HashMap<>();
                if (brokerState != null) {
                    if ((byte) brokerState.value() == BROKER_RECOVERY_STATE && remainingLogsToRecover != null && remainingSegmentsToRecover != null) {
                        Map<String, Object> recoveryState = new HashMap<>();
                        recoveryState.put("remainingLogsToRecover", remainingLogsToRecover.value());
                        recoveryState.put("remainingSegmentsToRecover", remainingSegmentsToRecover.value());
                        brokerStateResponse.put("brokerState", brokerState.value());
                        brokerStateResponse.put("recoveryState", recoveryState);
                    } else {
                        brokerStateResponse.put("brokerState", brokerState.value());
                    }

                    response.setStatus(HttpServletResponse.SC_OK);
                    String json = new ObjectMapper().writeValueAsString(brokerStateResponse);
                    response.getWriter().print(json);
                } else {
                    response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                    response.getWriter().print("Broker state metric not found");
                }
            }
        };
    }

    private SslContextFactory getSSLContextFactory() {
        SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();

        sslContextFactory.setKeyStorePath(sslKeyStorePath);
        sslContextFactory.setKeyStorePassword(sslKeyStorePassword);
        sslContextFactory.setKeyManagerPassword(sslKeyStorePassword);

        sslContextFactory.setTrustStorePath(sslTruststorePath);
        sslContextFactory.setTrustStorePassword(sslTruststorePassword);
        sslContextFactory.setNeedClientAuth(true);
        return  sslContextFactory;
    }

    /**
     * Creates a Handler instance to handle incoming HTTP requests for readiness check
     *
     * @return Handler
     */
    /* test */ Handler getReadinessHandler() {
        return new AbstractHandler() {
            @Override
            public void handle(String s, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException {
                response.setContentType("application/json");
                response.setCharacterEncoding("UTF-8");
                baseRequest.setHandled(true);
                if (brokerState != null) {
                    byte observedState = (byte) brokerState.value();
                    boolean stateIsRunning = BROKER_RUNNING_STATE <= observedState && BROKER_UNKNOWN_STATE != observedState;
                    if (stateIsRunning) {
                        LOGGER.trace("Broker is in running according to {}. The current state is {}", brokerStateName, observedState);
                        response.setStatus(HttpServletResponse.SC_NO_CONTENT);
                    } else {
                        LOGGER.trace("Broker is not running according to {}. The current state is {}", brokerStateName, observedState);
                        response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
                        response.getWriter().print("Readiness failed: brokerState is " + observedState);
                    }
                } else {
                    LOGGER.warn("Broker state metric not found");
                    response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                    response.getWriter().print("Broker state metric not found");
                }
            }
        };
    }

    /**
     * Agent entry point
     * @param agentArgs The agent arguments
     */
    public static void premain(String agentArgs) {
        String[] args = agentArgs.split(":");
        if (args.length != 1) {
            LOGGER.error("Not enough arguments to parse {}", agentArgs);
            System.exit(1);
        } else {
            final Properties agentProperties = new Properties();
            final Map<String, String> agentConfigs = new HashMap<>();

            try (FileInputStream fis = new FileInputStream(args[0])) {
                agentProperties.load(fis);
                for (String key : agentProperties.stringPropertyNames()) {
                    agentConfigs.put(key, agentProperties.getProperty(key));
                }
            } catch (IOException e) {
                LOGGER.error("Could not read and parse properties file {}", args[0]);
                System.exit(1);
            }

            final String sslKeyStorePath = agentConfigs.get("sslKeyStorePath");
            final String sslKeyStorePass = agentConfigs.get("sslKeyStorePass");
            final String sslTrustStorePath = agentConfigs.get("sslTrustStorePath");
            final String sslTrustStorePass = agentConfigs.get("sslTrustStorePass");
            if (sslKeyStorePath.isEmpty() || sslTrustStorePath.isEmpty()) {
                LOGGER.error("SSLKeyStorePath or SSLTrustStorePath is empty: sslKeyStorePath={} sslTrustStore={} ", sslKeyStorePath, sslTrustStorePath);
                System.exit(1);
            } else if (sslKeyStorePass.isEmpty()) {
                LOGGER.error("Keystore password is empty");
                System.exit(1);
            } else if (sslTrustStorePass.isEmpty()) {
                LOGGER.error("Truststore password is empty");
                System.exit(1);
            } else {
                LOGGER.info("Starting KafkaAgent with sslKeyStorePath={} and sslTrustStore={}", sslKeyStorePath, sslTrustStorePath);
                new KafkaAgent(sslKeyStorePath, sslKeyStorePass, sslTrustStorePath, sslTrustStorePass).run();
            }
        }
    }
}
