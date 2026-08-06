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
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
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
 *      If broker state is RECOVERY(2), it includes remainingLogsToRecover and remainingSegmentsToRecover in the response e.g.
 *      {"brokerState": 2,
 *       "recoveryState": {
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
    private static final int EXTERNAL_PORT = 8443;
    private static final int INTERNAL_PORT = 8080;
    private static final long GRACEFUL_SHUTDOWN_TIMEOUT_MS = 30 * 1000;
    private static final byte BROKER_RUNNING_STATE = 3;
    private static final byte BROKER_RECOVERY_STATE = 2;
    private static final byte BROKER_UNKNOWN_STATE = 127;

    private final KubernetesClient client;
    private final Map<String, String> config;

    private MetricName brokerStateName;
    private Gauge brokerState;
    private Gauge remainingLogsToRecover;
    private Gauge remainingSegmentsToRecover;

    /**
     * Constructor of the KafkaAgent
     *
     * @param client    Kubernetes client instance
     * @param config    Map with Kafka Agent configurations
     */
    public KafkaAgent(KubernetesClient client, Map<String, String> config) {
        this.client = client;
        this.config = config;
    }

    /**
     * Constructor of the KafkaAgent
     *
     * @param client                        Kubernetes client instance
     * @param config                        Kafka agent configuration
     * @param brokerState                   Current state of the broker
     * @param remainingLogsToRecover        Number of remaining logs to recover
     * @param remainingSegmentsToRecover    Number of remaining segments to recover
     */
    /* test */ KafkaAgent(KubernetesClient client, Map<String, String> config, Gauge brokerState, Gauge remainingLogsToRecover, Gauge remainingSegmentsToRecover) {
        this(client, config);

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
     * Acquires the MetricsRegistry from the KafkaYammerMetrics class.
     *
     * @return  Metrics Registry object
     */
    private MetricsRegistry metricsRegistry()   {
        return KafkaYammerMetrics.defaultRegistry();
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

    /* test */ Server startHttpServer() throws Exception {
        Server server = new Server();

        // External connector is used by the Operator to check on the Kafka node
        // While the port is always 8443, TLS is used optionally depending on the configuration
        ServerConnector externalConnector = createExternalHttpConnector(server);

        // Internal connector is used within the Pod only for health checks
        ServerConnector internalConnector  = new ServerConnector(server);
        internalConnector.setHost("localhost"); // Should not be exposed outside the Pod. So we use localhost only here.
        internalConnector.setPort(INTERNAL_PORT);

        ContextHandler brokerStateContext = new ContextHandler(BROKER_STATE_PATH);
        brokerStateContext.setHandler(getBrokerStateHandler());

        ContextHandler readinessContext = new ContextHandler(READINESS_ENDPOINT_PATH);
        readinessContext.setHandler(getReadinessHandler());

        server.setConnectors(new Connector[] {externalConnector, internalConnector});
        server.setHandler(new ContextHandlerCollection(brokerStateContext, readinessContext));

        server.setStopTimeout(GRACEFUL_SHUTDOWN_TIMEOUT_MS);
        server.setStopAtShutdown(true);
        server.start();

        return server;
    }

    private ServerConnector createExternalHttpConnector(Server server) throws GeneralSecurityException, IOException {
        ServerConnector httpConnector;
        if (config.get("sslKeyStoreSecretName") != null) {
            HttpConfiguration externalHttp = new HttpConfiguration();
            externalHttp.addCustomizer(new SecureRequestCustomizer());
            httpConnector = new ServerConnector(server,
                    new SslConnectionFactory(getSSLContextFactory(config.get("namespace"), config.get("sslTrustStoreSecretName"), config.get("sslKeyStoreSecretName")), "http/1.1"),
                    new HttpConnectionFactory(externalHttp));
        } else {
            httpConnector  = new ServerConnector(server);
        }

        httpConnector.setHost("0.0.0.0");
        httpConnector.setPort(EXTERNAL_PORT);

        return httpConnector;
    }

    /**
     * Creates a Handler instance to handle incoming HTTP requests for the broker state
     *
     * @return Handler
     */
    private Handler getBrokerStateHandler() {
        return new Handler.Abstract() {
            @Override
            public boolean handle(Request request, Response response, Callback callback) throws Exception {
                response.getHeaders().put(HttpHeader.CONTENT_TYPE, "application/json; charset=UTF-8");

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
                    response.write(true, StandardCharsets.UTF_8.encode(json), callback);
                } else {
                    response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                    response.write(true, StandardCharsets.UTF_8.encode("Broker state metric not found"), callback);
                }

                return true;
            }
        };
    }

    private SslContextFactory.Server getSSLContextFactory(String namespace, String caCertSecretName, String nodeCertSecretName) throws GeneralSecurityException, IOException {
        SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();
        sslContextFactory.setKeyStore(KafkaAgentUtils.keyStore(getKubernetesSecret(namespace, nodeCertSecretName)));

        if (caCertSecretName != null) {
            sslContextFactory.setTrustStore(KafkaAgentUtils.trustStore(getKubernetesSecret(namespace, caCertSecretName)));
            sslContextFactory.setNeedClientAuth(true);
        }

        return  sslContextFactory;
    }

    private Secret getKubernetesSecret(String namespace, String caCertSecretName) {
        return client.secrets().inNamespace(namespace).withName(caCertSecretName).get();
    }

    /**
     * Creates a Handler instance to handle incoming HTTP requests for readiness check
     *
     * @return Handler
     */
    private Handler getReadinessHandler() {
        return new Handler.Abstract() {
            @Override
            public boolean handle(Request request, Response response, Callback callback) {
                response.getHeaders().put(HttpHeader.CONTENT_TYPE, "application/json; charset=UTF-8");

                if (brokerState != null) {
                    byte observedState = (byte) brokerState.value();
                    boolean stateIsRunning = BROKER_RUNNING_STATE <= observedState && BROKER_UNKNOWN_STATE != observedState;
                    if (stateIsRunning) {
                        LOGGER.trace("Broker is running according to {}. The current state is {}", brokerStateName, observedState);
                        response.setStatus(HttpServletResponse.SC_NO_CONTENT);
                        response.write(true, null, callback);
                    } else {
                        LOGGER.trace("Broker is not running according to {}. The current state is {}", brokerStateName, observedState);
                        response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
                        response.write(true, StandardCharsets.UTF_8.encode("Readiness failed: brokerState is " + observedState), callback);
                    }
                } else {
                    LOGGER.warn("Broker state metric not found");
                    response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                    response.write(true, StandardCharsets.UTF_8.encode("Broker state metric not found"), callback);
                }

                return true;
            }
        };
    }

    /**
     * Agent entry point
     * @param agentArgs The agent arguments
     */
    @SuppressWarnings("unused")
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
                LOGGER.error("Could not read and parse properties file {}", args[0], e);
                System.exit(1);
            }

            LOGGER.info("Starting KafkaAgent with configuration {}", agentConfigs);
            KubernetesClient client = new KubernetesClientBuilder().build();
            new KafkaAgent(client, agentConfigs).run();
        }
    }
}
