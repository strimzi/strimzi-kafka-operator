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

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;

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
 *     <dt>{@code GET /v1/controller-ready}</dt>
 *     <dd>Returns HTTP code 204 if the KRaft controller node is attached to the metadata quorum,
 *      i.e. the {@code current-state} attribute of the {@code kafka.server:type=raft-metrics} JMX MBean
 *      is one of {@code leader} or {@code follower}. Returns 503 for any other state (e.g. {@code unattached},
 *      {@code candidate}, {@code voted}, {@code resigned}, {@code observer}). Returns 404 if the raft-metrics MBean
 *      is not registered (e.g. broker-only nodes before broker startup, or older Kafka versions without raft-metrics).
 *      Intended to back the controller-only readiness probe — distinguishes "port 9090 is listening" from
 *      "this controller is actually participating in the quorum."
 *     </dd>
 * </dl>
 */
public class KafkaAgent {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaAgent.class);
    private static final String BROKER_STATE_PATH = "/v1/broker-state";
    private static final String READINESS_ENDPOINT_PATH = "/v1/ready";
    private static final String CONTROLLER_READINESS_ENDPOINT_PATH = "/v1/controller-ready";
    private static final int HTTPS_PORT = 8443;
    private static final int HTTP_PORT = 8080;
    private static final long GRACEFUL_SHUTDOWN_TIMEOUT_MS = 30 * 1000;

    // KafkaYammerMetrics class in Kafka 3.3+
    private static final String YAMMER_METRICS_IN_KAFKA_3_3_AND_LATER = "org.apache.kafka.server.metrics.KafkaYammerMetrics";

    private static final byte BROKER_RUNNING_STATE = 3;
    private static final byte BROKER_RECOVERY_STATE = 2;
    private static final byte BROKER_UNKNOWN_STATE = 127;

    // KRaft raft-metrics MBean exposed by Apache Kafka. Read via the platform MBeanServer.
    static final String RAFT_METRICS_OBJECT_NAME = "kafka.server:type=raft-metrics";
    static final String RAFT_CURRENT_STATE_ATTRIBUTE = "current-state";
    // Raft state values that indicate the local node is attached to the quorum and able to serve.
    // Other values (unattached, candidate, voted, resigned, observer) mean the controller is either
    // mid-election (transient — handled by failureThreshold) or wedged.
    static final Set<String> RAFT_READY_STATES = Set.of("leader", "follower");

    static final SecureRandom RANDOM = new SecureRandom();
    private final Secret caCertSecret;
    private final Secret nodeCertSecret;
    private MetricName brokerStateName;
    private Gauge brokerState;
    private Gauge remainingLogsToRecover;
    private Gauge remainingSegmentsToRecover;
    // Returns the current value of the raft-metrics "current-state" attribute, or null if the MBean
    // is not (yet) registered. Pulled at request time so it always reflects current JVM state.
    private final Supplier<String> raftCurrentStateSupplier;

    /**
     * Constructor of the KafkaAgent
     *
     * @param client                Kubernetes client instance
     * @param caCertSecretName      CA certificate Secret name
     * @param nodeCertSecretName    Node certificate Secret name
     * @param namespace             Namespace where the Kafka cluster is running
     */
    /* test */ KafkaAgent(KubernetesClient client, String caCertSecretName, String nodeCertSecretName, String namespace) {
        this.caCertSecret = getKubernetesSecret(client, caCertSecretName, namespace);
        this.nodeCertSecret = getKubernetesSecret(client, nodeCertSecretName, namespace);
        this.raftCurrentStateSupplier = KafkaAgent::queryRaftCurrentStateFromJmx;
    }

    private Secret getKubernetesSecret(KubernetesClient client, String caCertSecretName, String namespace) {
        return client.secrets().inNamespace(namespace).withName(caCertSecretName).get();
    }

    /**
     * Constructor of the KafkaAgent
     *
     * @param caCertSecret                  CA certificate Secret
     * @param nodeCertSecret                Node certificate Secret
     * @param brokerState                   Current state of the broker
     * @param remainingLogsToRecover        Number of remaining logs to recover
     * @param remainingSegmentsToRecover    Number of remaining segments to recover
     */
    /* test */ KafkaAgent(Secret caCertSecret, Secret nodeCertSecret, Gauge brokerState, Gauge remainingLogsToRecover, Gauge remainingSegmentsToRecover) {
        this(caCertSecret, nodeCertSecret, brokerState, remainingLogsToRecover, remainingSegmentsToRecover, () -> null);
    }

    /**
     * Constructor of the KafkaAgent — variant exposing the raft-current-state supplier for tests.
     */
    /* test */ KafkaAgent(Secret caCertSecret, Secret nodeCertSecret, Gauge brokerState, Gauge remainingLogsToRecover, Gauge remainingSegmentsToRecover, Supplier<String> raftCurrentStateSupplier) {
        this.caCertSecret = caCertSecret;
        this.nodeCertSecret = nodeCertSecret;
        this.brokerState = brokerState;
        this.remainingLogsToRecover = remainingLogsToRecover;
        this.remainingSegmentsToRecover = remainingSegmentsToRecover;
        this.raftCurrentStateSupplier = raftCurrentStateSupplier;
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
                new SslConnectionFactory(getSSLContextFactory(caCertSecret, nodeCertSecret), "http/1.1"),
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

        ContextHandler controllerReadinessContext = new ContextHandler(CONTROLLER_READINESS_ENDPOINT_PATH);
        controllerReadinessContext.setHandler(getControllerReadinessHandler());

        server.setConnectors(new Connector[] {httpsConn, httpConn});
        server.setHandler(new ContextHandlerCollection(brokerStateContext, readinessContext, controllerReadinessContext));

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

    static SslContextFactory.Server getSSLContextFactory(Secret caCertSecret, Secret nodeCertSecret) throws GeneralSecurityException, IOException {
        SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();
        sslContextFactory.setTrustStore(KafkaAgentUtils.trustStore(caCertSecret));

        byte[] random = new byte[24];
        RANDOM.nextBytes(random);
        String password = Base64.getUrlEncoder().withoutPadding().encodeToString(random).substring(0, 32);

        sslContextFactory.setKeyStore(KafkaAgentUtils.keyStore(nodeCertSecret, password.toCharArray()));
        sslContextFactory.setKeyStorePassword(password);
        sslContextFactory.setNeedClientAuth(true);
        return  sslContextFactory;
    }

    /**
     * Creates a Handler instance to handle incoming HTTP requests for readiness check
     *
     * @return Handler
     */
    /* test */ Handler getReadinessHandler() {
        return new Handler.Abstract() {
            @Override
            public boolean handle(Request request, Response response, Callback callback) {
                response.getHeaders().put(HttpHeader.CONTENT_TYPE, "application/json; charset=UTF-8");

                if (brokerState != null) {
                    byte observedState = (byte) brokerState.value();
                    boolean stateIsRunning = BROKER_RUNNING_STATE <= observedState && BROKER_UNKNOWN_STATE != observedState;
                    if (stateIsRunning) {
                        LOGGER.trace("Broker is in running according to {}. The current state is {}", brokerStateName, observedState);
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
     * Creates a Handler instance to handle incoming HTTP requests for the controller readiness check.
     *
     * Returns 204 if the local KRaft node is attached to the quorum as {@code leader} or {@code follower},
     * 503 for any other raft state, and 404 if the raft-metrics MBean is not registered.
     *
     * @return Handler
     */
    /* test */ Handler getControllerReadinessHandler() {
        return new Handler.Abstract() {
            @Override
            public boolean handle(Request request, Response response, Callback callback) {
                response.getHeaders().put(HttpHeader.CONTENT_TYPE, "application/json; charset=UTF-8");

                String state = raftCurrentStateSupplier != null ? raftCurrentStateSupplier.get() : null;
                if (state == null) {
                    LOGGER.trace("Raft metrics MBean not found ({} attribute {}).", RAFT_METRICS_OBJECT_NAME, RAFT_CURRENT_STATE_ATTRIBUTE);
                    response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                    response.write(true, StandardCharsets.UTF_8.encode("{\"error\":\"raft-metrics MBean not found\"}"), callback);
                    return true;
                }

                if (RAFT_READY_STATES.contains(state)) {
                    LOGGER.trace("Controller is ready (raft current-state={}).", state);
                    response.setStatus(HttpServletResponse.SC_NO_CONTENT);
                    response.write(true, null, callback);
                } else {
                    LOGGER.trace("Controller is not ready (raft current-state={}).", state);
                    response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
                    String body = String.format("{\"error\":\"controller not ready, current raft state: %s\"}", state);
                    response.write(true, StandardCharsets.UTF_8.encode(body), callback);
                }
                return true;
            }
        };
    }

    /**
     * Default JMX-backed implementation of {@link #raftCurrentStateSupplier}. Returns the value of the
     * {@code current-state} attribute on {@code kafka.server:type=raft-metrics}, or {@code null} if
     * the MBean is not registered (e.g. broker-only nodes before broker startup, or older Kafka
     * versions without raft-metrics).
     */
    static String queryRaftCurrentStateFromJmx() {
        try {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = new ObjectName(RAFT_METRICS_OBJECT_NAME);
            Object value = server.getAttribute(name, RAFT_CURRENT_STATE_ATTRIBUTE);
            return value == null ? null : value.toString();
        } catch (InstanceNotFoundException e) {
            return null;
        } catch (MalformedObjectNameException e) {
            // RAFT_METRICS_OBJECT_NAME is a compile-time constant — this should never happen.
            LOGGER.warn("Malformed JMX object name {}", RAFT_METRICS_OBJECT_NAME, e);
            return null;
        } catch (Exception e) {
            LOGGER.warn("Could not query JMX attribute {} on {}: {}", RAFT_CURRENT_STATE_ATTRIBUTE, RAFT_METRICS_OBJECT_NAME, e.getMessage());
            return null;
        }
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

            final String caCertSecretName = agentConfigs.get("sslTrustStoreSecretName");
            final String nodeCertSecretName = agentConfigs.get("sslKeyStoreSecretName");
            final String namespace = agentConfigs.get("namespace");
            if (caCertSecretName.isEmpty() || nodeCertSecretName.isEmpty() || namespace.isEmpty()) {
                LOGGER.error("Missing the required Secret information: sslTrustStoreSecretName={} sslKeyStoreSecretName={} namespace={}", caCertSecretName, nodeCertSecretName, namespace);
                System.exit(1);
            } else {
                LOGGER.info("Starting KafkaAgent with sslTrustStoreSecretName={} sslKeyStoreSecretName={} namespace={}", caCertSecretName, nodeCertSecretName, namespace);
                KubernetesClient client = new KubernetesClientBuilder().build();
                new KafkaAgent(client, caCertSecretName, nodeCertSecretName, namespace).run();
            }
        }
    }
}
