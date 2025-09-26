/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.strimzi.api.kafka.model.common.template.ExternalTrafficPolicy;
import io.strimzi.api.kafka.model.common.template.IpFamily;
import io.strimzi.api.kafka.model.common.template.IpFamilyPolicy;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerConfiguration;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerConfigurationBroker;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationCustom;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationOAuth;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.kafka.listener.NodeAddressType;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Util methods for working with Kafka listeners
 */
public class ListenersUtils {
    /*test*/ static final String BACKWARDS_COMPATIBLE_PLAIN_PORT_NAME = "tcp-clients";
    /*test*/ static final String BACKWARDS_COMPATIBLE_TLS_PORT_NAME = "tcp-clientstls";
    /*test*/ static final String BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME = "tcp-external";

    /**
     * Finds out if any of the listeners has OAuth authentication enabled
     *
     * @param listeners List of all listeners
     *
     * @return          True if any listener in the list is using OAuth authentication. False otherwise.
     */
    public static boolean hasListenerWithOAuth(List<GenericKafkaListener> listeners)    {
        return listeners.stream()
                .anyMatch(ListenersUtils::isListenerWithOAuth);
    }

    /**
     * Checks whether the listener is using OAuth authentication
     *
     * @param listener  Listener to check
     *
     * @return  True if the listener is configured to use OAuth authentication. False otherwise.
     */
    @SuppressWarnings("deprecation") // OAuth authentication is deprecated
    public static boolean isListenerWithOAuth(GenericKafkaListener listener) {
        if (listener.getAuth() == null || listener.getAuth().getType() == null)
            return false;

        return KafkaListenerAuthenticationOAuth.TYPE_OAUTH.equals(listener.getAuth().getType());
    }

    /**
     * Checks whether the listener is using Custom authentication
     *
     * @param listener  Listener to check
     *
     * @return  True if the listener uses Custom authentication. False otherwise.
     */
    public static boolean isListenerWithCustomAuth(GenericKafkaListener listener) {
        if (listener.getAuth() == null || listener.getAuth().getType() == null)
            return false;

        return KafkaListenerAuthenticationCustom.TYPE_CUSTOM.equals(listener.getAuth().getType());
    }

    /**
     * Returns list of all listeners with given type
     *
     * @param listeners List of all listeners
     * @param type      Type of the listeners which should be returned
     * @return          List of internal listeners
     */
    private static List<GenericKafkaListener> listenersByType(List<GenericKafkaListener> listeners, KafkaListenerType type)    {
        return listeners.stream()
                .filter(listener -> type == listener.getType())
                .collect(Collectors.toList());
    }

    /**
     * Returns list of all internal listeners
     *
     * @param listeners List of all listeners
     * @return          List of internal listeners
     */
    public static List<GenericKafkaListener> internalListeners(List<GenericKafkaListener> listeners)    {
        return listenersByType(listeners, KafkaListenerType.INTERNAL);
    }

    /**
     * Returns list of all listeners which use their own services (i.e. all apart from type=internal)
     *
     * @param listeners List of all listeners
     * @return          List of listeners with their own services
     */
    public static List<GenericKafkaListener> listenersWithOwnServices(List<GenericKafkaListener> listeners)    {
        return listeners.stream()
                .filter(listener -> KafkaListenerType.INTERNAL != listener.getType())
                .collect(Collectors.toList());
    }

    /**
     * Returns list of all Route type listeners
     *
     * @param listeners List of all listeners
     * @return          List of route listeners
     */
    public static List<GenericKafkaListener> routeListeners(List<GenericKafkaListener> listeners)    {
        return listenersByType(listeners, KafkaListenerType.ROUTE);
    }

    /**
     * Returns list of all LoadBalancer type listeners
     *
     * @param listeners List of all listeners
     * @return          List of load balancer listeners
     */
    public static List<GenericKafkaListener> loadBalancerListeners(List<GenericKafkaListener> listeners)    {
        return listenersByType(listeners, KafkaListenerType.LOADBALANCER);
    }

    /**
     * Returns list of all NodePort type listeners
     *
     * @param listeners List of all listeners
     * @return          List of node port listeners
     */
    public static List<GenericKafkaListener> nodePortListeners(List<GenericKafkaListener> listeners)    {
        return listenersByType(listeners, KafkaListenerType.NODEPORT);
    }

    /**
     * Returns list of all Ingress type listeners
     *
     * @param listeners List of all listeners
     * @return          List of ingress listeners
     */
    public static List<GenericKafkaListener> ingressListeners(List<GenericKafkaListener> listeners)    {
        return listenersByType(listeners, KafkaListenerType.INGRESS);
    }

    /**
     * Returns list of all Ingress type listeners
     *
     * @param listeners List of all listeners
     * @return          List of clusterIP listeners
     */
    public static List<GenericKafkaListener> clusterIPListeners(List<GenericKafkaListener> listeners)    {
        return listenersByType(listeners, KafkaListenerType.CLUSTER_IP);
    }

    /**
     * Returns true if the list has a listener of given type and false otherwise.
     *
     * @param listeners List of all listeners
     * @param type      Type of the listeners which should be returned
     * @return          Whether a listener of given type was found or not
     */
    private static boolean hasListenerOfType(List<GenericKafkaListener> listeners, KafkaListenerType type)    {
        return listeners.stream()
                .anyMatch(listener -> type == listener.getType());
    }

    /**
     * Checks whether we have at least one NodePort listener
     *
     * @param listeners List of all listeners
     * @return          True if at least one NodePort listener exists. False otherwise.
     */
    public static boolean hasNodePortListener(List<GenericKafkaListener> listeners)    {
        return hasListenerOfType(listeners, KafkaListenerType.NODEPORT);
    }

    /**
     * Checks whether we have at least one Ingress listener
     *
     * @param listeners List of all listeners
     * @return          True if at least one Ingress listener exists. False otherwise.
     */
    public static boolean hasIngressListener(List<GenericKafkaListener> listeners)    {
        return hasListenerOfType(listeners, KafkaListenerType.INGRESS);
    }

    /**
     * Checks whether we have at least one ClusterIP listener
     *
     * @param listeners List of all listeners
     * @return          True if at least one ClusterIP listener exists. False otherwise.
     */
    public static boolean hasClusterIPListener(List<GenericKafkaListener> listeners)    {
        return hasListenerOfType(listeners, KafkaListenerType.CLUSTER_IP);
    }

    /**
     * Returns list of all additional DNS addresses for certificates
     *
     * @param listeners List of all listeners
     * @return          List of alternative DNS names for bootstrap
     */
    public static List<String> alternativeNames(List<GenericKafkaListener> listeners)    {
        return listeners.stream()
                .filter(listener -> listener.getConfiguration() != null
                        && listener.getConfiguration().getBootstrap() != null
                        && listener.getConfiguration().getBootstrap().getAlternativeNames() != null)
                .flatMap(listener -> listener.getConfiguration().getBootstrap().getAlternativeNames().stream())
                .distinct()
                .collect(Collectors.toList());
    }

    /**
     * Generates a listener identifier which is used to name the related volumes, volume mounts, etc.
     *
     * @param listener  Listener for which the name should be generated
     * @return          Identifier string
     */
    public static String identifier(GenericKafkaListener listener) {
        return listener.getName() + "-" + listener.getPort();
    }

    /**
     * Generates a listener identifier which can be used in environment variables
     *
     * @param listener  Listener for which the name should be generated
     * @return          Identifier string
     */
    public static String envVarIdentifier(GenericKafkaListener listener) {
        return listener.getName().toUpperCase(Locale.ENGLISH) + "_" + listener.getPort();
    }

    /**
     * Generates port names which are backwards compatible with the previous Strimzi versions
     *
     * @param listener  Listener for which the name should be generated
     * @return          Name of the port
     */
    public static String backwardsCompatiblePortName(GenericKafkaListener listener) {
        if (listener.getPort() == 9092 && "plain".equals(listener.getName()) && KafkaListenerType.INTERNAL == listener.getType())   {
            return BACKWARDS_COMPATIBLE_PLAIN_PORT_NAME;
        } else if (listener.getPort() == 9093 && "tls".equals(listener.getName()) && KafkaListenerType.INTERNAL == listener.getType())   {
            return BACKWARDS_COMPATIBLE_TLS_PORT_NAME;
        } else if (listener.getPort() == 9094 && "external".equals(listener.getName()))   {
            return BACKWARDS_COMPATIBLE_EXTERNAL_PORT_NAME;
        } else {
            return "tcp-" + listener.getName();
        }
    }

    /**
     * Generates bootstrap service names which are backwards compatible with the previous Strimzi versions
     *
     * @param clusterName Name of the cluster to which this service belongs
     * @param listener  Listener for which the name should be generated
     * @return          Name of the bootstrap service
     */
    public static String backwardsCompatibleBootstrapServiceName(String clusterName, GenericKafkaListener listener) {
        if (KafkaListenerType.INTERNAL == listener.getType()) {
            return clusterName + "-kafka-bootstrap";
        } else {
            return clusterName + "-kafka-" + listener.getName() + "-bootstrap";
        }
    }

    /**
     * Generates bootstrap route or ingress names which are backwards compatible with the previous Strimzi versions
     *
     * @param clusterName Name of the cluster to which this service belongs
     * @param listener  Listener for which the name should be generated
     * @return          Name of the bootstrap service
     */
    public static String backwardsCompatibleBootstrapRouteOrIngressName(String clusterName, GenericKafkaListener listener) {
        if (KafkaListenerType.INTERNAL == listener.getType()) {
            throw new UnsupportedOperationException("Bootstrap routes or ingresses are not used for internal listeners");
        } else if (listener.getPort() == 9094 && "external".equals(listener.getName()))   {
            return clusterName + "-kafka-bootstrap";
        } else {
            return clusterName + "-kafka-" + listener.getName() + "-bootstrap";
        }
    }

    /**
     * Generates broker service names which are backwards compatible with the previous Strimzi versions.
     *
     * @throws  UnsupportedOperationException Throws UnsupportedOperationException if called for internal service
     *                                          which does not have per-pod services
     *
     * @param baseName  The base name which should be used to generate the Service name - for example my-cluster-kafka
     * @param pod       Number of the pod for which this service will be used
     * @param listener  Listener for which the name should be generated
     *
     * @return          Name of the bootstrap service
     */
    public static String backwardsCompatiblePerBrokerServiceName(String baseName, int pod, GenericKafkaListener listener) {
        if (KafkaListenerType.INTERNAL == listener.getType()) {
            throw new UnsupportedOperationException("Per-broker services are not used for internal listener");
        } else if (listener.getPort() == 9094 && "external".equals(listener.getName()))   {
            return baseName + "-" + pod;
        } else {
            return baseName + "-" + listener.getName() + "-" + pod;
        }
    }

    /**
     * Finds bootstrap service node port
     *
     * @param listener  Listener for which the port should be found
     * @return          Node port or null if not specified
     */
    public static Integer bootstrapNodePort(GenericKafkaListener listener)    {
        if (listener.getConfiguration() != null
                && listener.getConfiguration().getBootstrap() != null) {
            return listener.getConfiguration().getBootstrap().getNodePort();
        } else {
            return null;
        }
    }

    /**
     * Finds broker service node port
     *
     * @param listener  Listener for which the port should be found
     * @param pod       Pod ID for which we should get the configuration option
     * @return          Node port or null if not specified
     */
    public static Integer brokerNodePort(GenericKafkaListener listener, int pod)    {
        if (listener.getConfiguration() != null
                && listener.getConfiguration().getBrokers() != null) {
            return listener.getConfiguration().getBrokers().stream()
                    .filter(broker -> broker != null && broker.getBroker() != null && broker.getBroker() == pod && broker.getNodePort() != null)
                    .map(GenericKafkaListenerConfigurationBroker::getNodePort)
                    .findAny()
                    .orElse(null);
        } else {
            return null;
        }
    }

    /**
     * Finds bootstrap service load balancer IP
     *
     * @param listener  Listener for which the load balancer IP should be found
     * @return          Load Balancer or null if not specified
     */
    public static String bootstrapLoadBalancerIP(GenericKafkaListener listener)    {
        if (listener.getConfiguration() != null
                && listener.getConfiguration().getBootstrap() != null) {
            return listener.getConfiguration().getBootstrap().getLoadBalancerIP();
        } else {
            return null;
        }
    }

    /**
     * Finds broker service load balancer IP
     *
     * @param listener  Listener for which the load balancer IP should be found
     * @param pod       Pod ID for which we should get the configuration option
     * @return          Load Balancer or null if not specified
     */
    public static String brokerLoadBalancerIP(GenericKafkaListener listener, int pod)    {
        if (listener.getConfiguration() != null
                && listener.getConfiguration().getBrokers() != null) {
            return listener.getConfiguration().getBrokers().stream()
                    .filter(broker -> broker != null && broker.getBroker() != null && broker.getBroker() == pod && broker.getLoadBalancerIP() != null)
                    .map(GenericKafkaListenerConfigurationBroker::getLoadBalancerIP)
                    .findAny()
                    .orElse(null);
        } else {
            return null;
        }
    }

    /**
     * Finds bootstrap service DNS annotations
     *
     * @param listener  Listener for which the load balancer IP should be found
     * @return          Map with DNS annotations or empty map if not specified
     */
    public static Map<String, String> bootstrapAnnotations(GenericKafkaListener listener)    {
        if (listener.getConfiguration() != null
                && listener.getConfiguration().getBootstrap() != null
                && listener.getConfiguration().getBootstrap().getAnnotations() != null) {
            return listener.getConfiguration().getBootstrap().getAnnotations();
        } else {
            return Collections.emptyMap();
        }
    }

    /**
     * Finds broker service DNS annotations
     *
     * @param listener  Listener for which the load balancer IP should be found
     * @param pod       Pod ID for which we should get the configuration option
     * @return          Map with DNS annotations or empty map if not specified
     */
    public static Map<String, String> brokerAnnotations(GenericKafkaListener listener, int pod)    {
        if (listener.getConfiguration() != null
                && listener.getConfiguration().getBrokers() != null) {
            return listener.getConfiguration().getBrokers().stream()
                    .filter(broker -> broker != null && broker.getBroker() != null && broker.getBroker() == pod && broker.getAnnotations() != null)
                    .map(GenericKafkaListenerConfigurationBroker::getAnnotations)
                    .findAny()
                    .orElse(Collections.emptyMap());
        } else {
            return Collections.emptyMap();
        }
    }

    /**
     * Finds bootstrap service labels
     *
     * @param listener  Listener for which the load balancer IP should be found
     * @return          Map with labels or empty map if not specified
     */
    public static Map<String, String> bootstrapLabels(GenericKafkaListener listener)    {
        if (listener.getConfiguration() != null
                && listener.getConfiguration().getBootstrap() != null
                && listener.getConfiguration().getBootstrap().getLabels() != null) {
            return listener.getConfiguration().getBootstrap().getLabels();
        } else {
            return Collections.emptyMap();
        }
    }

    /**
     * Finds broker service labels
     *
     * @param listener  Listener for which the load balancer IP should be found
     * @param pod       Pod ID for which we should get the configuration option
     * @return          Map with labels or empty map if not specified
     */
    public static Map<String, String> brokerLabels(GenericKafkaListener listener, int pod)    {
        if (listener.getConfiguration() != null
                && listener.getConfiguration().getBrokers() != null) {
            return listener.getConfiguration().getBrokers().stream()
                    .filter(broker -> broker != null && broker.getBroker() != null && broker.getBroker() == pod && broker.getLabels() != null)
                    .map(GenericKafkaListenerConfigurationBroker::getLabels)
                    .findAny()
                    .orElse(Collections.emptyMap());
        } else {
            return Collections.emptyMap();
        }
    }

    /**
     * Finds bootstrap host
     *
     * @param listener  Listener for which the host should be found
     * @return          Host name or null if not specified
     */
    public static String bootstrapHost(GenericKafkaListener listener)    {
        if (listener.getConfiguration() != null
                && listener.getConfiguration().getBootstrap() != null) {
            return listener.getConfiguration().getBootstrap().getHost();
        } else {
            return null;
        }
    }

    /**
     * Replaces the template fields in the template string with the corresponding values from the node reference.
     *
     * @param template  Template with the placeholders
     * @param node      Node reference that should be used to provide the final values
     *
     * @return  The rendered template
     */
    /* test */ static String renderHostTemplate(String template, NodeRef node) {
        return template
                .replace("{nodeId}", Integer.toString(node.nodeId()))
                .replace("{nodePodName}", node.podName());
    }

    /**
     * Finds per-broker host configuration based on node ID.
     *
     * @param listenerConfiguration     Configuration of the listener for which the host should be found
     * @param nodeId                    Node ID for which we should get the configuration option
     *
     * @return  Host configured for given node ID or null if not specified
     */
    private static String brokerHost(GenericKafkaListenerConfiguration listenerConfiguration, int nodeId)    {
        if (listenerConfiguration.getBrokers() != null) {
            return listenerConfiguration.getBrokers().stream()
                    .filter(broker -> broker != null && broker.getBroker() != null && broker.getBroker() == nodeId && broker.getHost() != null)
                    .map(GenericKafkaListenerConfigurationBroker::getHost)
                    .findAny()
                    .orElse(null);
        } else {
            return null;
        }
    }

    /**
     * Finds broker host configuration either in the per-broker configuration or renders it from the template. If no
     * per-broker value and no template are set, it returns null.
     *
     * @param listener  Listener for which the host should be found
     * @param node      Node reference describing the node for which we want to find the host
     *
     * @return          Host or null if not specified
     */
    public static String brokerHost(GenericKafkaListener listener, NodeRef node)    {
        if (listener.getConfiguration() != null)    {
            String host = brokerHost(listener.getConfiguration(), node.nodeId());

            if (host == null && listener.getConfiguration().getHostTemplate() != null)   {
                // There is no host defined specifically for given broker, so we try to use the template
                host = renderHostTemplate(listener.getConfiguration().getHostTemplate(), node);
            }

            return host;
        } else {
            return null;
        }
    }

    /**
     * Finds per-broker advertised host configuration based on node ID.
     *
     * @param listenerConfiguration     Configuration of the listener for which the advertised host should be found
     * @param nodeId                    Node ID for which we should get the configuration option
     *
     * @return  Advertised Host or null if not specified
     */
    private static String brokerAdvertisedHost(GenericKafkaListenerConfiguration listenerConfiguration, int nodeId)    {
        if (listenerConfiguration.getBrokers() != null) {
            return listenerConfiguration.getBrokers().stream()
                    .filter(broker -> broker != null && broker.getBroker() != null && broker.getBroker() == nodeId && broker.getAdvertisedHost() != null)
                    .map(GenericKafkaListenerConfigurationBroker::getAdvertisedHost)
                    .findAny()
                    .orElse(null);
        } else {
            return null;
        }
    }

    /**
     * Finds broker advertised host configuration either in the per-broker configuration or renders it from the template. If no
     * per-broker value and no template are set, it returns null.
     *
     * @param listener  Listener for which the advertised host should be found
     * @param node      The node for which the advertised hostname should be obtained
     *
     * @return  Advertised Host or null if not specified
     */
    public static String brokerAdvertisedHost(GenericKafkaListener listener, NodeRef node)    {
        if (listener.getConfiguration() != null)    {
            String advertisedHost = brokerAdvertisedHost(listener.getConfiguration(), node.nodeId());

            if (advertisedHost == null && listener.getConfiguration().getAdvertisedHostTemplate() != null)   {
                // There is no advertised host defined specifically for given broker, so we try to use the template
                advertisedHost = renderHostTemplate(listener.getConfiguration().getAdvertisedHostTemplate(), node);
            }

            return advertisedHost;
        } else {
            return null;
        }
    }

    /**
     * Finds broker advertised port configuration
     *
     * @param listener  Listener for which the advertised port should be found
     * @param pod       Pod ID for which we should get the configuration option
     * @return          Advertised port or null if not specified
     */
    public static Integer brokerAdvertisedPort(GenericKafkaListener listener, int pod)    {
        if (listener.getConfiguration() != null
                && listener.getConfiguration().getBrokers() != null) {
            return listener.getConfiguration().getBrokers().stream()
                    .filter(broker -> broker != null && broker.getBroker() != null && broker.getBroker() == pod && broker.getAdvertisedPort() != null)
                    .map(GenericKafkaListenerConfigurationBroker::getAdvertisedPort)
                    .findAny()
                    .orElse(null);
        } else {
            return null;
        }
    }

    /**
     * Finds load balancer source ranges
     *
     * @param listener  Listener for which the load balancer source ranges should be found
     * @return          Load Balancer source ranges or null if not specified
     */
    public static List<String> loadBalancerSourceRanges(GenericKafkaListener listener)    {
        if (listener.getConfiguration() != null) {
            return listener.getConfiguration().getLoadBalancerSourceRanges();
        } else {
            return null;
        }
    }

    /**
     * Finds load balancer finalizers
     *
     * @param listener  Listener for which the load balancer finalizers should be found
     * @return          Load Balancer finalizers or null if not specified
     */
    public static List<String> finalizers(GenericKafkaListener listener)    {
        if (listener.getConfiguration() != null) {
            return listener.getConfiguration().getFinalizers();
        } else {
            return null;
        }
    }

    /**
     * Finds external traffic policy
     *
     * @param listener  Listener for which the external traffic policy should be found
     * @return          External traffic policy or null if not specified
     */
    public static ExternalTrafficPolicy externalTrafficPolicy(GenericKafkaListener listener)    {
        if (listener.getConfiguration() != null) {
            return listener.getConfiguration().getExternalTrafficPolicy();
        } else {
            return null;
        }
    }
    
    /**
     * Finds publish not ready addresses configuration
     *
     * @param listener  Listener for which the publish not ready addresses value should be found
     * @return          publish not ready addresses value or null if not specified
     */
    public static Boolean publishNotReadyAddresses(GenericKafkaListener listener)    {
        return listener.getConfiguration() == null ? null : listener.getConfiguration().getPublishNotReadyAddresses();
    }

    /**
     * Finds IP family policy
     *
     * @param listener  Listener for which the IP family policy should be found
     * @return          IP family policy or null if not specified
     */
    public static IpFamilyPolicy ipFamilyPolicy(GenericKafkaListener listener)    {
        if (listener.getConfiguration() != null) {
            return listener.getConfiguration().getIpFamilyPolicy();
        } else {
            return null;
        }
    }

    /**
     * Finds IP families
     *
     * @param listener  Listener for which the IP families should be found
     * @return          IP families or null if not specified
     */
    public static List<IpFamily> ipFamilies(GenericKafkaListener listener)    {
        if (listener.getConfiguration() != null) {
            return listener.getConfiguration().getIpFamilies();
        } else {
            return null;
        }
    }

    /**
     * Finds preferred node address type
     *
     * @param listener  Listener for which the preferred node address type should be found
     * @return          Preferred node address type or null if not specified
     */
    public static NodeAddressType preferredNodeAddressType(GenericKafkaListener listener)    {
        if (listener.getConfiguration() != null) {
            return listener.getConfiguration().getPreferredNodePortAddressType();
        } else {
            return null;
        }
    }

    /**
     * Check whether we should skip the creation of the bootstrap service.
     *
     * @param listener Listener for which the createBootstrapService should be created or not.
     * @return         Whether we should create the Load Balancer Service for the Bootstrap Service.
     */
    public static Boolean skipCreateBootstrapService(GenericKafkaListener listener) {
        return KafkaListenerType.LOADBALANCER == listener.getType()
                && listener.getConfiguration() != null
                && !listener.getConfiguration().getCreateBootstrapService();
    }

    /**
     * Finds controller class which mean ingress class or loadbalancer class
     *
     * @param listener  Listener for which the controller class should be found
     * @return          Controller class or null if not specified
     */
    public static String controllerClass(GenericKafkaListener listener)    {
        if (listener.getConfiguration() != null) {
            return listener.getConfiguration().getControllerClass();
        } else {
            return null;
        }
    }

    /**
     * Utility function to help to determine the type of service based on external listener configuration
     *
     * @param listener Kafka listener for which we want to get the service type
     *
     * @return Service type
     */
    public static String serviceType(GenericKafkaListener listener) {
        if (listener.getType() == KafkaListenerType.NODEPORT) {
            return "NodePort";
        } else if (listener.getType() == KafkaListenerType.LOADBALANCER) {
            return "LoadBalancer";
        } else {
            return "ClusterIP";
        }
    }

    /**
     * Returns the advertised port for given broker. If user specified some override in the listener configuration, it
     * will return this override. If no override is specified, it will return the port obtained from Kubernetes
     * passes as parameter to this method.
     *
     * @param listener  Listener where the configuration should be found
     * @param nodeId    Kafka node ID
     * @param port      The advertised port
     *
     * @return  The advertised port as String
     */
    public static String advertisedPortFromOverrideOrParameter(GenericKafkaListener listener, int nodeId, Integer port) {
        Integer advertisedPort = ListenersUtils.brokerAdvertisedPort(listener, nodeId);

        return String.valueOf(advertisedPort != null ? advertisedPort : port);
    }

    /**
     * Returns bootstrap service external IPs
     * 
     * @param listener  Listener for which the external IPs should be found
     * 
     * @return  External IPs or null if not specified
     */
    public static List<String> bootstrapExternalIPs(GenericKafkaListener listener) {
        return (listener.getConfiguration() != null && listener.getConfiguration().getBootstrap() != null)
            ? listener.getConfiguration().getBootstrap().getExternalIPs()
                : null;
    }

    /**
     * Returns broker service external IPs
     * 
     * @param listener  Listener for which the external IPs should be found
     * @param nodeId       Node ID for which we should get the configuration option
     * 
     * @return          External IPs or null if not specified
     */
    public static List<String> brokerExternalIPs(GenericKafkaListener listener, int nodeId) {
        return (listener.getConfiguration() != null && listener.getConfiguration().getBrokers() != null)
                ? listener.getConfiguration().getBrokers().stream()
                    .filter(broker -> broker != null && broker.getBroker() != null && broker.getBroker() == nodeId && broker.getExternalIPs() != null)
                    .map(GenericKafkaListenerConfigurationBroker::getExternalIPs)
                    .findAny()
                    .orElse(null) : null;
    }

    /**
     * Returns whether to allocate NodePorts for LoadBalancer Service type
     *
     * @param listener Listener for which to allocate NodePorts
     * @return         Whether to allocate NodePorts for Service
     */
    @SuppressFBWarnings(value = {"NP_BOOLEAN_RETURN_NULL"}, justification = "Null is being checked by the caller, so " +
                                                                            "the Service defaults can takeover properly")
    public static Boolean allocateLoadBalancerNodePorts(GenericKafkaListener listener) {
        return listener.getConfiguration() != null ?
                listener.getConfiguration().getAllocateLoadBalancerNodePorts() :
                null;
    }
}
