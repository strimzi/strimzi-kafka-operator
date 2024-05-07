/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerConfiguration;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerConfigurationBroker;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationOAuth;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationTls;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.kafka.oauth.jsonpath.JsonPathFilterQuery;
import io.strimzi.kafka.oauth.jsonpath.JsonPathQuery;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.model.InvalidResourceException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.strimzi.operator.cluster.model.ListenersUtils.isListenerWithOAuth;

/**
 * Util methods for validating Kafka listeners
 */
public class ListenersValidator {
    protected static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ListenersValidator.class.getName());
    private final static Pattern LISTENER_NAME_PATTERN = Pattern.compile(GenericKafkaListener.LISTENER_NAME_REGEX);
    private final static List<Integer> FORBIDDEN_PORTS = List.of(9404, 9999);
    private final static int LOWEST_ALLOWED_PORT_NUMBER = 9092;

    /**
     * Validated the listener configuration. If the configuration is not valid, InvalidResourceException will be thrown.
     *
     * @param reconciliation    The reconciliation
     * @param brokerNodes       Broker nodes which are part of this Kafka cluster
     * @param listeners         Listeners which should be validated
     */
    public static void validate(Reconciliation reconciliation, Set<NodeRef> brokerNodes, List<GenericKafkaListener> listeners) throws InvalidResourceException {
        Set<String> errors = validateAndGetErrorMessages(brokerNodes, listeners);

        if (!errors.isEmpty())  {
            LOGGER.errorCr(reconciliation, "Listener configuration is not valid: {}", errors);
            throw new InvalidResourceException("Listener configuration is not valid: " + errors);
        }
    }

    /*test*/ static Set<String> validateAndGetErrorMessages(Set<NodeRef> brokerNodes, List<GenericKafkaListener> listeners)    {
        Set<String> errors = new HashSet<>(0);
        List<Integer> ports = getPorts(listeners);
        List<String> names = getNames(listeners);

        if (names.size() != listeners.size())   {
            errors.add("every listener needs to have a unique name");
        }

        List<String> invalidNames = names.stream().filter(name -> !LISTENER_NAME_PATTERN.matcher(name).matches()).toList();
        if (!invalidNames.isEmpty())    {
            errors.add("listener names " + invalidNames + " are invalid and do not match the pattern " + GenericKafkaListener.LISTENER_NAME_REGEX);
        }

        if (ports.size() != listeners.size())   {
            errors.add("every listener needs to have a unique port number");
        }

        for (GenericKafkaListener listener : listeners) {
            validatePortNumbers(errors, listener);
            validateRouteAndIngressTlsOnly(errors, listener);
            validateTlsFeaturesOnNonTlsListener(errors, listener);
            validateOauth(errors, listener);

            if (listener.getConfiguration() != null)    {
                validateServiceDnsDomain(errors, listener);
                validateIpFamilyPolicy(errors, listener);
                validateIpFamilies(errors, listener);
                validateControllerClass(errors, listener);
                validateExternalTrafficPolicy(errors, listener);
                validateLoadBalancerSourceRanges(errors, listener);
                validateFinalizers(errors, listener);
                validatePreferredAddressType(errors, listener);
                validateCreateBootstrapService(errors, listener);


                if (listener.getConfiguration().getBootstrap() != null) {
                    validateBootstrapHost(errors, listener);
                    validateBootstrapLoadBalancerIp(errors, listener);
                    validateBootstrapNodePort(errors, listener);
                    validateBootstrapLabelsAndAnnotations(errors, listener);
                    validateBootstrapExternalIPs(errors, listener);
                }

                if (listener.getConfiguration().getBrokers() != null) {
                    for (GenericKafkaListenerConfigurationBroker broker : listener.getConfiguration().getBrokers()) {
                        validateBrokerHost(errors, listener, broker);
                        validateBrokerLoadBalancerIp(errors, listener, broker);
                        validateBrokerNodePort(errors, listener, broker);
                        validateBrokerLabelsAndAnnotations(errors, listener, broker);
                        validateBrokerExternalIPs(errors, listener, broker);
                    }
                }

                if (listener.getConfiguration().getBrokerCertChainAndKey() != null) {
                    validateBrokerCertChainAndKey(errors, listener);
                }
            }

            if (KafkaListenerType.INGRESS.equals(listener.getType()))    {
                validateIngress(errors, brokerNodes, listener);
            }
        }

        return errors;
    }
    /**
     * Validates that the listener has a BrokerCertChainAndKey with non-empty values
     *
     * @param errors    List where any found errors will be added
     * @param listener  Listener which needs to be validated
     */
    private static void validateBrokerCertChainAndKey(Set<String> errors, GenericKafkaListener listener) {
        if (listener.getConfiguration().getBrokerCertChainAndKey().getSecretName() == null
                || listener.getConfiguration().getBrokerCertChainAndKey().getSecretName().isEmpty()) {
            errors.add("listener '" + listener.getName() + "' cannot have an empty secret name in the brokerCertChainAndKey");
        }

        if (listener.getConfiguration().getBrokerCertChainAndKey().getKey() == null
                || listener.getConfiguration().getBrokerCertChainAndKey().getKey().isEmpty()) {
            errors.add("listener '" + listener.getName() + "' cannot have an empty key in the brokerCertChainAndKey");
        }

        if (listener.getConfiguration().getBrokerCertChainAndKey().getCertificate() == null
                || listener.getConfiguration().getBrokerCertChainAndKey().getCertificate().isEmpty()) {
            errors.add("listener '" + listener.getName() + "' cannot have an empty certificate in the brokerCertChainAndKey");
        }
    }


    /**
     * Validates that the listener has an allowed port number
     *
     * @param errors    List where any found errors will be added
     * @param listener  Listener which needs to be validated
     */
    private static void validatePortNumbers(Set<String> errors, GenericKafkaListener listener) {
        int port = listener.getPort();

        if (FORBIDDEN_PORTS.contains(port)
                || port < LOWEST_ALLOWED_PORT_NUMBER)    {
            errors.add("port " + port + " is forbidden and cannot be used");
        }
    }

    /**
     * Validates that Ingress type listener has the right host configurations
     *
     * @param errors        List where any found errors will be added
     * @param brokerNodes   Broker nodes which are part of this Kafka cluster
     * @param listener      Listener which needs to be validated
     */
    private static void validateIngress(Set<String> errors, Set<NodeRef> brokerNodes, GenericKafkaListener listener) {
        if (listener.getConfiguration() != null)    {
            GenericKafkaListenerConfiguration conf = listener.getConfiguration();

            if (conf.getBootstrap() == null
                    || conf.getBootstrap().getHost() == null)   {
                errors.add("listener " + listener.getName() + " is missing a bootstrap host name which is required for Ingress based listeners");
            }

            if (conf.getBrokers() != null) {
                for (NodeRef node : brokerNodes)    {
                    GenericKafkaListenerConfigurationBroker broker = conf.getBrokers().stream().filter(b -> b.getBroker() == node.nodeId()).findFirst().orElse(null);

                    if (broker == null || broker.getHost() == null) {
                        errors.add("listener " + listener.getName() + " is missing a broker host name for broker with ID " + node.nodeId() + " which is required for Ingress based listeners");
                    }
                }
            } else {
                errors.add("listener " + listener.getName() + " is missing a broker configuration with host names which is required for Ingress based listeners");
            }


        } else {
            errors.add("listener " + listener.getName() + " is missing a configuration with host names which is required for Ingress based listeners");
        }
    }

    /**
     * Validates that useServiceDnsDomain is used only with internal type listener
     *
     * @param errors    List where any found errors will be added
     * @param listener  Listener which needs to be validated
     */
    private static void validateServiceDnsDomain(Set<String> errors, GenericKafkaListener listener) {
        if (!(KafkaListenerType.INTERNAL.equals(listener.getType()) || KafkaListenerType.CLUSTER_IP.equals(listener.getType()))
                && listener.getConfiguration().getUseServiceDnsDomain() != null)    {
            errors.add("listener " + listener.getName() + " cannot configure useServiceDnsDomain because it is not internal or cluster-ip listener");
        }
    }

    /**
     * Validates that createBootstrapService is used only with Load Balancer type listener
     *
     * @param errors    List where any found errors will be added
     * @param listener  Listener which needs to be validated
     */
    private static void validateCreateBootstrapService(Set<String> errors, GenericKafkaListener listener) {
        if (KafkaListenerType.LOADBALANCER != listener.getType()
                && listener.getConfiguration() != null
                && !listener.getConfiguration().getCreateBootstrapService())    {
            errors.add("listener " + listener.getName() + " cannot configure createBootstrapService because it is not load balancer listener");
        }
    }

    /**
     * Validates that ipFamilyPolicy is used only with external listeners
     *
     * @param errors    List where any found errors will be added
     * @param listener  Listener which needs to be validated
     */
    private static void validateIpFamilyPolicy(Set<String> errors, GenericKafkaListener listener) {
        if (KafkaListenerType.INTERNAL == listener.getType()
                && listener.getConfiguration().getIpFamilyPolicy() != null)    {
            errors.add("listener " + listener.getName() + " cannot configure ipFamilyPolicy because it is internal listener");
        }
    }

    /**
     * Validates that ipFamilies is used only with external listeners
     *
     * @param errors    List where any found errors will be added
     * @param listener  Listener which needs to be validated
     */
    private static void validateIpFamilies(Set<String> errors, GenericKafkaListener listener) {
        if (KafkaListenerType.INTERNAL == listener.getType()
                && listener.getConfiguration().getIpFamilies() != null)    {
            errors.add("listener " + listener.getName() + " cannot configure ipFamilies because it is internal listener");
        }
    }

    /**
     * Validates that controllerClass is used only with Ingress or LoadBalancer type listener
     *
     * @param errors    List where any found errors will be added
     * @param listener  Listener which needs to be validated
     */
    private static void validateControllerClass(Set<String> errors, GenericKafkaListener listener) {
        if (!(KafkaListenerType.INGRESS.equals(listener.getType()) || KafkaListenerType.LOADBALANCER.equals(listener.getType()))
                && listener.getConfiguration().getControllerClass() != null)    {
            errors.add("listener " + listener.getName() + " cannot configure class because it is not an Ingress or LoadBalancer based listener");
        }
    }

    /**
     * Validates that preferredAddressType is used only with NodePort type listener
     *
     * @param errors    List where any found errors will be added
     * @param listener  Listener which needs to be validated
     */
    private static void validatePreferredAddressType(Set<String> errors, GenericKafkaListener listener) {
        if (!KafkaListenerType.NODEPORT.equals(listener.getType())
                && listener.getConfiguration().getPreferredNodePortAddressType() != null)    {
            errors.add("listener " + listener.getName() + " cannot configure preferredAddressType because it is not NodePort based listener");
        }
    }

    /**
     * Validates that externalTrafficPolicy is used only with Loadbalancer type listener
     *
     * @param errors    List where any found errors will be added
     * @param listener  Listener which needs to be validated
     */
    private static void validateExternalTrafficPolicy(Set<String> errors, GenericKafkaListener listener) {
        if (!(KafkaListenerType.LOADBALANCER.equals(listener.getType()) || KafkaListenerType.NODEPORT.equals(listener.getType()))
                && listener.getConfiguration().getExternalTrafficPolicy() != null)    {
            errors.add("listener " + listener.getName() + " cannot configure externalTrafficPolicy because it is not LoadBalancer or NodePort based listener");
        }
    }

    /**
     * Validates that loadBalancerSourceRanges is used only with Loadbalancer type listener
     *
     * @param errors    List where any found errors will be added
     * @param listener  Listener which needs to be validated
     */
    private static void validateLoadBalancerSourceRanges(Set<String> errors, GenericKafkaListener listener) {
        if (!KafkaListenerType.LOADBALANCER.equals(listener.getType())
                && listener.getConfiguration().getLoadBalancerSourceRanges() != null
                && !listener.getConfiguration().getLoadBalancerSourceRanges().isEmpty())    {
            errors.add("listener " + listener.getName() + " cannot configure loadBalancerSourceRanges because it is not LoadBalancer based listener");
        }
    }

    /**
     * Validates that finalizers is used only with Loadbalancer type listener
     *
     * @param errors    List where any found errors will be added
     * @param listener  Listener which needs to be validated
     */
    private static void validateFinalizers(Set<String> errors, GenericKafkaListener listener) {
        if (!KafkaListenerType.LOADBALANCER.equals(listener.getType())
                && listener.getConfiguration().getFinalizers() != null
                && !listener.getConfiguration().getFinalizers().isEmpty())    {
            errors.add("listener " + listener.getName() + " cannot configure finalizers because it is not LoadBalancer based listener");
        }
    }

    /**
     * Validates that bootstrap.host is used only with Route or Ingress type listener
     *
     * @param errors    List where any found errors will be added
     * @param listener  Listener which needs to be validated
     */
    private static void validateBootstrapHost(Set<String> errors, GenericKafkaListener listener) {
        if ((!KafkaListenerType.ROUTE.equals(listener.getType()) && !KafkaListenerType.INGRESS.equals(listener.getType()))
                && listener.getConfiguration().getBootstrap().getHost() != null)    {
            errors.add("listener " + listener.getName() + " cannot configure bootstrap.host because it is not Route or Ingress based listener");
        }
    }

    /**
     * Validates that bootstrap.loadBalancerIP is used only with NodePort type listener
     *
     * @param errors    List where any found errors will be added
     * @param listener  Listener which needs to be validated
     */
    private static void validateBootstrapLoadBalancerIp(Set<String> errors, GenericKafkaListener listener) {
        if (!KafkaListenerType.LOADBALANCER.equals(listener.getType())
                && listener.getConfiguration().getBootstrap().getLoadBalancerIP() != null)    {
            errors.add("listener " + listener.getName() + " cannot configure bootstrap.loadBalancerIP because it is not LoadBalancer based listener");
        }
    }

    /**
     * Validates that bootstrap.nodePort is used only with NodePort type listener
     *
     * @param errors    List where any found errors will be added
     * @param listener  Listener which needs to be validated
     */
    private static void validateBootstrapNodePort(Set<String> errors, GenericKafkaListener listener) {
        if (!KafkaListenerType.NODEPORT.equals(listener.getType())
                && !KafkaListenerType.LOADBALANCER.equals(listener.getType())
                && listener.getConfiguration().getBootstrap().getNodePort() != null)    {
            errors.add("listener " + listener.getName() + " cannot configure bootstrap.nodePort because it is not NodePort based listener");
        }
    }
    
    /**
     * Validates that bootstrap.nodePort is used only with NodePort type listener
     *
     * @param errors    List where any found errors will be added
     * @param listener  Listener which needs to be validated
     */
    private static void validateBootstrapExternalIPs(Set<String> errors, GenericKafkaListener listener) {
        if (!KafkaListenerType.NODEPORT.equals(listener.getType())
                && listener.getConfiguration().getBootstrap().getExternalIPs() != null)    {
            errors.add("listener " + listener.getName() + " cannot configure bootstrap.externalIPs because it is not NodePort based listener");
        }
    }

    /**
     * Validates that bootstrap.annotations and bootstrap.labels are used only with LoadBalancer, NodePort, Route, or
     * Ingress type listener
     *
     * @param errors    List where any found errors will be added
     * @param listener  Listener which needs to be validated
     */
    private static void validateBootstrapLabelsAndAnnotations(Set<String> errors, GenericKafkaListener listener) {
        if (!KafkaListenerType.LOADBALANCER.equals(listener.getType())
                && !KafkaListenerType.NODEPORT.equals(listener.getType())
                && !KafkaListenerType.ROUTE.equals(listener.getType())
                && !KafkaListenerType.INGRESS.equals(listener.getType())
                && !KafkaListenerType.CLUSTER_IP.equals(listener.getType())) {
            if (listener.getConfiguration().getBootstrap().getLabels() != null
                    && !listener.getConfiguration().getBootstrap().getLabels().isEmpty()) {
                errors.add("listener " + listener.getName() + " cannot configure bootstrap.labels because it is not LoadBalancer, NodePort, Route, Ingress or ClusterIP based listener");
            }

            if (listener.getConfiguration().getBootstrap().getAnnotations() != null
                    && !listener.getConfiguration().getBootstrap().getAnnotations().isEmpty()) {
                errors.add("listener " + listener.getName() + " cannot configure bootstrap.annotations because it is not LoadBalancer, NodePort, Route, Ingress or ClusterIP based listener");
            }
        }
    }

    /**
     * Validates that brokers[].host is used only with Route or Ingress type listener
     *
     * @param errors    List where any found errors will be added
     * @param listener  Listener which needs to be validated
     * @param broker    Broker configuration which needs to be validated
     */
    private static void validateBrokerHost(Set<String> errors, GenericKafkaListener listener, GenericKafkaListenerConfigurationBroker broker) {
        if ((!KafkaListenerType.ROUTE.equals(listener.getType()) && !KafkaListenerType.INGRESS.equals(listener.getType()))
                && broker.getHost() != null)    {
            errors.add("listener " + listener.getName() + " cannot configure brokers[].host because it is not Route or Ingress based listener");
        }
    }

    /**
     * Validates that brokers[].loadBalancerIP is used only with NodePort type listener
     *
     * @param errors    List where any found errors will be added
     * @param listener  Listener which needs to be validated
     * @param broker    Broker configuration which needs to be validated
     */
    private static void validateBrokerLoadBalancerIp(Set<String> errors, GenericKafkaListener listener, GenericKafkaListenerConfigurationBroker broker) {
        if (!KafkaListenerType.LOADBALANCER.equals(listener.getType())
                && broker.getLoadBalancerIP() != null)    {
            errors.add("listener " + listener.getName() + " cannot configure brokers[].loadBalancerIP because it is not LoadBalancer based listener");
        }
    }

    /**
     * Validates that brokers[].nodePort is used only with NodePort type listener
     *
     * @param errors    List where any found errors will be added
     * @param listener  Listener which needs to be validated
     * @param broker    Broker configuration which needs to be validated
     */
    private static void validateBrokerNodePort(Set<String> errors, GenericKafkaListener listener, GenericKafkaListenerConfigurationBroker broker) {
        if (!KafkaListenerType.NODEPORT.equals(listener.getType())
                && !KafkaListenerType.LOADBALANCER.equals(listener.getType())
                && broker.getNodePort() != null)    {
            errors.add("listener " + listener.getName() + " cannot configure brokers[].nodePort because it is not NodePort based listener");
        }
    }
    
    /**
     * Validates that brokers[].nodePort is used only with NodePort type listener
     *
     * @param errors    List where any found errors will be added
     * @param listener  Listener which needs to be validated
     * @param broker    Broker configuration which needs to be validated
     */
    private static void validateBrokerExternalIPs(Set<String> errors, GenericKafkaListener listener, GenericKafkaListenerConfigurationBroker broker) {
        if (!KafkaListenerType.NODEPORT.equals(listener.getType())
                && broker.getExternalIPs() != null)    {
            errors.add("listener " + listener.getName() + " cannot configure brokers[].externalIPs because it is not NodePort based listener");
        }
    }

    /**
     * Validates that brokers[].annotations and brokers[].labels are used only with LoadBalancer, NodePort, Route or
     * Ingress type listener
     *
     * @param errors    List where any found errors will be added
     * @param listener  Listener which needs to be validated
     * @param broker    Broker configuration which needs to be validated
     */
    private static void validateBrokerLabelsAndAnnotations(Set<String> errors, GenericKafkaListener listener, GenericKafkaListenerConfigurationBroker broker) {
        if (!KafkaListenerType.LOADBALANCER.equals(listener.getType())
                && !KafkaListenerType.NODEPORT.equals(listener.getType())
                && !KafkaListenerType.ROUTE.equals(listener.getType())
                && !KafkaListenerType.INGRESS.equals(listener.getType())
                && !KafkaListenerType.CLUSTER_IP.equals(listener.getType()))  {
            if (broker.getLabels() != null
                    && !broker.getLabels().isEmpty()) {
                errors.add("listener " + listener.getName() + " cannot configure brokers[].labels because it is not LoadBalancer, NodePort, Route, Ingress or ClusterIP based listener");
            }

            if (broker.getAnnotations() != null
                    && !broker.getAnnotations().isEmpty()) {
                errors.add("listener " + listener.getName() + " cannot configure brokers[].annotations because it is not LoadBalancer, NodePort, Route, Ingress or ClusterIP based listener");
            }
        }
    }

    /**
     * Validates that Route and Ingress listeners have always enabled TLS
     *
     * @param errors    List where any found errors will be added
     * @param listener  Listener which needs to be validated
     */
    private static void validateRouteAndIngressTlsOnly(Set<String> errors, GenericKafkaListener listener) {
        if ((KafkaListenerType.ROUTE.equals(listener.getType()) || KafkaListenerType.INGRESS.equals(listener.getType()))
                && !listener.isTls())    {
            errors.add("listener " + listener.getName() + " is Route or Ingress type listener and requires enabled TLS encryption");
        }
    }

    /**
     * Validates whether any features depending on TLS are not configured on non-TLS listener
     *
     * @param errors    List where any found errors will be added
     * @param listener  Listener which needs to be validated
     */
    private static void validateTlsFeaturesOnNonTlsListener(Set<String> errors, GenericKafkaListener listener)    {
        if (!listener.isTls())  {
            if (listener.getAuth() != null
                    && KafkaListenerAuthenticationTls.TYPE_TLS.equals(listener.getAuth().getType())) {
                errors.add("listener " + listener.getName() + " cannot use " + KafkaListenerAuthenticationTls.TYPE_TLS + " type authentication with disabled TLS encryption");
            }

            if (listener.getConfiguration() != null
                    && listener.getConfiguration().getBrokerCertChainAndKey() != null)  {
                errors.add("listener " + listener.getName() + " cannot configure custom TLS certificate with disabled TLS encryption");
            }
        }
    }

    /**
     * Validates provided OAuth configuration. Throws InvalidResourceException when OAuth configuration contains forbidden combinations.
     *
     * @param errors    List where any found errors will be added
     * @param listener  The listener where OAuth authentication should be validated
     */
    @SuppressWarnings({"checkstyle:BooleanExpressionComplexity", "checkstyle:NPathComplexity", "checkstyle:CyclomaticComplexity"})
    private static void validateOauth(Set<String> errors, GenericKafkaListener listener) {
        if (isListenerWithOAuth(listener)) {
            KafkaListenerAuthenticationOAuth oAuth = (KafkaListenerAuthenticationOAuth) listener.getAuth();
            String listenerName = listener.getName();

            if (!oAuth.isEnablePlain() && !oAuth.isEnableOauthBearer()) {
                errors.add("listener " + listenerName + ": At least one of 'enablePlain', 'enableOauthBearer' has to be set to 'true'");
            }
            boolean hasJwksRefreshSecondsValidInput = oAuth.getJwksRefreshSeconds() != null && oAuth.getJwksRefreshSeconds() > 0;
            boolean hasJwksExpirySecondsValidInput = oAuth.getJwksExpirySeconds() != null && oAuth.getJwksExpirySeconds() > 0;
            boolean hasJwksMinRefreshPauseSecondsValidInput = oAuth.getJwksMinRefreshPauseSeconds() != null && oAuth.getJwksMinRefreshPauseSeconds() >= 0;

            if (oAuth.getIntrospectionEndpointUri() == null && oAuth.getJwksEndpointUri() == null) {
                errors.add("listener " + listenerName + ": Introspection endpoint URI or JWKS endpoint URI has to be specified");
            }

            if (oAuth.getValidIssuerUri() == null && oAuth.isCheckIssuer()) {
                errors.add("listener " + listenerName + ": Valid Issuer URI has to be specified or 'checkIssuer' set to 'false'");
            }

            if (oAuth.getConnectTimeoutSeconds() != null && oAuth.getConnectTimeoutSeconds() <= 0) {
                errors.add("listener " + listenerName + ": 'connectTimeoutSeconds' needs to be a positive integer (set to: " + oAuth.getConnectTimeoutSeconds() + ")");
            }

            if (oAuth.getReadTimeoutSeconds() != null && oAuth.getReadTimeoutSeconds() <= 0) {
                errors.add("listener " + listenerName + ": 'readTimeoutSeconds' needs to be a positive integer (set to: " + oAuth.getReadTimeoutSeconds() + ")");
            }

            if (oAuth.isCheckAudience() && oAuth.getClientId() == null) {
                errors.add("listener " + listenerName + ": 'clientId' has to be configured when 'checkAudience' is 'true'");
            }

            String customCheckQuery = oAuth.getCustomClaimCheck();
            if (customCheckQuery != null) {
                try {
                    JsonPathFilterQuery.parse(customCheckQuery);
                } catch (Exception e) {
                    errors.add("listener " + listenerName + ": 'customClaimCheck' value not a valid JsonPath filter query - " + e.getMessage());
                }
            }

            if (oAuth.getIntrospectionEndpointUri() != null && (oAuth.getClientId() == null || oAuth.getClientSecret() == null)) {
                errors.add("listener " + listenerName + ": Introspection Endpoint URI needs to be configured together with 'clientId' and 'clientSecret'");
            }

            if (oAuth.getUserInfoEndpointUri() != null && oAuth.getIntrospectionEndpointUri() == null) {
                errors.add("listener " + listenerName + ": User Info Endpoint URI can only be used if Introspection Endpoint URI is also configured");
            }

            if (oAuth.getJwksEndpointUri() == null && (hasJwksRefreshSecondsValidInput || hasJwksExpirySecondsValidInput || hasJwksMinRefreshPauseSecondsValidInput)) {
                errors.add("listener " + listenerName + ": 'jwksRefreshSeconds', 'jwksExpirySeconds' and 'jwksMinRefreshPauseSeconds' can only be used together with 'jwksEndpointUri'");
            }

            if (oAuth.getJwksRefreshSeconds() != null && !hasJwksRefreshSecondsValidInput) {
                errors.add("listener " + listenerName + ": 'jwksRefreshSeconds' needs to be a positive integer (set to: " + oAuth.getJwksRefreshSeconds() + ")");
            }

            if (oAuth.getJwksExpirySeconds() != null && !hasJwksExpirySecondsValidInput) {
                errors.add("listener " + listenerName + ": 'jwksExpirySeconds' needs to be a positive integer (set to: " + oAuth.getJwksExpirySeconds() + ")");
            }

            if (oAuth.getJwksMinRefreshPauseSeconds() != null && !hasJwksMinRefreshPauseSecondsValidInput) {
                errors.add("listener " + listenerName + ": 'jwksMinRefreshPauseSeconds' needs to be a positive integer or zero (set to: " + oAuth.getJwksMinRefreshPauseSeconds() + ")");
            }

            if ((hasJwksExpirySecondsValidInput && hasJwksRefreshSecondsValidInput && oAuth.getJwksExpirySeconds() < oAuth.getJwksRefreshSeconds() + 60) ||
                    (!hasJwksExpirySecondsValidInput && hasJwksRefreshSecondsValidInput && KafkaListenerAuthenticationOAuth.DEFAULT_JWKS_EXPIRY_SECONDS < oAuth.getJwksRefreshSeconds() + 60) ||
                    (hasJwksExpirySecondsValidInput && !hasJwksRefreshSecondsValidInput && oAuth.getJwksExpirySeconds() < KafkaListenerAuthenticationOAuth.DEFAULT_JWKS_REFRESH_SECONDS + 60)) {
                errors.add("listener " + listenerName + ": The refresh interval has to be at least 60 seconds shorter then the expiry interval specified in `jwksExpirySeconds`");
            }

            if (!oAuth.isAccessTokenIsJwt()) {
                if (oAuth.getJwksEndpointUri() != null) {
                    errors.add("listener " + listenerName + ": 'accessTokenIsJwt' can not be 'false' when 'jwksEndpointUri' is set");
                }
                if (!oAuth.isCheckAccessTokenType()) {
                    errors.add("listener " + listenerName + ": 'checkAccessTokenType' can not be set to 'false' when 'accessTokenIsJwt' is 'false'");
                }
            }

            if (!oAuth.isCheckAccessTokenType() && oAuth.getIntrospectionEndpointUri() != null) {
                errors.add("listener " + listenerName + ": 'checkAccessTokenType' can not be set to 'false' when 'introspectionEndpointUri' is set");
            }

            if (oAuth.getValidTokenType() != null && oAuth.getIntrospectionEndpointUri() == null) {
                errors.add("listener " + listenerName + ": 'validTokenType' can only be used when 'introspectionEndpointUri' is set");
            }

            String groupsQuery = oAuth.getGroupsClaim();
            if (groupsQuery != null) {
                try {
                    JsonPathQuery.parse(groupsQuery);
                } catch (Exception e) {
                    errors.add("listener " + listenerName + ": 'groupsClaim' value not a valid JsonPath query - " + e.getMessage());
                }
            }
        }
    }

    /**
     * Extracts all distinct port numbers from the listeners
     *
     * @param listeners List of all listeners
     * @return          List of used port numbers
     */
    private static List<Integer> getPorts(List<GenericKafkaListener> listeners)    {
        return listeners.stream().map(GenericKafkaListener::getPort).distinct().collect(Collectors.toList());
    }

    /**
     * Extracts all distinct listener names from the listeners
     *
     * @param listeners List of all listeners
     * @return          List of used names
     */
    private static List<String> getNames(List<GenericKafkaListener> listeners)    {
        return listeners.stream().map(GenericKafkaListener::getName).distinct().collect(Collectors.toList());
    }
}
