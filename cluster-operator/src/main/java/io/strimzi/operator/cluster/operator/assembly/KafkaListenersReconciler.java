/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.openshift.api.model.Route;
import io.strimzi.api.kafka.model.CertAndKeySecretSource;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.listener.NodeAddressType;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListener;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerConfigurationBroker;
import io.strimzi.api.kafka.model.status.ListenerAddressBuilder;
import io.strimzi.api.kafka.model.status.ListenerStatus;
import io.strimzi.api.kafka.model.status.ListenerStatusBuilder;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.model.CertUtils;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.DnsNameGenerator;
import io.strimzi.operator.cluster.model.InvalidResourceException;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.ListenersUtils;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.operator.resource.IngressOperator;
import io.strimzi.operator.common.operator.resource.RouteOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.common.operator.resource.ServiceOperator;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

/**
 * Class used for reconciliation of Kafka listeners. This class contains both the steps of the Kafka
 * reconciliation pipeline related to the listeners and is also used to store the state between them.
 */
public class KafkaListenersReconciler {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(KafkaListenersReconciler.class.getName());

    private final Reconciliation reconciliation;
    private final long operationTimeoutMs;
    private final KafkaCluster kafka;
    private final ClusterCa clusterCa;
    private final PlatformFeaturesAvailability pfa;

    private final SecretOperator secretOperator;
    private final ServiceOperator serviceOperator;
    private final RouteOperator routeOperator;
    private final IngressOperator ingressOperator;

    /* test */ final ReconciliationResult result;

    /**
     * Constructs the Kafka Listeners reconciler
     *
     * @param reconciliation            Reconciliation marker
     * @param kafka                     The Kafka mode
     * @param clusterCa                 The Cluster CA instance
     * @param pfa                       PlatformFeaturesAvailability describing the environment we run in
     * @param operationTimeoutMs        Timeout for Kubernetes operations
     * @param secretOperator            The Secret operator for working with Kubernetes Secrets
     * @param serviceOperator           The Service operator for working with Kubernetes Services
     * @param routeOperator             The Route operator for working with Kubernetes Route
     * @param ingressOperator           The Ingress operator for working with Kubernetes Ingress
     */
    public KafkaListenersReconciler(
            Reconciliation reconciliation,
            KafkaCluster kafka,
            ClusterCa clusterCa,
            PlatformFeaturesAvailability pfa,

            long operationTimeoutMs,

            SecretOperator secretOperator,
            ServiceOperator serviceOperator,
            RouteOperator routeOperator,
            IngressOperator ingressOperator
    ) {
        this.reconciliation = reconciliation;
        this.kafka = kafka;
        this.clusterCa = clusterCa;
        this.pfa = pfa;

        this.operationTimeoutMs = operationTimeoutMs;

        this.secretOperator = secretOperator;
        this.serviceOperator = serviceOperator;
        this.routeOperator = routeOperator;
        this.ingressOperator = ingressOperator;

        // Initialize the result object
        this.result = new ReconciliationResult();
        // Fill in the alternative names from the listener configuration
        this.result.bootstrapDnsNames.addAll(ListenersUtils.alternativeNames(kafka.getListeners()));
    }

    /**
     * The main reconciliation method which triggers the whole reconciliation pipeline. This is the method which is
     * expected to be called from the outside to trigger the reconciliation.
     *
     * @return  Future which completes when the reconciliation completes. It contains the result of the reconciliation
     *          which contains the collected addresses, prepared listener statuses etc.
     */
    public Future<ReconciliationResult> reconcile()    {
        return services()
                .compose(i -> routes())
                .compose(i -> ingresses())
                .compose(i -> internalServicesReady())
                .compose(i -> loadBalancerServicesReady())
                .compose(i -> nodePortServicesReady())
                .compose(i -> routesReady())
                .compose(i -> ingressesReady())
                .compose(i -> clusterIPServicesReady())
                .compose(i -> customListenerCertificates())
                // This method should be called only after customListenerCertificates
                .compose(customListenerCertificates -> addCertificatesToListenerStatuses(customListenerCertificates))
                .compose(i -> Future.succeededFuture(result));
    }

    /**
     * Makes sure all desired services are updated and the rest is deleted. This method updates all services in one go
     *           => the regular headless, node-port or load balancer ones.
     *
     * @return  Future which completes when all services are created or deleted.
     */
    protected Future<Void> services() {
        List<Service> services = new ArrayList<>();
        services.add(kafka.generateService());
        services.add(kafka.generateHeadlessService());
        services.addAll(kafka.generateExternalBootstrapServices());

        int replicas = kafka.getReplicas();
        for (int i = 0; i < replicas; i++) {
            services.addAll(kafka.generateExternalServices(i));
        }

        return serviceOperator.batchReconcile(reconciliation, reconciliation.namespace(), services, kafka.getSelectorLabels());
    }

    /**
     * Makes sure all desired routes are updated and the rest is deleted.
     *
     * @return Future which completes when all routes are created or deleted.
     */
    protected Future<Void> routes() {
        List<Route> routes = new ArrayList<>(kafka.generateExternalBootstrapRoutes());

        if (routes.size() > 0) {
            if (pfa.hasRoutes()) {
                int replicas = kafka.getReplicas();
                for (int i = 0; i < replicas; i++) {
                    routes.addAll(kafka.generateExternalRoutes(i));
                }

                return routeOperator.batchReconcile(reconciliation, reconciliation.namespace(), routes, kafka.getSelectorLabels());
            } else {
                LOGGER.warnCr(reconciliation, "The OpenShift route API is not available in this Kubernetes cluster. Exposing Kafka cluster {} using routes is not possible.", reconciliation.name());
                return Future.failedFuture("The OpenShift route API is not available in this Kubernetes cluster. Exposing Kafka cluster " + reconciliation.name() + " using routes is not possible.");
            }
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Makes sure all desired ingresses are updated and the rest is deleted.
     *
     * @return  Future which completes when all ingresses are created or deleted.
     */
    protected Future<Void> ingresses() {
        List<Ingress> ingresses = new ArrayList<>(kafka.generateExternalBootstrapIngresses());

        int replicas = kafka.getReplicas();
        for (int i = 0; i < replicas; i++) {
            ingresses.addAll(kafka.generateExternalIngresses(i));
        }

        return ingressOperator.batchReconcile(reconciliation, reconciliation.namespace(), ingresses, kafka.getSelectorLabels());
    }

    /**
     * Utility method which helps to register the advertised hostnames for a specific listener of a specific broker.
     * The broker hostname passed to this method is based on the infrastructure (Service, Load Balancer, etc.).
     * This method in addition checks for any overrides and uses them if configured.
     *
     * @param brokerId          ID of the broker to which this hostname belongs
     * @param listener          The Listener for which is this hostname used
     * @param brokerHostname    The hostname which might be used for the broker when no overrides are configured
     */
    private void registerAdvertisedHostname(int brokerId, GenericKafkaListener listener, String brokerHostname)   {
        result.advertisedHostnames
                .computeIfAbsent(brokerId, id -> new HashMap<>())
                .put(ListenersUtils.envVarIdentifier(listener), kafka.getAdvertisedHostname(listener, brokerId, brokerHostname));
    }

    /**
     * Utility method which helps to register the advertised port for a specific listener of a specific broker.
     * The broker port passed to this method is based on the infrastructure (Service, Load Balancer, etc.).
     * This method in addition checks for any overrides and uses them if configured.
     *
     * @param brokerId      ID of the broker to which this port belongs
     * @param listener      The Listener for which is this port used
     * @param brokerPort    The port which might be used for the broker when no overrides are configured
     */
    private void registerAdvertisedPort(int brokerId, GenericKafkaListener listener, int brokerPort)   {
        result.advertisedPorts
                .computeIfAbsent(brokerId, id -> new HashMap<>())
                .put(ListenersUtils.envVarIdentifier(listener), kafka.getAdvertisedPort(listener, brokerId, brokerPort));
    }

    /**
     * Generates the name of the environment variable which will contain the advertised address for given listener. The
     * environment variable will be different for Node Port listeners which need to consume the address from the init
     * container corresponding to their preferred node.
     *
     * @param listener              The listener
     * @return                      The environment variable which will have the address
     */
    private static String nodePortAddressEnvVar(GenericKafkaListener listener)  {
        String preferredNodeAddressType;
        NodeAddressType preferredType = ListenersUtils.preferredNodeAddressType(listener);

        if (preferredType != null && preferredType.toValue() != null)  {
            preferredNodeAddressType = preferredType.toValue().toUpperCase(Locale.ENGLISH);
        } else {
            preferredNodeAddressType = "DEFAULT";
        }

        return String.format("${STRIMZI_NODEPORT_%s_ADDRESS}", preferredNodeAddressType);
    }

    /**
     * Generates the hostname of an internal service with or without the DNS suffix
     *
     * @param namespace             Namespace of the service
     * @param serviceName           Name of the service
     * @param useServiceDnsDomain   Flag indicating whether the address should contain the DNS suffix or not
     *
     * @return  The DNS name of the service
     */
    private static String getInternalServiceHostname(String namespace, String serviceName, boolean useServiceDnsDomain)    {
        if (useServiceDnsDomain)    {
            return DnsNameGenerator.serviceDnsNameWithClusterDomain(namespace, serviceName);
        } else {
            return DnsNameGenerator.serviceDnsNameWithoutClusterDomain(namespace, serviceName);
        }
    }

    /**
     * Checks the readiness of the internal services. The internal services are ready out of the box and there is no
     * need to wait for them. But this method at least collects their addresses for the reconciliation result and
     * prepares the listener statuses.
     *
     * @return  Future which completes when the internal services are ready and their addresses are collected
     */
    protected Future<Void> internalServicesReady()   {
        for (GenericKafkaListener listener : ListenersUtils.internalListeners(kafka.getListeners())) {
            boolean useServiceDnsDomain = (listener.getConfiguration() != null && listener.getConfiguration().getUseServiceDnsDomain() != null)
                    ? listener.getConfiguration().getUseServiceDnsDomain() : false;

            // Set status based on bootstrap service
            String bootstrapAddress = getInternalServiceHostname(reconciliation.namespace(), ListenersUtils.backwardsCompatibleBootstrapServiceName(reconciliation.name(), listener), useServiceDnsDomain);

            ListenerStatus ls = new ListenerStatusBuilder()
                    .withName(listener.getName())
                    .withAddresses(new ListenerAddressBuilder()
                            .withHost(bootstrapAddress)
                            .withPort(listener.getPort())
                            .build())
                    .build();
            result.listenerStatuses.add(ls);

            // Set advertised hostnames and ports
            for (int brokerId = 0; brokerId < kafka.getReplicas(); brokerId++) {
                String brokerAddress;

                if (useServiceDnsDomain) {
                    brokerAddress = DnsNameGenerator.podDnsNameWithClusterDomain(reconciliation.namespace(), KafkaResources.brokersServiceName(reconciliation.name()), KafkaResources.kafkaStatefulSetName(reconciliation.name()) + "-" + brokerId);
                } else {
                    brokerAddress = DnsNameGenerator.podDnsNameWithoutClusterDomain(reconciliation.namespace(), KafkaResources.brokersServiceName(reconciliation.name()), KafkaResources.kafkaStatefulSetName(reconciliation.name()) + "-" + brokerId);
                }

                String userConfiguredAdvertisedHostname = ListenersUtils.brokerAdvertisedHost(listener, brokerId);
                if (userConfiguredAdvertisedHostname != null && listener.isTls()) {
                    // If user configured a custom advertised hostname, add it to the SAN names used in the certificate
                    result.brokerDnsNames.computeIfAbsent(brokerId, k -> new HashSet<>(1)).add(userConfiguredAdvertisedHostname);
                }

                registerAdvertisedHostname(brokerId, listener, brokerAddress);
                registerAdvertisedPort(brokerId, listener, listener.getPort());
            }
        }

        return Future.succeededFuture();
    }

    /**
     * Collect all addresses of services related to clusterIP addresses for Statuses, certificates and advertised
     * addresses. This method for all ClusterIP type listeners:
     *      1) Collects the relevant addresses of bootstrap service and stores them for use in certificates
     *      2) Collects the clusterIP addresses for certificates and advertised hostnames
     *
     * @return  Future which completes clusterIP service addresses are collected
     */
    protected Future<Void> clusterIPServicesReady()   {
        for (GenericKafkaListener listener : ListenersUtils.clusterIPListeners(kafka.getListeners())) {
            boolean useServiceDnsDomain = (listener.getConfiguration() != null && listener.getConfiguration().getUseServiceDnsDomain() != null)
                    ? listener.getConfiguration().getUseServiceDnsDomain() : false;

            String bootstrapAddress = getInternalServiceHostname(reconciliation.namespace(), ListenersUtils.backwardsCompatibleBootstrapServiceName(reconciliation.name(), listener), useServiceDnsDomain);

            if (listener.isTls()) {
                result.bootstrapDnsNames.addAll(ModelUtils.generateAllServiceDnsNames(reconciliation.namespace(), ListenersUtils.backwardsCompatibleBootstrapServiceName(reconciliation.name(), listener)));
            }

            ListenerStatus ls = new ListenerStatusBuilder()
                    .withName(listener.getName())
                    .withAddresses(new ListenerAddressBuilder()
                            .withHost(bootstrapAddress)
                            .withPort(listener.getPort())
                            .build())
                    .build();
            result.listenerStatuses.add(ls);

            // Set advertised hostnames and ports
            for (int brokerId = 0; brokerId < kafka.getReplicas(); brokerId++) {
                String brokerServiceName = ListenersUtils.backwardsCompatibleBrokerServiceName(reconciliation.name(), brokerId, listener);
                String brokerAddress = getInternalServiceHostname(reconciliation.namespace(), brokerServiceName, useServiceDnsDomain);
                if (listener.isTls()) {
                    result.brokerDnsNames.computeIfAbsent(brokerId, k -> new HashSet<>(2)).add(brokerAddress);

                    String userConfiguredAdvertisedHostname = ListenersUtils.brokerAdvertisedHost(listener, brokerId);
                    if (userConfiguredAdvertisedHostname != null) {
                        result.brokerDnsNames.get(brokerId).add(userConfiguredAdvertisedHostname);
                    }
                }

                registerAdvertisedHostname(brokerId, listener, brokerAddress);
                registerAdvertisedPort(brokerId, listener, listener.getPort());
            }
        }

        return Future.succeededFuture();
    }

    /**
     * Makes sure all services related to load balancers are ready and collects their addresses for Statuses,
     * certificates and advertised addresses. This method for all Load Balancer type listeners:
     *      1) Checks if the bootstrap service has been provisioned (has a loadbalancer address)
     *      2) Collects the relevant addresses and stores them for use in certificates and in CR status
     *      3) Checks if the broker services have been provisioned (have a loadbalancer address)
     *      4) Collects the loadbalancer addresses for certificates and advertised hostnames
     *
     * @return  Future which completes when all Load Balancer services are ready and their addresses are collected
     */
    protected Future<Void> loadBalancerServicesReady() {
        List<GenericKafkaListener> loadBalancerListeners = ListenersUtils.loadBalancerListeners(kafka.getListeners());
        @SuppressWarnings({ "rawtypes" }) // Has to use Raw type because of the CompositeFuture
        List<Future> listenerFutures = new ArrayList<>(loadBalancerListeners.size());

        for (GenericKafkaListener listener : loadBalancerListeners) {
            String bootstrapServiceName = ListenersUtils.backwardsCompatibleBootstrapServiceName(reconciliation.name(), listener);

            List<String> bootstrapListenerAddressList = new ArrayList<>(kafka.getReplicas());

            Future<Void> perListenerFut = Future.succeededFuture().compose(i -> {
                if (ListenersUtils.skipCreateBootstrapService(listener)) {
                    return Future.succeededFuture();
                } else {
                    return serviceOperator.hasIngressAddress(reconciliation, reconciliation.namespace(), bootstrapServiceName, 1_000, operationTimeoutMs)
                            .compose(res -> serviceOperator.getAsync(reconciliation.namespace(), bootstrapServiceName))
                            .compose(svc -> {
                                String bootstrapAddress;

                                if (svc.getStatus().getLoadBalancer().getIngress().get(0).getHostname() != null) {
                                    bootstrapAddress = svc.getStatus().getLoadBalancer().getIngress().get(0).getHostname();
                                } else {
                                    bootstrapAddress = svc.getStatus().getLoadBalancer().getIngress().get(0).getIp();
                                }

                                LOGGER.debugCr(reconciliation, "Found address {} for Service {}", bootstrapAddress, bootstrapServiceName);

                                result.bootstrapDnsNames.add(bootstrapAddress);
                                bootstrapListenerAddressList.add(bootstrapAddress);
                                return Future.succeededFuture();
                            });
                }
            }).compose(res -> {
                @SuppressWarnings({ "rawtypes" }) // Has to use Raw type because of the CompositeFuture
                List<Future> perPodFutures = new ArrayList<>(kafka.getReplicas());

                for (int pod = 0; pod < kafka.getReplicas(); pod++)  {
                    perPodFutures.add(
                            serviceOperator.hasIngressAddress(reconciliation, reconciliation.namespace(), ListenersUtils.backwardsCompatibleBrokerServiceName(reconciliation.name(), pod, listener), 1_000, operationTimeoutMs)
                    );
                }

                return CompositeFuture.join(perPodFutures);
            }).compose(res -> {
                @SuppressWarnings({ "rawtypes" }) // Has to use Raw type because of the CompositeFuture
                List<Future> perPodFutures = new ArrayList<>(kafka.getReplicas());

                for (int brokerId = 0; brokerId < kafka.getReplicas(); brokerId++)  {
                    final int finalBrokerId = brokerId;
                    Future<Void> perBrokerFut = serviceOperator.getAsync(reconciliation.namespace(), ListenersUtils.backwardsCompatibleBrokerServiceName(reconciliation.name(), brokerId, listener))
                            .compose(svc -> {
                                String brokerAddress;

                                if (svc.getStatus().getLoadBalancer().getIngress().get(0).getHostname() != null)    {
                                    brokerAddress = svc.getStatus().getLoadBalancer().getIngress().get(0).getHostname();
                                } else {
                                    brokerAddress = svc.getStatus().getLoadBalancer().getIngress().get(0).getIp();
                                }
                                LOGGER.debugCr(reconciliation, "Found address {} for Service {}", brokerAddress, svc.getMetadata().getName());

                                if (ListenersUtils.skipCreateBootstrapService(listener)) {
                                    bootstrapListenerAddressList.add(brokerAddress);
                                }
                                result.brokerDnsNames.computeIfAbsent(finalBrokerId, k -> new HashSet<>(2)).add(brokerAddress);

                                String advertisedHostname = ListenersUtils.brokerAdvertisedHost(listener, finalBrokerId);
                                if (advertisedHostname != null) {
                                    result.brokerDnsNames.get(finalBrokerId).add(ListenersUtils.brokerAdvertisedHost(listener, finalBrokerId));
                                }

                                registerAdvertisedHostname(finalBrokerId, listener, brokerAddress);
                                registerAdvertisedPort(finalBrokerId, listener, listener.getPort());

                                return Future.succeededFuture();
                            });

                    perPodFutures.add(perBrokerFut);
                }

                return CompositeFuture.join(perPodFutures);
            }).compose(res -> {
                ListenerStatus ls = new ListenerStatusBuilder()
                        .withName(listener.getName())
                        .withAddresses(bootstrapListenerAddressList.stream()
                                .map(listenerAddress -> new ListenerAddressBuilder().withHost(listenerAddress)
                                        .withPort(listener.getPort())
                                        .build())
                                .collect(Collectors.toList()))
                        .build();
                result.listenerStatuses.add(ls);

                return Future.succeededFuture();
            });

            listenerFutures.add(perListenerFut);
        }

        return CompositeFuture
                .join(listenerFutures)
                .map((Void) null);
    }

    /**
     * Makes sure all services related to node ports are ready and collects their addresses for Statuses,
     * certificates and advertised addresses. This method for all NodePort type listeners:
     *      1) Checks if the bootstrap service has been provisioned (has a node port)
     *      2) Collects the node port for use in CR status
     *      3) Checks it the broker services have been provisioned (have a node port)
     *      4) Collects the node ports for advertised hostnames
     *
     * @return  Future which completes when all Node Port services are ready and their ports are collected
     */
    protected Future<Void> nodePortServicesReady() {
        List<GenericKafkaListener> loadBalancerListeners = ListenersUtils.nodePortListeners(kafka.getListeners());
        @SuppressWarnings({ "rawtypes" }) // Has to use Raw type because of the CompositeFuture
        List<Future> listenerFutures = new ArrayList<>(loadBalancerListeners.size());

        for (GenericKafkaListener listener : loadBalancerListeners) {
            String bootstrapServiceName = ListenersUtils.backwardsCompatibleBootstrapServiceName(reconciliation.name(), listener);

            @SuppressWarnings({ "rawtypes" }) // Has to use Raw type because of the CompositeFuture
            Future perListenerFut = serviceOperator.hasNodePort(reconciliation, reconciliation.namespace(), bootstrapServiceName, 1_000, operationTimeoutMs)
                    .compose(res -> serviceOperator.getAsync(reconciliation.namespace(), bootstrapServiceName))
                    .compose(svc -> {
                        Integer externalBootstrapNodePort = svc.getSpec().getPorts().get(0).getNodePort();
                        LOGGER.debugCr(reconciliation, "Found node port {} for Service {}", externalBootstrapNodePort, bootstrapServiceName);
                        result.bootstrapNodePorts.put(ListenersUtils.identifier(listener), externalBootstrapNodePort);

                        return Future.succeededFuture();
                    })
                    .compose(res -> {
                        @SuppressWarnings({ "rawtypes" }) // Has to use Raw type because of the CompositeFuture
                        List<Future> perPodFutures = new ArrayList<>(kafka.getReplicas());

                        for (int pod = 0; pod < kafka.getReplicas(); pod++)  {
                            perPodFutures.add(
                                    serviceOperator.hasNodePort(reconciliation, reconciliation.namespace(), ListenersUtils.backwardsCompatibleBrokerServiceName(reconciliation.name(), pod, listener), 1_000, operationTimeoutMs)
                            );
                        }

                        return CompositeFuture.join(perPodFutures);
                    })
                    .compose(res -> {
                        @SuppressWarnings({ "rawtypes" }) // Has to use Raw type because of the CompositeFuture
                        List<Future> perPodFutures = new ArrayList<>(kafka.getReplicas());

                        for (int brokerId = 0; brokerId < kafka.getReplicas(); brokerId++)  {
                            final int finalBrokerId = brokerId;
                            Future<Void> perBrokerFut = serviceOperator.getAsync(reconciliation.namespace(), ListenersUtils.backwardsCompatibleBrokerServiceName(reconciliation.name(), brokerId, listener))
                                    .compose(svc -> {
                                        Integer externalBrokerNodePort = svc.getSpec().getPorts().get(0).getNodePort();
                                        LOGGER.debugCr(reconciliation, "Found node port {} for Service {}", externalBrokerNodePort, svc.getMetadata().getName());

                                        registerAdvertisedPort(finalBrokerId, listener, externalBrokerNodePort);

                                        String advertisedHostname = ListenersUtils.brokerAdvertisedHost(listener, finalBrokerId);

                                        if (advertisedHostname != null) {
                                            result.brokerDnsNames.computeIfAbsent(finalBrokerId, k -> new HashSet<>(1)).add(advertisedHostname);
                                        }

                                        registerAdvertisedHostname(finalBrokerId, listener, nodePortAddressEnvVar(listener));

                                        return Future.succeededFuture();
                                    });

                            perPodFutures.add(perBrokerFut);
                        }

                        return CompositeFuture.join(perPodFutures);
                    }).compose(res -> {
                        ListenerStatus ls = new ListenerStatusBuilder()
                                .withName(listener.getName())
                                .build();
                        result.listenerStatuses.add(ls);

                        return Future.succeededFuture();
                    });

            listenerFutures.add(perListenerFut);
        }

        return CompositeFuture
                .join(listenerFutures)
                .map((Void) null);
    }

    /**
     * Makes sure all routes are ready and collects their addresses for Statuses,
     * certificates and advertised addresses. This method for all routes:
     *      1) Checks if the bootstrap route has been provisioned (has a loadbalancer address)
     *      2) Collects the relevant addresses and stores them for use in certificates and in CR status
     *      3) Checks it the broker routes have been provisioned (have an address)
     *      4) Collects the route addresses for certificates and advertised hostnames
     *
     * @return  Future which completes when all Routes are ready and their addresses are collected
     */
    protected Future<Void> routesReady() {
        List<GenericKafkaListener> routeListeners = ListenersUtils.routeListeners(kafka.getListeners());
        @SuppressWarnings({ "rawtypes" }) // Has to use Raw type because of the CompositeFuture
        List<Future> listenerFutures = new ArrayList<>(routeListeners.size());

        for (GenericKafkaListener listener : routeListeners) {
            String bootstrapRouteName = ListenersUtils.backwardsCompatibleBootstrapRouteOrIngressName(reconciliation.name(), listener);

            @SuppressWarnings({ "rawtypes" }) // Has to use Raw type because of the CompositeFuture
            Future perListenerFut = routeOperator.hasAddress(reconciliation, reconciliation.namespace(), bootstrapRouteName, 1_000, operationTimeoutMs)
                    .compose(res -> routeOperator.getAsync(reconciliation.namespace(), bootstrapRouteName))
                    .compose(route -> {
                        String bootstrapAddress = route.getStatus().getIngress().get(0).getHost();
                        LOGGER.debugCr(reconciliation, "Found address {} for Route {}", bootstrapAddress, bootstrapRouteName);

                        result.bootstrapDnsNames.add(bootstrapAddress);

                        ListenerStatus ls = new ListenerStatusBuilder()
                                .withName(listener.getName())
                                .withAddresses(new ListenerAddressBuilder()
                                        .withHost(bootstrapAddress)
                                        .withPort(KafkaCluster.ROUTE_PORT)
                                        .build())
                                .build();
                        result.listenerStatuses.add(ls);

                        return Future.succeededFuture();
                    })
                    .compose(res -> {
                        @SuppressWarnings({ "rawtypes" }) // Has to use Raw type because of the CompositeFuture
                        List<Future> perPodFutures = new ArrayList<>(kafka.getReplicas());

                        for (int pod = 0; pod < kafka.getReplicas(); pod++)  {
                            perPodFutures.add(
                                    routeOperator.hasAddress(reconciliation, reconciliation.namespace(), ListenersUtils.backwardsCompatibleBrokerServiceName(reconciliation.name(), pod, listener), 1_000, operationTimeoutMs)
                            );
                        }

                        return CompositeFuture.join(perPodFutures);
                    })
                    .compose(res -> {
                        @SuppressWarnings({ "rawtypes" }) // Has to use Raw type because of the CompositeFuture
                        List<Future> perPodFutures = new ArrayList<>(kafka.getReplicas());

                        for (int brokerId = 0; brokerId < kafka.getReplicas(); brokerId++)  {
                            final int finalBrokerId = brokerId;
                            Future<Void> perBrokerFut = routeOperator.getAsync(reconciliation.namespace(), ListenersUtils.backwardsCompatibleBrokerServiceName(reconciliation.name(), brokerId, listener))
                                    .compose(route -> {
                                        String brokerAddress = route.getStatus().getIngress().get(0).getHost();
                                        LOGGER.debugCr(reconciliation, "Found address {} for Route {}", brokerAddress, route.getMetadata().getName());

                                        result.brokerDnsNames.computeIfAbsent(finalBrokerId, k -> new HashSet<>(2)).add(brokerAddress);

                                        String advertisedHostname = ListenersUtils.brokerAdvertisedHost(listener, finalBrokerId);
                                        if (advertisedHostname != null) {
                                            result.brokerDnsNames.get(finalBrokerId).add(ListenersUtils.brokerAdvertisedHost(listener, finalBrokerId));
                                        }

                                        registerAdvertisedHostname(finalBrokerId, listener, brokerAddress);
                                        registerAdvertisedPort(finalBrokerId, listener, KafkaCluster.ROUTE_PORT);

                                        return Future.succeededFuture();
                                    });

                            perPodFutures.add(perBrokerFut);
                        }

                        return CompositeFuture.join(perPodFutures);
                    });

            listenerFutures.add(perListenerFut);
        }

        return CompositeFuture
                .join(listenerFutures)
                .map((Void) null);
    }

    /**
     * Makes sure all ingresses are ready and collects their addresses for Statuses,
     * certificates and advertised addresses. This method for all ingresses:
     *      1) Checks if the bootstrap ingress has been provisioned (has a loadbalancer address)
     *      2) Collects the relevant addresses and stores them for use in certificates and in CR status
     *      3) Checks it the broker ingresses have been provisioned (have an address)
     *      4) Collects the route addresses for certificates and advertised hostnames
     *
     * @return  Future which completes when all Ingresses are ready and their addresses are collected
     */
    protected Future<Void> ingressesReady() {

        List<GenericKafkaListener> ingressListeners = ListenersUtils.ingressListeners(kafka.getListeners());
        @SuppressWarnings({ "rawtypes" }) // Has to use Raw type because of the CompositeFuture
        List<Future> listenerFutures = new ArrayList<>(ingressListeners.size());

        for (GenericKafkaListener listener : ingressListeners) {
            String bootstrapIngressName = ListenersUtils.backwardsCompatibleBootstrapRouteOrIngressName(reconciliation.name(), listener);

            Future<Void> perListenerFut = ingressOperator.hasIngressAddress(reconciliation, reconciliation.namespace(), bootstrapIngressName, 1_000, operationTimeoutMs)
                    .compose(res -> {
                        String bootstrapAddress = listener.getConfiguration().getBootstrap().getHost();
                        LOGGER.debugCr(reconciliation, "Using address {} for Ingress {}", bootstrapAddress, bootstrapIngressName);

                        result.bootstrapDnsNames.add(bootstrapAddress);

                        ListenerStatus ls = new ListenerStatusBuilder()
                                .withName(listener.getName())
                                .withAddresses(new ListenerAddressBuilder()
                                        .withHost(bootstrapAddress)
                                        .withPort(KafkaCluster.ROUTE_PORT)
                                        .build())
                                .build();
                        result.listenerStatuses.add(ls);

                        // Check if broker ingresses are ready
                        @SuppressWarnings({ "rawtypes" }) // Has to use Raw type because of the CompositeFuture
                        List<Future> perPodFutures = new ArrayList<>(kafka.getReplicas());

                        for (int pod = 0; pod < kafka.getReplicas(); pod++)  {
                            perPodFutures.add(
                                    ingressOperator.hasIngressAddress(reconciliation, reconciliation.namespace(), ListenersUtils.backwardsCompatibleBrokerServiceName(reconciliation.name(), pod, listener), 1_000, operationTimeoutMs)
                            );
                        }

                        return CompositeFuture.join(perPodFutures);
                    })
                    .compose(res -> {
                        for (int brokerId = 0; brokerId < kafka.getReplicas(); brokerId++)  {
                            final int finalBrokerId = brokerId;
                            String brokerAddress = listener.getConfiguration().getBrokers().stream()
                                    .filter(broker -> broker.getBroker() == finalBrokerId)
                                    .map(GenericKafkaListenerConfigurationBroker::getHost)
                                    .findAny()
                                    .orElse(null);
                            LOGGER.debugCr(reconciliation, "Using address {} for Ingress {}", brokerAddress, ListenersUtils.backwardsCompatibleBrokerServiceName(reconciliation.name(), brokerId, listener));

                            result.brokerDnsNames.computeIfAbsent(brokerId, k -> new HashSet<>(2)).add(brokerAddress);

                            String advertisedHostname = ListenersUtils.brokerAdvertisedHost(listener, finalBrokerId);
                            if (advertisedHostname != null) {
                                result.brokerDnsNames.get(finalBrokerId).add(ListenersUtils.brokerAdvertisedHost(listener, finalBrokerId));
                            }

                            registerAdvertisedHostname(finalBrokerId, listener, brokerAddress);
                            registerAdvertisedPort(finalBrokerId, listener, KafkaCluster.INGRESS_PORT);
                        }

                        return Future.succeededFuture();
                    });

            listenerFutures.add(perListenerFut);
        }

        return CompositeFuture
                .join(listenerFutures)
                .map((Void) null);
    }

    /**
     * Collects the custom listener certificates from the secrets and stores them for later use
     *
     * @return  Future which completes when all custom listener certificates are collected and are valid
     */
    protected Future<Map<String, String>> customListenerCertificates() {
        List<String> secretNames = kafka.getListeners().stream()
                .filter(listener -> listener.isTls()
                        && listener.getConfiguration() != null
                        && listener.getConfiguration().getBrokerCertChainAndKey() != null)
                .map(listener -> listener.getConfiguration().getBrokerCertChainAndKey().getSecretName())
                .distinct()
                .collect(Collectors.toList());
        LOGGER.debugCr(reconciliation, "Validating secret {} with custom TLS listener certificates", secretNames);

        @SuppressWarnings({ "rawtypes" }) // Has to use Raw type because of the CompositeFuture
        List<Future> secretFutures = new ArrayList<>(secretNames.size());
        Map<String, Secret> customSecrets = new HashMap<>(secretNames.size());

        for (String secretName : secretNames)   {
            Future<Secret> fut = secretOperator.getAsync(reconciliation.namespace(), secretName)
                    .compose(secret -> {
                        if (secret != null) {
                            customSecrets.put(secretName, secret);
                            LOGGER.debugCr(reconciliation, "Found secrets {} with custom TLS listener certificate", secretName);
                        }

                        return Future.succeededFuture();
                    });

            secretFutures.add(fut);
        }

        return CompositeFuture.join(secretFutures)
                .compose(res -> {
                    List<String> errors = new ArrayList<>();
                    Map<String, String> customListenerCertificates = new HashMap<>();

                    for (GenericKafkaListener listener : kafka.getListeners())   {
                        if (listener.isTls()
                                && listener.getConfiguration() != null
                                && listener.getConfiguration().getBrokerCertChainAndKey() != null)  {
                            CertAndKeySecretSource customCert = listener.getConfiguration().getBrokerCertChainAndKey();
                            Secret secret = customSecrets.get(customCert.getSecretName());

                            if (secret != null) {
                                if (!secret.getData().containsKey(customCert.getCertificate())) {
                                    errors.add("Secret " + customCert.getSecretName() + " does not contain certificate under the key " + customCert.getCertificate() + ".");
                                } else if (!secret.getData().containsKey(customCert.getKey())) {
                                    errors.add("Secret " + customCert.getSecretName() + " does not contain custom certificate private key under the key " + customCert.getKey() + ".");
                                } else  {
                                    byte[] publicKeyBytes = Base64.getDecoder().decode(secret.getData().get(customCert.getCertificate()));
                                    customListenerCertificates.put(listener.getName(), new String(publicKeyBytes, StandardCharsets.US_ASCII));
                                    result.customListenerCertificateThumbprints.put(listener.getName(), CertUtils.getCertificateShortThumbprint(secret, customCert.getCertificate()));
                                }
                            } else {
                                errors.add("Secret " + customCert.getSecretName() + " with custom TLS certificate does not exist.");
                            }

                        }
                    }

                    if (errors.isEmpty())   {
                        return Future.succeededFuture(customListenerCertificates);
                    } else {
                        LOGGER.errorCr(reconciliation, "Failed to process Secrets with custom certificates: {}", errors);
                        return Future.failedFuture(new InvalidResourceException("Failed to process Secrets with custom certificates: " + errors));
                    }
                });
    }

    /**
     * Adds certificate to the status of an individual listener. This is a utility method to simplify the code in
     * addCertificatesToListenerStatuses method
     *
     * @param listenerName      Name of the lister to which the certificate belongs
     * @param customCertificate Custom listener certificate or null if custom listener certificate is not used
     * @param caCertificate     The public key from the Cluster CA which is used when no custom listener certificate is set
     */
    private void addCertificateToListenerStatus(String listenerName, String customCertificate, String caCertificate)    {
        ListenerStatus status = result.listenerStatuses
                .stream()
                .filter(listenerStatus -> listenerName.equals(listenerStatus.getName()))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Status for listener " + listenerName + " not found"));

        status.setCertificates(singletonList(customCertificate != null ? customCertificate : caCertificate));
    }

    /**
     * Adds listener certificates to their statuses. This method goes through all TLS enabled listeners and updates
     * their status with either the custom listener certificate or with the CA certificate. This method should be
     * called only after This method should be called only after customListenerCertificates() which fills in the customListenerCertificates field.
     *
     * @return  Future which completes when all statuses are updated
     */
    protected Future<Void> addCertificatesToListenerStatuses(Map<String, String> customListenerCertificates) {
        String caCertificate = new String(clusterCa.currentCaCertBytes(), StandardCharsets.US_ASCII);

        for (GenericKafkaListener listener : kafka.getListeners())   {
            if (listener.isTls())   {
                LOGGER.debugCr(reconciliation, "Adding certificate to status for listener: {}", listener.getName());
                addCertificateToListenerStatus(listener.getName(), customListenerCertificates.get(listener.getName()), caCertificate);
            }
        }

        return Future.succeededFuture();
    }

    /**
     * Class used to carry the result of the reconciliation:
     *   - Prepared listener statuses
     *   - DNS names, ports, advertised addresses and advertised ports
     *   - Custom listener certificates
     */
    public static class ReconciliationResult {
        /**
         * List of ListenerStatus objects for the Kafka custom resource status
         */
        public final List<ListenerStatus> listenerStatuses = new ArrayList<>();

        /**
         * Bootstrap DNS names
         */
        public final Set<String> bootstrapDnsNames = new HashSet<>();

        /**
         * Broker DNS names
         */
        public final Map<Integer, Set<String>> brokerDnsNames = new HashMap<>();

        /**
         * Advertised hostnames
         */
        public final Map<Integer, Map<String, String>> advertisedHostnames = new HashMap<>();

        /**
         * Advertised ports
         */
        public final Map<Integer, Map<String, String>> advertisedPorts = new HashMap<>();

        /**
         * Bootstrap node ports
         */
        public final Map<String, Integer> bootstrapNodePorts = new HashMap<>();

        /**
         * Custom Listener certificates hash stubs to be used for rolling updates when the certificate changes
         */
        public final Map<String, String> customListenerCertificateThumbprints = new HashMap<>();
    }
}
