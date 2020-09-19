/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.listener.arraylistener;

import io.strimzi.api.kafka.model.listener.IngressListenerBrokerConfiguration;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternal;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternalIngress;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternalLoadBalancer;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternalNodePort;
import io.strimzi.api.kafka.model.listener.KafkaListenerExternalRoute;
import io.strimzi.api.kafka.model.listener.KafkaListenerPlain;
import io.strimzi.api.kafka.model.listener.KafkaListenerTls;
import io.strimzi.api.kafka.model.listener.KafkaListeners;
import io.strimzi.api.kafka.model.listener.LoadBalancerListenerBrokerOverride;
import io.strimzi.api.kafka.model.listener.NodePortListenerBrokerOverride;
import io.strimzi.api.kafka.model.listener.RouteListenerBrokerOverride;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Util methods used for working with Kafka listeners
 */
public class ListenersConvertor {
    /**
     * Converts the Kafka listener configuration from old format to the new format
     *
     * @param oldListeners  Old Kafka listeners configuration
     * @return  New Kafka listeners configuration
     */
    @SuppressWarnings("deprecation")
    public static List<GenericKafkaListener> convertToNewFormat(KafkaListeners oldListeners)    {
        List<GenericKafkaListener> newListeners = new ArrayList<>(3);

        if (oldListeners != null)   {
            if (oldListeners.getPlain() != null)    {
                newListeners.add(convertPlainListener(oldListeners.getPlain()));
            }

            if (oldListeners.getTls() != null)    {
                newListeners.add(convertTlsListener(oldListeners.getTls()));
            }

            if (oldListeners.getExternal() != null)    {
                newListeners.add(convertExternalListener(oldListeners.getExternal()));
            }
        }

        return newListeners;
    }

    /**
     * Converts plain listener into the new format
     *
     * @param plain Plain listener in old format
     * @return  Listener in the new format
     */
    /*test*/ static GenericKafkaListener convertPlainListener(KafkaListenerPlain plain)  {
        return new GenericKafkaListenerBuilder()
                .withName("plain")
                .withPort(9092)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(false)
                .withAuth(plain.getAuth())
                .withNetworkPolicyPeers(plain.getNetworkPolicyPeers())
                .build();
    }

    /**
     * Converts TLS listener into the new format
     *
     * @param tls TLS listener in old format
     * @return  Listener in the new format
     */
    /*test*/ static GenericKafkaListener convertTlsListener(KafkaListenerTls tls)  {
        GenericKafkaListenerConfiguration configuration = null;

        if (tls.getConfiguration() != null) {
            configuration = new GenericKafkaListenerConfigurationBuilder()
                    .withBrokerCertChainAndKey(tls.getConfiguration().getBrokerCertChainAndKey())
                    .build();
        }

        return new GenericKafkaListenerBuilder()
                .withName("tls")
                .withPort(9093)
                .withType(KafkaListenerType.INTERNAL)
                .withTls(true)
                .withAuth(tls.getAuth())
                .withNetworkPolicyPeers(tls.getNetworkPolicyPeers())
                .withConfiguration(configuration)
                .build();
    }

    /**
     * Converts External listener into the new format
     *
     * @param external External listener in old format
     * @return  Listener in the new format
     */
    /*test*/ static GenericKafkaListener convertExternalListener(KafkaListenerExternal external)  {
        GenericKafkaListener listener = new GenericKafkaListenerBuilder()
                .withName("external")
                .withPort(9094)
                .withAuth(external.getAuth())
                .withNetworkPolicyPeers(external.getNetworkPolicyPeers())
                .build();

        if (KafkaListenerExternalLoadBalancer.TYPE_LOADBALANCER.equals(external.getType())) {
            convertLoadBalancerListener(listener, (KafkaListenerExternalLoadBalancer) external);
        } else if (KafkaListenerExternalNodePort.TYPE_NODEPORT.equals(external.getType())) {
            convertNodePortListener(listener, (KafkaListenerExternalNodePort) external);
        } else if (KafkaListenerExternalRoute.TYPE_ROUTE.equals(external.getType())) {
            convertRouteListener(listener, (KafkaListenerExternalRoute) external);
        } else if (KafkaListenerExternalIngress.TYPE_INGRESS.equals(external.getType())) {
            convertIngressListener(listener, (KafkaListenerExternalIngress) external);
        }

        return listener;
    }

    /**
     * Converts Load Balancer listener from old to new configuration
     *
     * @param newListener   New listener which will be configured
     * @param oldListener   Old listener
     */
    private static void convertLoadBalancerListener(GenericKafkaListener newListener, KafkaListenerExternalLoadBalancer oldListener)    {
        newListener.setType(KafkaListenerType.LOADBALANCER);
        newListener.setTls(oldListener.isTls());

        if (oldListener.getConfiguration() == null && oldListener.getOverrides() == null) {
            return;
        }

        GenericKafkaListenerConfiguration configuration = new GenericKafkaListenerConfiguration();

        if (oldListener.getConfiguration() != null) {
            configuration.setBrokerCertChainAndKey(oldListener.getConfiguration().getBrokerCertChainAndKey());
        }

        if (oldListener.getOverrides() != null) {
            if (oldListener.getOverrides().getBootstrap() != null) {
                GenericKafkaListenerConfigurationBootstrap bootstrapConfiguration = new GenericKafkaListenerConfigurationBootstrap();
                bootstrapConfiguration.setLoadBalancerIP(oldListener.getOverrides().getBootstrap().getLoadBalancerIP());
                bootstrapConfiguration.setAnnotations(oldListener.getOverrides().getBootstrap().getDnsAnnotations());

                if (oldListener.getOverrides().getBootstrap().getAddress() != null) {
                    bootstrapConfiguration.setAlternativeNames(Collections.singletonList(oldListener.getOverrides().getBootstrap().getAddress()));
                }

                configuration.setBootstrap(bootstrapConfiguration);
            }

            if (oldListener.getOverrides().getBrokers() != null) {
                List<GenericKafkaListenerConfigurationBroker> brokers = new ArrayList<>(oldListener.getOverrides().getBrokers().size());

                for (LoadBalancerListenerBrokerOverride oldBroker : oldListener.getOverrides().getBrokers()) {
                    GenericKafkaListenerConfigurationBroker brokerConfiguration = new GenericKafkaListenerConfigurationBroker();
                    brokerConfiguration.setBroker(oldBroker.getBroker());
                    brokerConfiguration.setLoadBalancerIP(oldBroker.getLoadBalancerIP());
                    brokerConfiguration.setAnnotations(oldBroker.getDnsAnnotations());
                    brokerConfiguration.setAdvertisedHost(oldBroker.getAdvertisedHost());
                    brokerConfiguration.setAdvertisedPort(oldBroker.getAdvertisedPort());

                    brokers.add(brokerConfiguration);
                }

                configuration.setBrokers(brokers);
            }
        }

        newListener.setConfiguration(configuration);
    }

    /**
     * Converts Node Port listener from old to new configuration
     *
     * @param newListener   New listener which will be configured
     * @param oldListener   Old listener
     */
    private static void convertNodePortListener(GenericKafkaListener newListener, KafkaListenerExternalNodePort oldListener)    {
        newListener.setType(KafkaListenerType.NODEPORT);
        newListener.setTls(oldListener.isTls());

        if (oldListener.getConfiguration() == null && oldListener.getOverrides() == null) {
            return;
        }

        GenericKafkaListenerConfiguration configuration = new GenericKafkaListenerConfiguration();

        if (oldListener.getConfiguration() != null) {
            configuration.setBrokerCertChainAndKey(oldListener.getConfiguration().getBrokerCertChainAndKey());
            configuration.setPreferredNodePortAddressType(oldListener.getConfiguration().getPreferredAddressType());
        }

        if (oldListener.getOverrides() != null) {
            if (oldListener.getOverrides().getBootstrap() != null) {
                GenericKafkaListenerConfigurationBootstrap bootstrapConfiguration = new GenericKafkaListenerConfigurationBootstrap();
                bootstrapConfiguration.setNodePort(oldListener.getOverrides().getBootstrap().getNodePort());
                bootstrapConfiguration.setAnnotations(oldListener.getOverrides().getBootstrap().getDnsAnnotations());

                if (oldListener.getOverrides().getBootstrap().getAddress() != null) {
                    bootstrapConfiguration.setAlternativeNames(Collections.singletonList(oldListener.getOverrides().getBootstrap().getAddress()));
                }

                configuration.setBootstrap(bootstrapConfiguration);
            }

            if (oldListener.getOverrides().getBrokers() != null) {
                List<GenericKafkaListenerConfigurationBroker> brokers = new ArrayList<>(oldListener.getOverrides().getBrokers().size());

                for (NodePortListenerBrokerOverride oldBroker : oldListener.getOverrides().getBrokers()) {
                    GenericKafkaListenerConfigurationBroker brokerConfiguration = new GenericKafkaListenerConfigurationBroker();
                    brokerConfiguration.setBroker(oldBroker.getBroker());
                    brokerConfiguration.setNodePort(oldBroker.getNodePort());
                    brokerConfiguration.setAnnotations(oldBroker.getDnsAnnotations());
                    brokerConfiguration.setAdvertisedHost(oldBroker.getAdvertisedHost());
                    brokerConfiguration.setAdvertisedPort(oldBroker.getAdvertisedPort());

                    brokers.add(brokerConfiguration);
                }

                configuration.setBrokers(brokers);
            }
        }

        newListener.setConfiguration(configuration);
    }

    /**
     * Converts Route listener from old to new configuration
     *
     * @param newListener   New listener which will be configured
     * @param oldListener   Old listener
     */
    private static void convertRouteListener(GenericKafkaListener newListener, KafkaListenerExternalRoute oldListener)    {
        newListener.setType(KafkaListenerType.ROUTE);
        newListener.setTls(true);

        if (oldListener.getConfiguration() != null || oldListener.getOverrides() != null) {
            GenericKafkaListenerConfiguration configuration = new GenericKafkaListenerConfiguration();

            if (oldListener.getConfiguration() != null) {
                configuration.setBrokerCertChainAndKey(oldListener.getConfiguration().getBrokerCertChainAndKey());
            }

            if (oldListener.getOverrides() != null) {
                if (oldListener.getOverrides().getBootstrap() != null) {
                    GenericKafkaListenerConfigurationBootstrap bootstrapConfiguration = new GenericKafkaListenerConfigurationBootstrap();
                    bootstrapConfiguration.setHost(oldListener.getOverrides().getBootstrap().getHost());

                    if (oldListener.getOverrides().getBootstrap().getAddress() != null) {
                        bootstrapConfiguration.setAlternativeNames(Collections.singletonList(oldListener.getOverrides().getBootstrap().getAddress()));
                    }

                    configuration.setBootstrap(bootstrapConfiguration);
                }

                if (oldListener.getOverrides().getBrokers() != null) {
                    List<GenericKafkaListenerConfigurationBroker> brokers = new ArrayList<>(oldListener.getOverrides().getBrokers().size());

                    for (RouteListenerBrokerOverride oldBroker : oldListener.getOverrides().getBrokers()) {
                        GenericKafkaListenerConfigurationBroker brokerConfiguration = new GenericKafkaListenerConfigurationBroker();
                        brokerConfiguration.setBroker(oldBroker.getBroker());
                        brokerConfiguration.setHost(oldBroker.getHost());
                        brokerConfiguration.setAdvertisedHost(oldBroker.getAdvertisedHost());
                        brokerConfiguration.setAdvertisedPort(oldBroker.getAdvertisedPort());

                        brokers.add(brokerConfiguration);
                    }

                    configuration.setBrokers(brokers);
                }
            }

            newListener.setConfiguration(configuration);
        }
    }

    /**
     * Converts Ingress listener from old to new configuration
     *
     * @param newListener   New listener which will be configured
     * @param oldListener   Old listener
     */
    private static void convertIngressListener(GenericKafkaListener newListener, KafkaListenerExternalIngress oldListener)    {
        newListener.setType(KafkaListenerType.INGRESS);
        newListener.setTls(true);

        if (oldListener.getConfiguration() != null || oldListener.getIngressClass() != null) {
            GenericKafkaListenerConfiguration configuration = new GenericKafkaListenerConfiguration();
            configuration.setIngressClass(oldListener.getIngressClass());

            if (oldListener.getConfiguration() != null) {
                configuration.setBrokerCertChainAndKey(oldListener.getConfiguration().getBrokerCertChainAndKey());

                if (oldListener.getConfiguration().getBootstrap() != null) {
                    GenericKafkaListenerConfigurationBootstrap bootstrapConfiguration = new GenericKafkaListenerConfigurationBootstrap();
                    bootstrapConfiguration.setHost(oldListener.getConfiguration().getBootstrap().getHost());
                    bootstrapConfiguration.setAnnotations(oldListener.getConfiguration().getBootstrap().getDnsAnnotations());

                    if (oldListener.getConfiguration().getBootstrap().getAddress() != null) {
                        bootstrapConfiguration.setAlternativeNames(Collections.singletonList(oldListener.getConfiguration().getBootstrap().getAddress()));
                    }

                    configuration.setBootstrap(bootstrapConfiguration);
                }

                if (oldListener.getConfiguration().getBrokers() != null) {
                    List<GenericKafkaListenerConfigurationBroker> brokers = new ArrayList<>(oldListener.getConfiguration().getBrokers().size());

                    for (IngressListenerBrokerConfiguration oldBroker : oldListener.getConfiguration().getBrokers()) {
                        GenericKafkaListenerConfigurationBroker brokerConfiguration = new GenericKafkaListenerConfigurationBroker();
                        brokerConfiguration.setBroker(oldBroker.getBroker());
                        brokerConfiguration.setHost(oldBroker.getHost());
                        brokerConfiguration.setAnnotations(oldBroker.getDnsAnnotations());
                        brokerConfiguration.setAdvertisedHost(oldBroker.getAdvertisedHost());
                        brokerConfiguration.setAdvertisedPort(oldBroker.getAdvertisedPort());

                        brokers.add(brokerConfiguration);
                    }

                    configuration.setBrokers(brokers);
                }
            }

            newListener.setConfiguration(configuration);
        }
    }
}
