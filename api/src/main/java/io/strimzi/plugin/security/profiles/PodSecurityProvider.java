/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.plugin.security.profiles;

import io.fabric8.kubernetes.api.model.PodSecurityContext;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.strimzi.platform.PlatformFeatures;

/**
 * Interface describing the Pod Security Provider. It contains the methods called to get security context for all the
 * different pods and containers used by Strimzi.
 */
public interface PodSecurityProvider {
    /**
     * A method called first to initiate the provider. It is always called before any of the other methods for providing
     * security context are called. This method can be used to preconfigure the provider based on the platform it is
     * running on. But it can also configure the provider based on information from additional sources (for example
     * environment variables).
     *
     * @param platformFeatures  Describes the platform we are running on and the features it provides
     */
    void configure(PlatformFeatures platformFeatures);

    /**
     * Provides the Pod security context for the ZooKeeper pods. The default implementation just returns the security
     * context configured by the user in the template section or null (no Pod security context).
     *
     * @param context   Provides the context which can be used to generate the Pod security context
     *
     * @return  Pod security context which will be set for the ZooKeeper pods
     */
    default PodSecurityContext zooKeeperPodSecurityContext(PodSecurityProviderContext context) {
        return podSecurityContextOrNull(context);
    }

    /**
     * Provides the (container) security context for the ZooKeeper containers. The default implementation just
     * returns the security context configured by the user in the template section or null (no security context).
     *
     * @param context   Provides the context which can be used to generate the security context
     *
     * @return  Security context which will be set for the ZooKeeper containers
     */
    default SecurityContext zooKeeperContainerSecurityContext(ContainerSecurityProviderContext context) {
        return securityContextOrNull(context);
    }

    /**
     * Provides the Pod security context for the Kafka pods. The default implementation just returns the security
     * context configured by the user in the template section or null (no Pod security context).
     *
     * @param context   Provides the context which can be used to generate the Pod security context
     *
     * @return  Pod security context which will be set for the Kafka pods
     */
    default PodSecurityContext kafkaPodSecurityContext(PodSecurityProviderContext context) {
        return podSecurityContextOrNull(context);
    }

    /**
     * Provides the (container) security context for the Kafka containers. The default implementation just
     * returns the security context configured by the user in the template section or null (no security context).
     *
     * @param context   Provides the context which can be used to generate the security context
     *
     * @return  Security context which will be set for the Kafka containers
     */
    default SecurityContext kafkaContainerSecurityContext(ContainerSecurityProviderContext context) {
        return securityContextOrNull(context);
    }

    /**
     * Provides the (container) security context for the Kafka init containers. The default implementation just
     * returns the security context configured by the user in the template section or null (no security context).
     *
     * @param context   Provides the context which can be used to generate the security context
     *
     * @return  Security context which will be set for the Kafka init containers
     */
    default SecurityContext kafkaInitContainerSecurityContext(ContainerSecurityProviderContext context) {
        return securityContextOrNull(context);
    }

    /**
     * Provides the Pod security context for the Entity Operator pod. The default implementation just returns the security
     * context configured by the user in the template section or null (no Pod security context).
     *
     * @param context   Provides the context which can be used to generate the Pod security context
     *
     * @return  Pod security context which will be set for the Entity Operator pod
     */
    default PodSecurityContext entityOperatorPodSecurityContext(PodSecurityProviderContext context) {
        return podSecurityContextOrNull(context);
    }

    /**
     * Provides the (container) security context for the Topic Operator container. The default implementation just
     * returns the security context configured by the user in the template section or null (no security context).
     *
     * @param context   Provides the context which can be used to generate the security context
     *
     * @return  Security context which will be set for the Topic Operator container
     */
    default SecurityContext entityTopicOperatorContainerSecurityContext(ContainerSecurityProviderContext context) {
        return securityContextOrNull(context);
    }

    /**
     * Provides the (container) security context for the User Operator container. The default implementation just
     * returns the security context configured by the user in the template section or null (no security context).
     *
     * @param context   Provides the context which can be used to generate the security context
     *
     * @return  Security context which will be set for the User Operator container
     */
    default SecurityContext entityUserOperatorContainerSecurityContext(ContainerSecurityProviderContext context) {
        return securityContextOrNull(context);
    }

    /**
     * Provides the (container) security context for the TLS sidecar container. The default implementation just
     * returns the security context configured by the user in the template section or null (no security context).
     *
     * @param context   Provides the context which can be used to generate the security context
     *
     * @return  Security context which will be set for the TLS sidecar container
     */
    default SecurityContext entityOperatorTlsSidecarContainerSecurityContext(ContainerSecurityProviderContext context) {
        return securityContextOrNull(context);
    }

    /**
     * Provides the Pod security context for the Kafka Exporter pod. The default implementation just returns the security
     * context configured by the user in the template section or null (no Pod security context).
     *
     * @param context   Provides the context which can be used to generate the Pod security context
     *
     * @return  Pod security context which will be set for the Kafka Exporter pod
     */
    default PodSecurityContext kafkaExporterPodSecurityContext(PodSecurityProviderContext context) {
        return podSecurityContextOrNull(context);
    }

    /**
     * Provides the (container) security context for the Kafka Exporter container. The default implementation just
     * returns the security context configured by the user in the template section or null (no security context).
     *
     * @param context   Provides the context which can be used to generate the security context
     *
     * @return  Security context which will be set for the Kafka Exporter container
     */
    default SecurityContext kafkaExporterContainerSecurityContext(ContainerSecurityProviderContext context) {
        return securityContextOrNull(context);
    }

    /**
     * Provides the Pod security context for the Cruise Control pod. The default implementation just returns the security
     * context configured by the user in the template section or null (no Pod security context).
     *
     * @param context   Provides the context which can be used to generate the Pod security context
     *
     * @return  Pod security context which will be set for the Cruise Control pod
     */
    default PodSecurityContext cruiseControlPodSecurityContext(PodSecurityProviderContext context) {
        return podSecurityContextOrNull(context);
    }

    /**
     * Provides the (container) security context for the Cruise Control container. The default implementation just
     * returns the security context configured by the user in the template section or null (no security context).
     *
     * @param context   Provides the context which can be used to generate the security context
     *
     * @return  Security context which will be set for the Cruise Control container
     */
    default SecurityContext cruiseControlContainerSecurityContext(ContainerSecurityProviderContext context) {
        return securityContextOrNull(context);
    }

    /**
     * Provides the Pod security context for the JMXTrans pod. The default implementation just returns the security
     * context configured by the user in the template section or null (no Pod security context).
     *
     * @param context   Provides the context which can be used to generate the Pod security context
     *
     * @return  Pod security context which will be set for the JMXTrans pod
     */
    default PodSecurityContext jmxTransPodSecurityContext(PodSecurityProviderContext context) {
        return podSecurityContextOrNull(context);
    }

    /**
     * Provides the (container) security context for the JMXTrans container. The default implementation just
     * returns the security context configured by the user in the template section or null (no security context).
     *
     * @param context   Provides the context which can be used to generate the security context
     *
     * @return  Security context which will be set for the JMXTrans container
     */
    default SecurityContext jmxTransContainerSecurityContext(ContainerSecurityProviderContext context) {
        return securityContextOrNull(context);
    }

    /**
     * Provides the Pod security context for the Kafka Connect pods. The default implementation just returns the security
     * context configured by the user in the template section or null (no Pod security context).
     *
     * @param context   Provides the context which can be used to generate the Pod security context
     *
     * @return  Pod security context which will be set for the Kafka Connect pods
     */
    default PodSecurityContext kafkaConnectPodSecurityContext(PodSecurityProviderContext context) {
        return podSecurityContextOrNull(context);
    }

    /**
     * Provides the (container) security context for the Kafka Connect containers. The default implementation just
     * returns the security context configured by the user in the template section or null (no security context).
     *
     * @param context   Provides the context which can be used to generate the security context
     *
     * @return  Security context which will be set for the Kafka Connect containers
     */
    default SecurityContext kafkaConnectContainerSecurityContext(ContainerSecurityProviderContext context) {
        return securityContextOrNull(context);
    }

    /**
     * Provides the (container) security context for the Kafka Connect init containers. The default implementation just
     * returns the security context configured by the user in the template section or null (no security context).
     *
     * @param context   Provides the context which can be used to generate the security context
     *
     * @return  Security context which will be set for the Kafka Connect init containers
     */
    default SecurityContext kafkaConnectInitContainerSecurityContext(ContainerSecurityProviderContext context) {
        return securityContextOrNull(context);
    }


    /**
     * Provides the Pod security context for the Kafka Connect Build (Kaniko) pod. The default implementation just returns
     * the security context configured by the user in the template section or null (no Pod security context).
     *
     * @param context   Provides the context which can be used to generate the Pod security context
     *
     * @return  Pod security context which will be set for the Kafka Connect Build (Kaniko) pod
     */
    default PodSecurityContext kafkaConnectBuildPodSecurityContext(PodSecurityProviderContext context) {
        return podSecurityContextOrNull(context);
    }

    /**
     * Provides the (container) security context for the Kafka Connect Build container. The default implementation just
     * returns the security context configured by the user in the template section or null (no security context).
     *
     * @param context   Provides the context which can be used to generate the security context
     *
     * @return  Security context which will be set for the Kafka Connect Build container
     */
    default SecurityContext kafkaConnectBuildContainerSecurityContext(ContainerSecurityProviderContext context) {
        return securityContextOrNull(context);
    }

    /**
     * Provides the Pod security context for the Kafka Mirror Maker 1 pods. The default implementation just returns the
     * security context configured by the user in the template section or null (no Pod security context).
     *
     * @param context   Provides the context which can be used to generate the Pod security context
     *
     * @return  Pod security context which will be set for the Kafka Mirror Maker 1 pods
     */
    default PodSecurityContext kafkaMirrorMakerPodSecurityContext(PodSecurityProviderContext context) {
        return podSecurityContextOrNull(context);
    }

    /**
     * Provides the (container) security context for the Kafka Mirror Maker 1 containers. The default implementation just
     * returns the security context configured by the user in the template section or null (no security context).
     *
     * @param context   Provides the context which can be used to generate the security context
     *
     * @return  Security context which will be set for the Kafka Mirror Maker 1 containers
     */
    default SecurityContext kafkaMirrorMakerContainerSecurityContext(ContainerSecurityProviderContext context) {
        return securityContextOrNull(context);
    }

    /**
     * Provides the Pod security context for the Strimzi Bridge pods. The default implementation just returns the security
     * context configured by the user in the template section or null (no Pod security context).
     *
     * @param context   Provides the context which can be used to generate the Pod security context
     *
     * @return  Pod security context which will be set for the Strimzi Bridge pods
     */
    default PodSecurityContext bridgePodSecurityContext(PodSecurityProviderContext context) {
        return podSecurityContextOrNull(context);
    }

    /**
     * Provides the (container) security context for the Strimzi Bridge containers. The default implementation just
     * returns the security context configured by the user in the template section or null (no security context).
     *
     * @param context   Provides the context which can be used to generate the security context
     *
     * @return  Security context which will be set for the Strimzi Bridge containers
     */
    default SecurityContext bridgeContainerSecurityContext(ContainerSecurityProviderContext context) {
        return securityContextOrNull(context);
    }
    
    /**
     * Provides the (container) security context for the Kafka Bridge init containers. The default implementation just
     * returns the security context configured by the user in the template section or null (no security context).
     *
     * @param context   Provides the context which can be used to generate the security context
     *
     * @return  Security context which will be set for the Kafka Bridge init containers
     */
    default SecurityContext bridgeInitContainerSecurityContext(ContainerSecurityProviderContext context) {
        return securityContextOrNull(context);
    }


    /**
     * Internal method to handle the default Pod security context.
     *
     * @param context   Context for generating the Pod security context
     *
     * @return  If any user-supplied Pod security context is set, it will be returned. Otherwise, it returns null
     *                (i.e. no pod security context).
     */
    private PodSecurityContext podSecurityContextOrNull(PodSecurityProviderContext context)  {
        if (context != null)   {
            return context.userSuppliedSecurityContext();
        } else {
            return null;
        }
    }

    /**
     * Internal method to handle the default (container) security context.
     *
     * @param context   Context for generating the (container) security context
     *
     * @return  If any user-supplied (container) security context is set, it will be returned. Otherwise, it returns null
     *                (i.e. no security context).
     */
    private SecurityContext securityContextOrNull(ContainerSecurityProviderContext context)  {
        if (context != null)   {
            return context.userSuppliedSecurityContext();
        } else {
            return null;
        }
    }
}
