/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.plugin.security.profiles;

import io.fabric8.kubernetes.api.model.PodSecurityContext;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.strimzi.platform.PlatformFeatures;

public interface PodSecurityProvider {
    void configure(PlatformFeatures platformFeatures);

    default PodSecurityContext zooKeeperPodSecurityContext(PodSecurityProviderContext context) {
        return podSecurityContextOrNull(context);
    }

    default SecurityContext zooKeeperContainerSecurityContext(ContainerSecurityProviderContext context) {
        return securityContextOrNull(context);
    }

    default PodSecurityContext kafkaPodSecurityContext(PodSecurityProviderContext context) {
        return podSecurityContextOrNull(context);
    }

    default SecurityContext kafkaContainerSecurityContext(ContainerSecurityProviderContext context) {
        return securityContextOrNull(context);
    }

    default SecurityContext kafkaInitContainerSecurityContext(ContainerSecurityProviderContext context) {
        return securityContextOrNull(context);
    }

    default PodSecurityContext entityOperatorPodSecurityContext(PodSecurityProviderContext context) {
        return podSecurityContextOrNull(context);
    }

    default SecurityContext entityTopicOperatorContainerSecurityContext(ContainerSecurityProviderContext context) {
        return securityContextOrNull(context);
    }

    default SecurityContext entityUserOperatorContainerSecurityContext(ContainerSecurityProviderContext context) {
        return securityContextOrNull(context);
    }

    default SecurityContext entityOperatorTlsSidecarContainerSecurityContext(ContainerSecurityProviderContext context) {
        return securityContextOrNull(context);
    }

    default PodSecurityContext kafkaExporterPodSecurityContext(PodSecurityProviderContext context) {
        return podSecurityContextOrNull(context);
    }

    default SecurityContext kafkaExporterContainerSecurityContext(ContainerSecurityProviderContext context) {
        return securityContextOrNull(context);
    }

    default PodSecurityContext cruiseControlPodSecurityContext(PodSecurityProviderContext context) {
        return podSecurityContextOrNull(context);
    }

    default SecurityContext cruiseControlContainerSecurityContext(ContainerSecurityProviderContext context) {
        return securityContextOrNull(context);
    }

    default PodSecurityContext jmxTransPodSecurityContext(PodSecurityProviderContext context) {
        return podSecurityContextOrNull(context);
    }

    default SecurityContext jmxTransContainerSecurityContext(ContainerSecurityProviderContext context) {
        return securityContextOrNull(context);
    }

    default PodSecurityContext kafkaConnectPodSecurityContext(PodSecurityProviderContext context) {
        return podSecurityContextOrNull(context);
    }

    default SecurityContext kafkaConnectContainerSecurityContext(ContainerSecurityProviderContext context) {
        return securityContextOrNull(context);
    }

    default SecurityContext kafkaConnectInitContainerSecurityContext(ContainerSecurityProviderContext context) {
        return securityContextOrNull(context);
    }

    default PodSecurityContext kafkaConnectBuildPodSecurityContext(PodSecurityProviderContext context) {
        return podSecurityContextOrNull(context);
    }

    default SecurityContext kafkaConnectBuildContainerSecurityContext(ContainerSecurityProviderContext context) {
        return securityContextOrNull(context);
    }

    default PodSecurityContext kafkaMirrorMakerPodSecurityContext(PodSecurityProviderContext context) {
        return podSecurityContextOrNull(context);
    }

    default SecurityContext kafkaMirrorMakerContainerSecurityContext(ContainerSecurityProviderContext context) {
        return securityContextOrNull(context);
    }

    default PodSecurityContext bridgePodSecurityContext(PodSecurityProviderContext context) {
        return podSecurityContextOrNull(context);
    }

    default SecurityContext bridgeContainerSecurityContext(ContainerSecurityProviderContext context) {
        return securityContextOrNull(context);
    }

    private PodSecurityContext podSecurityContextOrNull(PodSecurityProviderContext context)  {
        if (context != null)   {
            return context.userSuppliedSecurityContext();
        } else {
            return null;
        }
    }

    private SecurityContext securityContextOrNull(ContainerSecurityProviderContext context)  {
        if (context != null)   {
            return context.userSuppliedSecurityContext();
        } else {
            return null;
        }
    }
}
