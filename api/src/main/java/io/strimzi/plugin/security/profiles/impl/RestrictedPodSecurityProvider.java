/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.plugin.security.profiles.impl;

import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.strimzi.plugin.security.profiles.ContainerSecurityProviderContext;

public class RestrictedPodSecurityProvider extends BaselinePodSecurityProvider {
    private SecurityContext createRestrictedContainerSecurityContext(ContainerSecurityProviderContext context)    {
        if (context == null)    {
            return null;
        } else if (context.userSuppliedSecurityContext() != null)    {
            return context.userSuppliedSecurityContext();
        } else {
            return new SecurityContextBuilder()
                    .withAllowPrivilegeEscalation(false)
                    .withRunAsNonRoot(true)
                    .withNewSeccompProfile()
                        .withType("RuntimeDefault")
                    .endSeccompProfile()
                    .withNewCapabilities()
                        .withDrop("ALL")
                    .endCapabilities()
                    .build();
        }
    }

    @Override
    public SecurityContext zooKeeperContainerSecurityContext(ContainerSecurityProviderContext context) {
        return createRestrictedContainerSecurityContext(context);
    }

    @Override
    public SecurityContext kafkaContainerSecurityContext(ContainerSecurityProviderContext context) {
        return createRestrictedContainerSecurityContext(context);
    }

    @Override
    public SecurityContext kafkaInitContainerSecurityContext(ContainerSecurityProviderContext context) {
        return createRestrictedContainerSecurityContext(context);
    }

    @Override
    public SecurityContext entityTopicOperatorContainerSecurityContext(ContainerSecurityProviderContext context) {
        return createRestrictedContainerSecurityContext(context);
    }

    @Override
    public SecurityContext entityUserOperatorContainerSecurityContext(ContainerSecurityProviderContext context) {
        return createRestrictedContainerSecurityContext(context);
    }

    @Override
    public SecurityContext entityOperatorTlsSidecarContainerSecurityContext(ContainerSecurityProviderContext context) {
        return createRestrictedContainerSecurityContext(context);
    }

    @Override
    public SecurityContext kafkaExporterContainerSecurityContext(ContainerSecurityProviderContext context) {
        return createRestrictedContainerSecurityContext(context);
    }

    @Override
    public SecurityContext cruiseControlContainerSecurityContext(ContainerSecurityProviderContext context) {
        return createRestrictedContainerSecurityContext(context);
    }

    @Override
    public SecurityContext jmxTransContainerSecurityContext(ContainerSecurityProviderContext context) {
        return createRestrictedContainerSecurityContext(context);
    }

    @Override
    public SecurityContext kafkaConnectContainerSecurityContext(ContainerSecurityProviderContext context) {
        return createRestrictedContainerSecurityContext(context);
    }

    @Override
    public SecurityContext kafkaConnectInitContainerSecurityContext(ContainerSecurityProviderContext context) {
        return createRestrictedContainerSecurityContext(context);
    }

    @Override
    public SecurityContext kafkaConnectBuildContainerSecurityContext(ContainerSecurityProviderContext context) {
        if (context != null
                && context.userSuppliedSecurityContext() != null)    {
            return context.userSuppliedSecurityContext();
        } else {
            throw new UnsupportedOperationException("Kafka Connect Build using the Kaniko builder is not available under the restricted security profile");
        }
    }

    @Override
    public SecurityContext kafkaMirrorMakerContainerSecurityContext(ContainerSecurityProviderContext context) {
        return createRestrictedContainerSecurityContext(context);
    }

    @Override
    public SecurityContext bridgeContainerSecurityContext(ContainerSecurityProviderContext context) {
        return createRestrictedContainerSecurityContext(context);
    }
}
