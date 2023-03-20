/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.plugin.security.profiles.impl;

import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.strimzi.plugin.security.profiles.ContainerSecurityProviderContext;

/**
 * An implementation of the PodSecurityProvider which implements the restricted Kubernetes security profile
 */
public class RestrictedPodSecurityProvider extends BaselinePodSecurityProvider {
    /**
     * Internal method which generates the restricted container security context. If any user-supplied pod security
     * context is set, it will be used. If not, then a context matching the restricted Kubernetes profile will be
     * provided.
     *
     * @param context   Context for providing the container security context
     *
     * @return  Returns the generated security context
     */
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
            // Kaniko does not support running under the restricted Kubernetes profile (needs to run as root and needs some capabilities to build containers)
            //    => if Kafka Connect Build is used, we throw an exception to not deploy it.
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

    @Override
    public SecurityContext bridgeInitContainerSecurityContext(ContainerSecurityProviderContext context) {
        return createRestrictedContainerSecurityContext(context);
    }
}
