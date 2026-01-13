/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.strimzi.plugin.security.profiles.ContainerSecurityProviderContext;
import io.strimzi.plugin.security.profiles.PodSecurityProviderContext;
import io.strimzi.plugin.security.profiles.impl.RestrictedPodSecurityProvider;

/**
 * This is a test security provider used for testing how security providers integrate into the generated Kubernetes
 * resources. As the RestrictedPodSEcurityProvider does not return values for all fields, we can use this Test provider
 * instead.
 */
public class TestPodSecurityProvider extends RestrictedPodSecurityProvider {
    public TestPodSecurityProvider() {
        super();
    }

    @Override
    public Boolean kafkaHostUsers(PodSecurityProviderContext context) {
        return false;
    }

    @Override
    public Boolean entityOperatorHostUsers(PodSecurityProviderContext context) {
        return false;
    }

    @Override
    public Boolean kafkaExporterHostUsers(PodSecurityProviderContext context) {
        return false;
    }

    @Override
    public Boolean cruiseControlHostUsers(PodSecurityProviderContext context) {
        return false;
    }

    @Override
    public Boolean kafkaConnectHostUsers(PodSecurityProviderContext context) {
        return false;
    }

    @Override
    public Boolean kafkaConnectBuildHostUsers(PodSecurityProviderContext context) {
        return false;
    }

    @Override
    public Boolean bridgeHostUsers(PodSecurityProviderContext context) {
        return false;
    }

    @Override
    public SecurityContext kafkaConnectBuildContainerSecurityContext(ContainerSecurityProviderContext context) {
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
