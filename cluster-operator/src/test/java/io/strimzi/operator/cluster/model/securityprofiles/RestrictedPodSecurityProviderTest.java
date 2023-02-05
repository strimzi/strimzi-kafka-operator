/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.securityprofiles;

import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.strimzi.api.kafka.model.template.ContainerTemplate;
import io.strimzi.plugin.security.profiles.PodSecurityProvider;
import io.strimzi.plugin.security.profiles.impl.RestrictedPodSecurityProvider;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RestrictedPodSecurityProviderTest extends BaselinePodSecurityProviderTest {
    protected final static SecurityContext RESTRICTED_CONTAINER_SECURITY_CONTEXT = new SecurityContextBuilder()
            .withAllowPrivilegeEscalation(false)
            .withRunAsNonRoot(true)
            .withNewSeccompProfile()
                .withType("RuntimeDefault")
            .endSeccompProfile()
            .withNewCapabilities()
                .withDrop("ALL")
            .endCapabilities()
            .build();

    protected PodSecurityProvider createProvider()    {
        return new RestrictedPodSecurityProvider();
    }

    @Test
    public void testContainerContext() {
        PodSecurityProvider provider = createProvider();
        provider.configure(ON_KUBERNETES);

        assertThat(provider.zooKeeperContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.zooKeeperContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.zooKeeperContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.zooKeeperContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));

        assertThat(provider.kafkaContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));

        assertThat(provider.kafkaInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));

        assertThat(provider.entityTopicOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.entityTopicOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.entityTopicOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.entityTopicOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));

        assertThat(provider.entityUserOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.entityUserOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.entityUserOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.entityUserOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));

        assertThat(provider.entityOperatorTlsSidecarContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.entityOperatorTlsSidecarContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.entityOperatorTlsSidecarContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.entityOperatorTlsSidecarContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));

        assertThat(provider.kafkaExporterContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaExporterContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaExporterContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaExporterContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));

        assertThat(provider.cruiseControlContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.cruiseControlContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.cruiseControlContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.cruiseControlContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));

        assertThat(provider.jmxTransContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.jmxTransContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.jmxTransContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.jmxTransContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));

        assertThat(provider.kafkaConnectContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaConnectContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaConnectContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaConnectContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));

        assertThat(provider.kafkaConnectInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaConnectInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaConnectInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaConnectInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));

        assertThat(provider.kafkaMirrorMakerContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaMirrorMakerContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaMirrorMakerContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaMirrorMakerContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));

        assertThat(provider.bridgeContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.bridgeContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.bridgeContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.bridgeContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));

        assertThrows(UnsupportedOperationException.class, () -> provider.kafkaConnectBuildContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, null)));
        assertThrows(UnsupportedOperationException.class, () -> provider.kafkaConnectBuildContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, null)));
        assertThrows(UnsupportedOperationException.class, () -> provider.kafkaConnectBuildContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, null)));
        assertThrows(UnsupportedOperationException.class, () -> provider.kafkaConnectBuildContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, null)));
    }

    @Test
    public void testContainerContextWithEmptyTemplate() {
        PodSecurityProvider provider = createProvider();
        provider.configure(ON_KUBERNETES);

        assertThat(provider.zooKeeperContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.zooKeeperContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.zooKeeperContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.zooKeeperContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));

        assertThat(provider.kafkaContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));

        assertThat(provider.kafkaInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));

        assertThat(provider.entityTopicOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.entityTopicOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.entityTopicOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.entityTopicOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));

        assertThat(provider.entityUserOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.entityUserOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.entityUserOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.entityUserOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));

        assertThat(provider.entityOperatorTlsSidecarContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.entityOperatorTlsSidecarContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.entityOperatorTlsSidecarContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.entityOperatorTlsSidecarContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));

        assertThat(provider.kafkaExporterContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaExporterContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaExporterContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaExporterContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));

        assertThat(provider.cruiseControlContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.cruiseControlContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.cruiseControlContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.cruiseControlContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));

        assertThat(provider.jmxTransContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.jmxTransContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.jmxTransContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.jmxTransContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));

        assertThat(provider.kafkaConnectContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaConnectContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaConnectContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaConnectContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));

        assertThat(provider.kafkaConnectInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaConnectInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaConnectInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaConnectInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));

        assertThat(provider.kafkaMirrorMakerContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaMirrorMakerContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaMirrorMakerContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.kafkaMirrorMakerContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));

        assertThat(provider.bridgeContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.bridgeContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.bridgeContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));
        assertThat(provider.bridgeContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, new ContainerTemplate())), CoreMatchers.is(RESTRICTED_CONTAINER_SECURITY_CONTEXT));

        assertThrows(UnsupportedOperationException.class, () -> provider.kafkaConnectBuildContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, new ContainerTemplate())));
        assertThrows(UnsupportedOperationException.class, () -> provider.kafkaConnectBuildContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, new ContainerTemplate())));
        assertThrows(UnsupportedOperationException.class, () -> provider.kafkaConnectBuildContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, new ContainerTemplate())));
        assertThrows(UnsupportedOperationException.class, () -> provider.kafkaConnectBuildContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, new ContainerTemplate())));
    }
    
    @Test
    public void testRestrictedContainerContextWithUserProvidedContext() {
        PodSecurityProvider provider = createProvider();
        provider.configure(ON_KUBERNETES);

        assertThat(provider.zooKeeperContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.zooKeeperContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.zooKeeperContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.zooKeeperContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));

        assertThat(provider.kafkaContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));

        assertThat(provider.kafkaInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));

        assertThat(provider.entityTopicOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.entityTopicOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.entityTopicOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.entityTopicOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));

        assertThat(provider.entityUserOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.entityUserOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.entityUserOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.entityUserOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));

        assertThat(provider.entityOperatorTlsSidecarContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.entityOperatorTlsSidecarContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.entityOperatorTlsSidecarContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.entityOperatorTlsSidecarContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));

        assertThat(provider.kafkaExporterContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaExporterContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaExporterContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaExporterContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));

        assertThat(provider.cruiseControlContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.cruiseControlContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.cruiseControlContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.cruiseControlContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));

        assertThat(provider.jmxTransContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.jmxTransContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.jmxTransContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.jmxTransContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));

        assertThat(provider.kafkaConnectContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaConnectContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaConnectContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaConnectContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));

        assertThat(provider.kafkaConnectInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaConnectInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaConnectInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaConnectInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));

        assertThat(provider.kafkaMirrorMakerContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaMirrorMakerContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaMirrorMakerContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaMirrorMakerContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));

        assertThat(provider.bridgeContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.bridgeContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.bridgeContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.bridgeContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));

        assertThat(provider.kafkaConnectBuildContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaConnectBuildContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaConnectBuildContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaConnectBuildContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
    }
}
