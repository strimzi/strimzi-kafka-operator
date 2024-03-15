/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model.securityprofiles;

import io.fabric8.kubernetes.api.model.PodSecurityContext;
import io.fabric8.kubernetes.api.model.PodSecurityContextBuilder;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.strimzi.api.kafka.model.common.template.ContainerTemplate;
import io.strimzi.api.kafka.model.common.template.ContainerTemplateBuilder;
import io.strimzi.api.kafka.model.common.template.PodTemplate;
import io.strimzi.api.kafka.model.common.template.PodTemplateBuilder;
import io.strimzi.api.kafka.model.kafka.EphemeralStorage;
import io.strimzi.api.kafka.model.kafka.JbodStorageBuilder;
import io.strimzi.api.kafka.model.kafka.PersistentClaimStorageBuilder;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.platform.PlatformFeatures;
import io.strimzi.plugin.security.profiles.PodSecurityProvider;
import io.strimzi.plugin.security.profiles.impl.BaselinePodSecurityProvider;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;

public class BaselinePodSecurityProviderTest {
    protected final static PlatformFeatures ON_OPENSHIFT = new PlatformFeaturesAvailability(true, KubernetesVersion.MINIMAL_SUPPORTED_VERSION);
    protected final static PlatformFeatures ON_KUBERNETES = new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION);

    protected final static Storage EPHEMERAL = new EphemeralStorage();
    protected final static Storage PERSISTENT = new PersistentClaimStorageBuilder().withSize("100Gi").build();
    protected final static Storage JBOD = new JbodStorageBuilder().withVolumes(new PersistentClaimStorageBuilder().withSize("100Gi").build()).build();

    protected final static PodSecurityContext DEFAULT_KUBE_POD_SECURITY_CONTEXT = new PodSecurityContextBuilder()
            .withFsGroup(0L)
            .build();

    protected final static PodTemplate CUSTOM_POD_SECURITY_CONTEXT = new PodTemplateBuilder()
            .withSecurityContext(new PodSecurityContextBuilder()
                    .withRunAsUser(1874L)
                    .build())
            .build();

    protected final static ContainerTemplate CUSTOM_CONTAINER_SECURITY_CONTEXT = new ContainerTemplateBuilder()
            .withSecurityContext(new SecurityContextBuilder()
                    .withPrivileged(true)
                    .build())
            .build();
    
    /**
     * Used to override the tested provider in extending classes
     *
     * @return  Created PodSecurityProvider
     */
    protected PodSecurityProvider createProvider()    {
        return new BaselinePodSecurityProvider();
    }

    @Test
    public void testPodContextOnOpenShift() {
        PodSecurityProvider provider = createProvider();
        provider.configure(ON_OPENSHIFT);

        assertThat(provider.zooKeeperPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.zooKeeperPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.zooKeeperPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.kafkaPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.entityOperatorPodSecurityContext(new PodSecurityProviderContextImpl(null, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.entityOperatorPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.entityOperatorPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.entityOperatorPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.kafkaExporterPodSecurityContext(new PodSecurityProviderContextImpl(null, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaExporterPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaExporterPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaExporterPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.cruiseControlPodSecurityContext(new PodSecurityProviderContextImpl(null, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.cruiseControlPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.cruiseControlPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.cruiseControlPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.kafkaConnectPodSecurityContext(new PodSecurityProviderContextImpl(null, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.kafkaConnectBuildPodSecurityContext(new PodSecurityProviderContextImpl(null, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectBuildPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectBuildPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectBuildPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.kafkaMirrorMakerPodSecurityContext(new PodSecurityProviderContextImpl(null, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaMirrorMakerPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaMirrorMakerPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaMirrorMakerPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.bridgePodSecurityContext(new PodSecurityProviderContextImpl(null, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.bridgePodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.bridgePodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.bridgePodSecurityContext(new PodSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(CoreMatchers.nullValue()));
    }

    @Test
    public void testPodContextWithUserProvidedContextOnOpenShift() {
        PodSecurityProvider provider = createProvider();
        provider.configure(ON_OPENSHIFT);

        assertThat(provider.zooKeeperPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.zooKeeperPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.zooKeeperPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));

        assertThat(provider.kafkaPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));

        assertThat(provider.entityOperatorPodSecurityContext(new PodSecurityProviderContextImpl(null, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.entityOperatorPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.entityOperatorPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.entityOperatorPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));

        assertThat(provider.kafkaExporterPodSecurityContext(new PodSecurityProviderContextImpl(null, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaExporterPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaExporterPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaExporterPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));

        assertThat(provider.cruiseControlPodSecurityContext(new PodSecurityProviderContextImpl(null, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.cruiseControlPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.cruiseControlPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.cruiseControlPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));

        assertThat(provider.kafkaConnectPodSecurityContext(new PodSecurityProviderContextImpl(null, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaConnectPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaConnectPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaConnectPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));

        assertThat(provider.kafkaConnectBuildPodSecurityContext(new PodSecurityProviderContextImpl(null, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaConnectBuildPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaConnectBuildPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaConnectBuildPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));

        assertThat(provider.kafkaMirrorMakerPodSecurityContext(new PodSecurityProviderContextImpl(null, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaMirrorMakerPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaMirrorMakerPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaMirrorMakerPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));

        assertThat(provider.bridgePodSecurityContext(new PodSecurityProviderContextImpl(null, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.bridgePodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.bridgePodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.bridgePodSecurityContext(new PodSecurityProviderContextImpl(JBOD, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
    }

    @Test
    public void testPodContextOnKubernetes() {
        PodSecurityProvider provider = createProvider();
        provider.configure(ON_KUBERNETES);

        assertThat(provider.zooKeeperPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.zooKeeperPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(DEFAULT_KUBE_POD_SECURITY_CONTEXT));
        assertThat(provider.zooKeeperPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(DEFAULT_KUBE_POD_SECURITY_CONTEXT));

        assertThat(provider.kafkaPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(DEFAULT_KUBE_POD_SECURITY_CONTEXT));
        assertThat(provider.kafkaPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(DEFAULT_KUBE_POD_SECURITY_CONTEXT));

        assertThat(provider.entityOperatorPodSecurityContext(new PodSecurityProviderContextImpl(null, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.entityOperatorPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.entityOperatorPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.entityOperatorPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.kafkaExporterPodSecurityContext(new PodSecurityProviderContextImpl(null, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaExporterPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaExporterPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaExporterPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.cruiseControlPodSecurityContext(new PodSecurityProviderContextImpl(null, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.cruiseControlPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.cruiseControlPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.cruiseControlPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.kafkaConnectPodSecurityContext(new PodSecurityProviderContextImpl(null, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.kafkaConnectBuildPodSecurityContext(new PodSecurityProviderContextImpl(null, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectBuildPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectBuildPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectBuildPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.kafkaMirrorMakerPodSecurityContext(new PodSecurityProviderContextImpl(null, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaMirrorMakerPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaMirrorMakerPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaMirrorMakerPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.bridgePodSecurityContext(new PodSecurityProviderContextImpl(null, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.bridgePodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.bridgePodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.bridgePodSecurityContext(new PodSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(CoreMatchers.nullValue()));
    }

    @Test
    public void testPodContextOnKubernetesWithEmptyTemplate() {
        PodSecurityProvider provider = createProvider();
        provider.configure(ON_KUBERNETES);

        assertThat(provider.zooKeeperPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, new PodTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.zooKeeperPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, new PodTemplate())), CoreMatchers.is(DEFAULT_KUBE_POD_SECURITY_CONTEXT));
        assertThat(provider.zooKeeperPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, new PodTemplate())), CoreMatchers.is(DEFAULT_KUBE_POD_SECURITY_CONTEXT));

        assertThat(provider.kafkaPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, new PodTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, new PodTemplate())), CoreMatchers.is(DEFAULT_KUBE_POD_SECURITY_CONTEXT));
        assertThat(provider.kafkaPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, new PodTemplate())), CoreMatchers.is(DEFAULT_KUBE_POD_SECURITY_CONTEXT));

        assertThat(provider.entityOperatorPodSecurityContext(new PodSecurityProviderContextImpl(null, new PodTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.entityOperatorPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, new PodTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.entityOperatorPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, new PodTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.entityOperatorPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, new PodTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.kafkaExporterPodSecurityContext(new PodSecurityProviderContextImpl(null, new PodTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaExporterPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, new PodTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaExporterPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, new PodTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaExporterPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, new PodTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.cruiseControlPodSecurityContext(new PodSecurityProviderContextImpl(null, new PodTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.cruiseControlPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, new PodTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.cruiseControlPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, new PodTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.cruiseControlPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, new PodTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.kafkaConnectPodSecurityContext(new PodSecurityProviderContextImpl(null, new PodTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, new PodTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, new PodTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, new PodTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.kafkaConnectBuildPodSecurityContext(new PodSecurityProviderContextImpl(null, new PodTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectBuildPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, new PodTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectBuildPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, new PodTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectBuildPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, new PodTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.kafkaMirrorMakerPodSecurityContext(new PodSecurityProviderContextImpl(null, new PodTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaMirrorMakerPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, new PodTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaMirrorMakerPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, new PodTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaMirrorMakerPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, new PodTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.bridgePodSecurityContext(new PodSecurityProviderContextImpl(null, new PodTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.bridgePodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, new PodTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.bridgePodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, new PodTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.bridgePodSecurityContext(new PodSecurityProviderContextImpl(JBOD, new PodTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
    }

    @Test
    public void testPodContextWithUserProvidedContextOnKubernetes() {
        PodSecurityProvider provider = createProvider();
        provider.configure(ON_KUBERNETES);

        assertThat(provider.zooKeeperPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.zooKeeperPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.zooKeeperPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));

        assertThat(provider.kafkaPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));

        assertThat(provider.entityOperatorPodSecurityContext(new PodSecurityProviderContextImpl(null, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.entityOperatorPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.entityOperatorPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.entityOperatorPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));

        assertThat(provider.kafkaExporterPodSecurityContext(new PodSecurityProviderContextImpl(null, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaExporterPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaExporterPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaExporterPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));

        assertThat(provider.cruiseControlPodSecurityContext(new PodSecurityProviderContextImpl(null, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.cruiseControlPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.cruiseControlPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.cruiseControlPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));

        assertThat(provider.kafkaConnectPodSecurityContext(new PodSecurityProviderContextImpl(null, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaConnectPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaConnectPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaConnectPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));

        assertThat(provider.kafkaConnectBuildPodSecurityContext(new PodSecurityProviderContextImpl(null, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaConnectBuildPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaConnectBuildPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaConnectBuildPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));

        assertThat(provider.kafkaMirrorMakerPodSecurityContext(new PodSecurityProviderContextImpl(null, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaMirrorMakerPodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaMirrorMakerPodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaMirrorMakerPodSecurityContext(new PodSecurityProviderContextImpl(JBOD, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));

        assertThat(provider.bridgePodSecurityContext(new PodSecurityProviderContextImpl(null, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.bridgePodSecurityContext(new PodSecurityProviderContextImpl(EPHEMERAL, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.bridgePodSecurityContext(new PodSecurityProviderContextImpl(PERSISTENT, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.bridgePodSecurityContext(new PodSecurityProviderContextImpl(JBOD, CUSTOM_POD_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_POD_SECURITY_CONTEXT.getSecurityContext()));
    }

    @Test
    public void testContainerContext() {
        PodSecurityProvider provider = createProvider();
        provider.configure(ON_KUBERNETES);

        assertThat(provider.zooKeeperContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.zooKeeperContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.zooKeeperContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.zooKeeperContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.kafkaContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.kafkaInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.entityTopicOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.entityTopicOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.entityTopicOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.entityTopicOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.entityUserOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.entityUserOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.entityUserOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.entityUserOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.kafkaExporterContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaExporterContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaExporterContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaExporterContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.cruiseControlContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.cruiseControlContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.cruiseControlContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.cruiseControlContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.kafkaConnectContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.kafkaConnectInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.kafkaMirrorMakerContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaMirrorMakerContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaMirrorMakerContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaMirrorMakerContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.bridgeContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.bridgeContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.bridgeContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.bridgeContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.kafkaConnectBuildContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectBuildContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectBuildContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, null)), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectBuildContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, null)), CoreMatchers.is(CoreMatchers.nullValue()));
    }

    @Test
    public void testContainerContextWithEmptyTemplate() {
        PodSecurityProvider provider = createProvider();
        provider.configure(ON_KUBERNETES);

        assertThat(provider.zooKeeperContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.zooKeeperContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.zooKeeperContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.zooKeeperContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.kafkaContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.kafkaInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.entityTopicOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.entityTopicOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.entityTopicOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.entityTopicOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.entityUserOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.entityUserOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.entityUserOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.entityUserOperatorContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.kafkaExporterContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaExporterContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaExporterContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaExporterContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.cruiseControlContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.cruiseControlContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.cruiseControlContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.cruiseControlContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.kafkaConnectContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.kafkaConnectInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectInitContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.kafkaMirrorMakerContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaMirrorMakerContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaMirrorMakerContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaMirrorMakerContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.bridgeContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.bridgeContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.bridgeContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.bridgeContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));

        assertThat(provider.kafkaConnectBuildContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectBuildContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectBuildContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
        assertThat(provider.kafkaConnectBuildContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, new ContainerTemplate())), CoreMatchers.is(CoreMatchers.nullValue()));
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

        assertThat(provider.kafkaExporterContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaExporterContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaExporterContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.kafkaExporterContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));

        assertThat(provider.cruiseControlContainerSecurityContext(new ContainerSecurityProviderContextImpl(null, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.cruiseControlContainerSecurityContext(new ContainerSecurityProviderContextImpl(EPHEMERAL, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.cruiseControlContainerSecurityContext(new ContainerSecurityProviderContextImpl(PERSISTENT, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));
        assertThat(provider.cruiseControlContainerSecurityContext(new ContainerSecurityProviderContextImpl(JBOD, CUSTOM_CONTAINER_SECURITY_CONTEXT)), CoreMatchers.is(CUSTOM_CONTAINER_SECURITY_CONTEXT.getSecurityContext()));

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
