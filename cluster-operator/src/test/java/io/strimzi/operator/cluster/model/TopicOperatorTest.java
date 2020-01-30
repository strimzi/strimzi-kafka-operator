/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.strimzi.api.kafka.model.storage.EphemeralStorage;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.ProbeBuilder;
import io.strimzi.api.kafka.model.storage.SingleVolumeStorage;
import io.strimzi.api.kafka.model.storage.Storage;
import io.strimzi.api.kafka.model.TlsSidecar;
import io.strimzi.api.kafka.model.TlsSidecarBuilder;
import io.strimzi.api.kafka.model.TlsSidecarLogLevel;
import io.strimzi.api.kafka.model.TopicOperatorSpecBuilder;
import io.strimzi.api.kafka.model.TopicOperatorSpec;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class TopicOperatorTest {

    private static final KafkaVersion.Lookup VERSIONS = new KafkaVersion.Lookup(
            new StringReader(KafkaVersionTestUtils.getKafkaVersionYaml()),
            KafkaVersionTestUtils.getKafkaImageMap(),
            emptyMap(),
            emptyMap(),
            emptyMap(),
            emptyMap()) { };

    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 3;
    private final String image = "my-image:latest";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;
    private final int tlsHealthDelay = 120;
    private final int tlsHealthTimeout = 30;
    private final Map<String, Object> metricsCm = singletonMap("animal", "wombat");
    private final Map<String, Object> kafkaConfig = singletonMap("foo", "bar");
    private final Map<String, Object> zooConfig = singletonMap("foo", "bar");
    private final Storage kafkaStorage = new EphemeralStorage();
    private final SingleVolumeStorage zkStorage = new EphemeralStorage();
    private final InlineLogging kafkaLogJson = new InlineLogging();
    private final InlineLogging zooLogJson = new InlineLogging();
    private final InlineLogging topicOperatorLogging = new InlineLogging();

    {
        kafkaLogJson.setLoggers(singletonMap("kafka.root.logger.level", "OFF"));
        zooLogJson.setLoggers(singletonMap("zookeeper.root.logger", "OFF"));
        topicOperatorLogging.setLoggers(Collections.singletonMap("topic-operator.root.logger", "OFF"));
    }

    private final String tcWatchedNamespace = "my-topic-namespace";
    private final String tcImage = "my-topic-operator-image";
    private final int tcReconciliationInterval = 90;
    private final int tcZookeeperSessionTimeout = 20;
    private final int tcTopicMetadataMaxAttempts = 3;

    private final TlsSidecar tlsSidecar = new TlsSidecarBuilder()
            .withLivenessProbe(new ProbeBuilder().withInitialDelaySeconds(tlsHealthDelay).withTimeoutSeconds(tlsHealthTimeout).build())
            .withReadinessProbe(new ProbeBuilder().withInitialDelaySeconds(tlsHealthDelay).withTimeoutSeconds(tlsHealthTimeout).build())
            .build();

    private final TopicOperatorSpec topicOperator = new TopicOperatorSpecBuilder()
            .withWatchedNamespace(tcWatchedNamespace)
            .withImage(tcImage)
            .withReconciliationIntervalSeconds(tcReconciliationInterval)
            .withZookeeperSessionTimeoutSeconds(tcZookeeperSessionTimeout)
            .withTopicMetadataMaxAttempts(tcTopicMetadataMaxAttempts)
            .withLogging(topicOperatorLogging)
            .withTlsSidecar(tlsSidecar)
            .build();

    private final Kafka resource = ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image, healthDelay, healthTimeout, metricsCm, kafkaConfig, zooConfig, kafkaStorage, zkStorage, topicOperator, kafkaLogJson, zooLogJson, null);
    private final TopicOperator tc = TopicOperator.fromCrd(resource, VERSIONS);

    private List<EnvVar> getExpectedEnvVars() {
        List<EnvVar> expected = new ArrayList<>();
        expected.add(new EnvVarBuilder().withName(TopicOperator.ENV_VAR_RESOURCE_LABELS).withValue(TopicOperator.defaultTopicConfigMapLabels(cluster)).build());
        expected.add(new EnvVarBuilder().withName(TopicOperator.ENV_VAR_KAFKA_BOOTSTRAP_SERVERS).withValue(TopicOperator.defaultBootstrapServers(cluster)).build());
        expected.add(new EnvVarBuilder().withName(TopicOperator.ENV_VAR_ZOOKEEPER_CONNECT).withValue(String.format("%s:%d", "localhost", TopicOperatorSpec.DEFAULT_ZOOKEEPER_PORT)).build());
        expected.add(new EnvVarBuilder().withName(TopicOperator.ENV_VAR_WATCHED_NAMESPACE).withValue(tcWatchedNamespace).build());
        expected.add(new EnvVarBuilder().withName(TopicOperator.ENV_VAR_FULL_RECONCILIATION_INTERVAL_MS).withValue(String.valueOf(tcReconciliationInterval * 1000)).build());
        expected.add(new EnvVarBuilder().withName(TopicOperator.ENV_VAR_ZOOKEEPER_SESSION_TIMEOUT_MS).withValue(String.valueOf(tcZookeeperSessionTimeout * 1000)).build());
        expected.add(new EnvVarBuilder().withName(TopicOperator.ENV_VAR_TOPIC_METADATA_MAX_ATTEMPTS).withValue(String.valueOf(tcTopicMetadataMaxAttempts)).build());
        expected.add(new EnvVarBuilder().withName(TopicOperator.ENV_VAR_TLS_ENABLED).withValue(Boolean.toString(true)).build());
        expected.add(new EnvVarBuilder().withName(TopicOperator.ENV_VAR_STRIMZI_GC_LOG_ENABLED).withValue(Boolean.toString(AbstractModel.DEFAULT_JVM_GC_LOGGING_ENABLED)).build());

        return expected;
    }

    @Test
    public void testFromConfigMapNoConfig() {
        Kafka resource = ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image,
                healthDelay, healthTimeout, metricsCm, null, kafkaLogJson, zooLogJson);
        TopicOperator tc = TopicOperator.fromCrd(resource, VERSIONS);
        assertThat(tc, is(nullValue()));
    }

    @Test
    public void testFromConfigMapDefaultConfig() {
        Kafka resource = ResourceUtils.createKafkaCluster(namespace, cluster, replicas, image,
                healthDelay, healthTimeout, metricsCm, kafkaConfig, zooConfig,
                kafkaStorage, zkStorage, new TopicOperatorSpec(), kafkaLogJson, zooLogJson, null);
        TopicOperator tc = TopicOperator.fromCrd(resource, VERSIONS);
        assertThat(tc.getImage(), is("strimzi/operator:latest"));
        assertThat(tc.getWatchedNamespace(), is(namespace));
        assertThat(tc.getReconciliationIntervalMs(), is(TopicOperatorSpec.DEFAULT_FULL_RECONCILIATION_INTERVAL_SECONDS * 1000));
        assertThat(tc.getZookeeperSessionTimeoutMs(), is(TopicOperatorSpec.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_SECONDS * 1000));
        assertThat(tc.getKafkaBootstrapServers(), is(TopicOperator.defaultBootstrapServers(cluster)));
        assertThat(tc.getZookeeperConnect(), is(TopicOperator.defaultZookeeperConnect(cluster)));
        assertThat(tc.getTopicConfigMapLabels(), is(TopicOperator.defaultTopicConfigMapLabels(cluster)));
        assertThat(tc.getTopicMetadataMaxAttempts(), is(TopicOperatorSpec.DEFAULT_TOPIC_METADATA_MAX_ATTEMPTS));
        assertThat(tc.getLogging(), is(nullValue()));
    }

    @Test
    public void testFromConfigMap() {
        assertThat(tc.namespace, is(namespace));
        assertThat(tc.cluster, is(cluster));
        assertThat(tc.image, is(tcImage));
        assertThat(tc.replicas, is(TopicOperatorSpec.DEFAULT_REPLICAS));
        assertThat(tc.readinessProbeOptions.getInitialDelaySeconds(), is(TopicOperatorSpec.DEFAULT_HEALTHCHECK_DELAY));
        assertThat(tc.readinessProbeOptions.getTimeoutSeconds(), is(TopicOperatorSpec.DEFAULT_HEALTHCHECK_TIMEOUT));
        assertThat(tc.livenessProbeOptions.getInitialDelaySeconds(), is(TopicOperatorSpec.DEFAULT_HEALTHCHECK_DELAY));
        assertThat(tc.livenessProbeOptions.getTimeoutSeconds(), is(TopicOperatorSpec.DEFAULT_HEALTHCHECK_TIMEOUT));
        assertThat(tc.getImage(), is(tcImage));
        assertThat(tc.getWatchedNamespace(), is(tcWatchedNamespace));
        assertThat(tc.getReconciliationIntervalMs(), is(tcReconciliationInterval * 1000));
        assertThat(tc.getZookeeperSessionTimeoutMs(), is(tcZookeeperSessionTimeout * 1000));
        assertThat(tc.getKafkaBootstrapServers(), is(TopicOperator.defaultBootstrapServers(cluster)));
        assertThat(tc.getZookeeperConnect(), is(TopicOperator.defaultZookeeperConnect(cluster)));
        assertThat(tc.getTopicConfigMapLabels(), is(TopicOperator.defaultTopicConfigMapLabels(cluster)));
        assertThat(tc.getTopicMetadataMaxAttempts(), is(tcTopicMetadataMaxAttempts));
        assertThat(tc.getLogging().getType(), is(topicOperatorLogging.getType()));
        assertThat(((InlineLogging) tc.getLogging()).getLoggers(), is(topicOperatorLogging.getLoggers()));
    }

    @Test
    public void testGenerateDeployment() {
        Deployment dep = tc.generateDeployment(true, null, null);

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertThat(containers.size(), is(2));
        assertThat(dep.getMetadata().getName(), is(tc.topicOperatorName(cluster)));
        assertThat(dep.getMetadata().getNamespace(), is(namespace));
        assertThat(dep.getSpec().getReplicas(), is(new Integer(TopicOperatorSpec.DEFAULT_REPLICAS)));
        assertThat(containers.get(0).getName(), is(TopicOperator.TOPIC_OPERATOR_NAME));
        assertThat(dep.getMetadata().getOwnerReferences().size(), is(1));
        assertThat(dep.getMetadata().getOwnerReferences().get(0), is(tc.createOwnerReference()));

        // checks on the main Topic Operator container
        assertThat(containers.get(0).getImage(), is(tc.image));
        assertThat(containers.get(0).getEnv(), is(getExpectedEnvVars()));
        assertThat(containers.get(0).getLivenessProbe().getInitialDelaySeconds(), is(new Integer(TopicOperatorSpec.DEFAULT_HEALTHCHECK_DELAY)));
        assertThat(containers.get(0).getLivenessProbe().getTimeoutSeconds(), is(new Integer(TopicOperatorSpec.DEFAULT_HEALTHCHECK_TIMEOUT)));
        assertThat(containers.get(0).getReadinessProbe().getInitialDelaySeconds(), is(new Integer(TopicOperatorSpec.DEFAULT_HEALTHCHECK_DELAY)));
        assertThat(containers.get(0).getReadinessProbe().getTimeoutSeconds(), is(new Integer(TopicOperatorSpec.DEFAULT_HEALTHCHECK_TIMEOUT)));
        assertThat(containers.get(0).getPorts().size(), is(1));
        assertThat(containers.get(0).getPorts().get(0).getContainerPort(), is(new Integer(TopicOperator.HEALTHCHECK_PORT)));
        assertThat(containers.get(0).getPorts().get(0).getName(), is(TopicOperator.HEALTHCHECK_PORT_NAME));
        assertThat(containers.get(0).getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(dep.getSpec().getStrategy().getType(), is("Recreate"));
        assertThat(containers.get(0).getVolumeMounts().get(1).getName(), is(TopicOperator.TLS_SIDECAR_EO_CERTS_VOLUME_NAME));
        assertThat(containers.get(0).getVolumeMounts().get(1).getMountPath(), is(TopicOperator.TLS_SIDECAR_EO_CERTS_VOLUME_MOUNT));
        assertLoggingConfig(dep);
        // checks on the TLS sidecar container
        Container tlsSidecarContainer = containers.get(1);
        assertThat(tlsSidecarContainer.getImage(), is(image));
        assertThat(AbstractModel.containerEnvVars(tlsSidecarContainer).get(TopicOperator.ENV_VAR_ZOOKEEPER_CONNECT), is(TopicOperator.defaultZookeeperConnect(cluster)));
        assertThat(AbstractModel.containerEnvVars(tlsSidecarContainer).get(ModelUtils.TLS_SIDECAR_LOG_LEVEL), is(TlsSidecarLogLevel.NOTICE.toValue()));
        assertThat(tlsSidecarContainer.getVolumeMounts().get(0).getName(), is(TopicOperator.TLS_SIDECAR_EO_CERTS_VOLUME_NAME));
        assertThat(tlsSidecarContainer.getVolumeMounts().get(0).getMountPath(), is(TopicOperator.TLS_SIDECAR_EO_CERTS_VOLUME_MOUNT));
        assertThat(tlsSidecarContainer.getReadinessProbe().getInitialDelaySeconds(), is(new Integer(tlsHealthDelay)));
        assertThat(tlsSidecarContainer.getReadinessProbe().getTimeoutSeconds(), is(new Integer(tlsHealthTimeout)));
        assertThat(tlsSidecarContainer.getLivenessProbe().getInitialDelaySeconds(), is(new Integer(tlsHealthDelay)));
        assertThat(tlsSidecarContainer.getLivenessProbe().getTimeoutSeconds(), is(new Integer(tlsHealthTimeout)));
    }

    @Test
    public void testEnvVars()   {
        assertThat(tc.getEnvVars(), is(getExpectedEnvVars()));
    }

    @Test
    public void withAffinity() throws IOException {
        ResourceTester<Kafka, TopicOperator> helper = new ResourceTester<>(Kafka.class, VERSIONS, TopicOperator::fromCrd, this.getClass().getSimpleName() + ".withAffinity");
        helper.assertDesiredResource("-Deployment.yaml", zc -> zc.generateDeployment(true, null, null).getSpec().getTemplate().getSpec().getAffinity());
    }

    private void assertLoggingConfig(Deployment dep) {
        Volume volume = dep.getSpec().getTemplate().getSpec().getVolumes().get(0);
        VolumeMount volumeMount = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts().get(0);

        assertThat(volumeMount.getMountPath(), is("/opt/topic-operator/custom-config/"));
        assertThat(volumeMount.getName(), is("topic-operator-metrics-and-logging"));
        assertThat(volume.getName(), is("topic-operator-metrics-and-logging"));
        assertThat(volume.getConfigMap().getName(), is("foo-topic-operator-config"));
    }

    @Test
    public void testImagePullPolicy() {
        Deployment dep = tc.generateDeployment(true, ImagePullPolicy.ALWAYS, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(1).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));

        dep = tc.generateDeployment(true, ImagePullPolicy.IFNOTPRESENT, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT.toString()));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(1).getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT.toString()));
    }

    @Test
    public void testRoleBinding()   {
        RoleBinding binding = tc.generateRoleBinding(namespace, tcWatchedNamespace);

        assertThat(binding.getSubjects().get(0).getNamespace(), is(namespace));
        assertThat(binding.getMetadata().getNamespace(), is(tcWatchedNamespace));
    }

    @AfterAll
    public static void cleanUp() {
        ResourceUtils.cleanUpTemporaryTLSFiles();
    }
}
