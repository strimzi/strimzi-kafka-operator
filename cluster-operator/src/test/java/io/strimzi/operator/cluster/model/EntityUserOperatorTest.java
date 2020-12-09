/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.strimzi.api.kafka.model.CertificateAuthority;
import io.strimzi.api.kafka.model.EntityOperatorSpec;
import io.strimzi.api.kafka.model.EntityOperatorSpecBuilder;
import io.strimzi.api.kafka.model.EntityUserOperatorSpec;
import io.strimzi.api.kafka.model.EntityUserOperatorSpecBuilder;
import io.strimzi.api.kafka.model.InlineLogging;
import io.strimzi.api.kafka.model.JmxPrometheusExporterMetrics;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.Probe;
import io.strimzi.api.kafka.model.SystemProperty;
import io.strimzi.api.kafka.model.SystemPropertyBuilder;
import io.strimzi.operator.cluster.ResourceUtils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class EntityUserOperatorTest {

    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 3;
    private final String image = "my-image:latest";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;
    private final InlineLogging userOperatorLogging = new InlineLogging();
    {
        userOperatorLogging.setLoggers(Collections.singletonMap("user-operator.root.logger", "OFF"));
    }
    private final Probe livenessProbe = new Probe();
    {
        livenessProbe.setInitialDelaySeconds(15);
        livenessProbe.setTimeoutSeconds(20);
        livenessProbe.setFailureThreshold(12);
        livenessProbe.setSuccessThreshold(5);
        livenessProbe.setPeriodSeconds(180);
    }

    private final Probe readinessProbe = new Probe();
    {
        readinessProbe.setInitialDelaySeconds(15);
        livenessProbe.setInitialDelaySeconds(20);
        readinessProbe.setFailureThreshold(12);
        readinessProbe.setSuccessThreshold(5);
        readinessProbe.setPeriodSeconds(180);
    }

    private final String uoWatchedNamespace = "my-user-namespace";
    private final String uoImage = "my-user-operator-image";
    private final String secretPrefix = "strimzi-";
    private final int uoReconciliationInterval = 90;
    private final int uoZookeeperSessionTimeout = 20;

    private final String metricsCmJson = "{\"animal\":\"wombat\"}";
    private final String metricsCMName = "metrics-cm";
    private final ConfigMap metricsCM = io.strimzi.operator.cluster.TestUtils.getJmxMetricsCm(metricsCmJson, metricsCMName);
    private final JmxPrometheusExporterMetrics jmxMetricsConfig = io.strimzi.operator.cluster.TestUtils.getJmxPrometheusExporterMetrics(AbstractModel.ANCILLARY_CM_KEY_METRICS, metricsCMName);
    private final List<SystemProperty> javaSystemProperties = new ArrayList<SystemProperty>() {{
            add(new SystemPropertyBuilder().withName("javax.net.debug").withValue("verbose").build());
            add(new SystemPropertyBuilder().withName("something.else").withValue("42").build());
        }};

    private final EntityUserOperatorSpec entityUserOperatorSpec = new EntityUserOperatorSpecBuilder()
            .withWatchedNamespace(uoWatchedNamespace)
            .withImage(uoImage)
            .withReconciliationIntervalSeconds(uoReconciliationInterval)
            .withZookeeperSessionTimeoutSeconds(uoZookeeperSessionTimeout)
            .withSecretPrefix(secretPrefix)
            .withLivenessProbe(livenessProbe)
            .withReadinessProbe(readinessProbe)
            .withLogging(userOperatorLogging)
            .withNewJvmOptions()
                .addAllToJavaSystemProperties(javaSystemProperties)
                .withNewXmx("256m")
            .endJvmOptions()
            .build();

    private final EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder()
            .withUserOperator(entityUserOperatorSpec)
            .build();

    private final Kafka resource =
            new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                    .editSpec()
                    .withEntityOperator(entityOperatorSpec)
                    .endSpec()
                    .build();

    private final EntityUserOperator entityUserOperator = EntityUserOperator.fromCrd(resource);

    private List<EnvVar> getExpectedEnvVars() {
        List<EnvVar> expected = new ArrayList<>();
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_ZOOKEEPER_CONNECT).withValue(String.format("%s:%d", "localhost", EntityUserOperatorSpec.DEFAULT_ZOOKEEPER_PORT)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_KAFKA_BOOTSTRAP_SERVERS).withValue(String.format("%s:%d", "foo-kafka-bootstrap", EntityUserOperatorSpec.DEFAULT_BOOTSTRAP_SERVERS_PORT)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_WATCHED_NAMESPACE).withValue(uoWatchedNamespace).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_RESOURCE_LABELS).withValue(ModelUtils.defaultResourceLabels(cluster)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_FULL_RECONCILIATION_INTERVAL_MS).withValue(String.valueOf(uoReconciliationInterval * 1000)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_ZOOKEEPER_SESSION_TIMEOUT_MS).withValue(String.valueOf(uoZookeeperSessionTimeout * 1000)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_CLIENTS_CA_KEY_SECRET_NAME).withValue(KafkaCluster.clientsCaKeySecretName(cluster)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_CLIENTS_CA_CERT_SECRET_NAME).withValue(KafkaCluster.clientsCaCertSecretName(cluster)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_CLIENTS_CA_NAMESPACE).withValue(namespace).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_CLUSTER_CA_CERT_SECRET_NAME).withValue(KafkaCluster.clusterCaCertSecretName(cluster)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_EO_KEY_SECRET_NAME).withValue(EntityOperator.secretName(cluster)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_STRIMZI_GC_LOG_ENABLED).withValue(Boolean.toString(AbstractModel.DEFAULT_JVM_GC_LOGGING_ENABLED)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_CLIENTS_CA_VALIDITY).withValue(Integer.toString(CertificateAuthority.DEFAULT_CERTS_VALIDITY_DAYS)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_CLIENTS_CA_RENEWAL).withValue(Integer.toString(CertificateAuthority.DEFAULT_CERTS_RENEWAL_DAYS)).build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_STRIMZI_JAVA_OPTS).withValue("-Xmx256m").build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_STRIMZI_JAVA_SYSTEM_PROPERTIES).withValue("-Djavax.net.debug=verbose -Dsomething.else=42").build());
        expected.add(new EnvVarBuilder().withName(EntityUserOperator.ENV_VAR_SECRET_PREFIX).withValue(secretPrefix).build());

        return expected;
    }

    private void checkEnvVars(List<EnvVar> expected, List<EnvVar> actual)   {
        assertThat(actual.size(), is(expected.size()));

        for (EnvVar var : expected) {
            assertThat(actual.contains(var), is(true));
        }
    }

    @Test
    public void testEnvVars()   {
        checkEnvVars(getExpectedEnvVars(), entityUserOperator.getEnvVars());
    }

    @Test
    public void testFromCrd() {
        assertThat(entityUserOperator.namespace, is(namespace));
        assertThat(entityUserOperator.cluster, is(cluster));
        assertThat(entityUserOperator.image, is(uoImage));
        assertThat(entityUserOperator.readinessProbeOptions.getInitialDelaySeconds(), is(readinessProbe.getInitialDelaySeconds()));
        assertThat(entityUserOperator.readinessProbeOptions.getTimeoutSeconds(), is(readinessProbe.getTimeoutSeconds()));
        assertThat(entityUserOperator.readinessProbeOptions.getSuccessThreshold(), is(readinessProbe.getSuccessThreshold()));
        assertThat(entityUserOperator.readinessProbeOptions.getFailureThreshold(), is(readinessProbe.getFailureThreshold()));
        assertThat(entityUserOperator.readinessProbeOptions.getPeriodSeconds(), is(readinessProbe.getPeriodSeconds()));
        assertThat(entityUserOperator.livenessProbeOptions.getInitialDelaySeconds(), is(livenessProbe.getInitialDelaySeconds()));
        assertThat(entityUserOperator.livenessProbeOptions.getTimeoutSeconds(), is(livenessProbe.getTimeoutSeconds()));
        assertThat(entityUserOperator.livenessProbeOptions.getSuccessThreshold(), is(livenessProbe.getSuccessThreshold()));
        assertThat(entityUserOperator.livenessProbeOptions.getFailureThreshold(), is(livenessProbe.getFailureThreshold()));
        assertThat(entityUserOperator.livenessProbeOptions.getPeriodSeconds(), is(livenessProbe.getPeriodSeconds()));
        assertThat(entityUserOperator.getWatchedNamespace(), is(uoWatchedNamespace));
        assertThat(entityUserOperator.getReconciliationIntervalMs(), is(uoReconciliationInterval * 1000L));
        assertThat(entityUserOperator.getZookeeperSessionTimeoutMs(), is(uoZookeeperSessionTimeout * 1000L));
        assertThat(entityUserOperator.getZookeeperConnect(), is(EntityUserOperator.defaultZookeeperConnect(cluster)));
        assertThat(entityUserOperator.getKafkaBootstrapServers(), is(String.format("%s:%d", KafkaCluster.serviceName(cluster), EntityUserOperatorSpec.DEFAULT_BOOTSTRAP_SERVERS_PORT)));
        assertThat(entityUserOperator.getLogging().getType(), is(userOperatorLogging.getType()));
        assertThat(((InlineLogging) entityUserOperator.getLogging()).getLoggers(), is(userOperatorLogging.getLoggers()));
        assertThat(entityUserOperator.getSecretPrefix(), is(secretPrefix));
    }

    @Test
    public void testFromCrdDefault() {
        EntityUserOperatorSpec entityUserOperatorSpec = new EntityUserOperatorSpecBuilder()
                .build();
        EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder()
                .withUserOperator(entityUserOperatorSpec)
                .build();
        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                        .withEntityOperator(entityOperatorSpec)
                        .endSpec()
                        .build();
        EntityUserOperator entityUserOperator = EntityUserOperator.fromCrd(resource);

        assertThat(entityUserOperator.getWatchedNamespace(), is(namespace));
        assertThat(entityUserOperator.getImage(), is("strimzi/operator:latest"));
        assertThat(entityUserOperator.getReconciliationIntervalMs(), is(EntityUserOperatorSpec.DEFAULT_FULL_RECONCILIATION_INTERVAL_SECONDS * 1000));
        assertThat(entityUserOperator.getZookeeperSessionTimeoutMs(), is(EntityUserOperatorSpec.DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_SECONDS * 1000));
        assertThat(entityUserOperator.readinessProbeOptions.getInitialDelaySeconds(), is(EntityUserOperatorSpec.DEFAULT_HEALTHCHECK_DELAY));
        assertThat(entityUserOperator.readinessProbeOptions.getTimeoutSeconds(), is(EntityUserOperatorSpec.DEFAULT_HEALTHCHECK_TIMEOUT));
        assertThat(entityUserOperator.livenessProbeOptions.getInitialDelaySeconds(), is(EntityUserOperatorSpec.DEFAULT_HEALTHCHECK_DELAY));
        assertThat(entityUserOperator.livenessProbeOptions.getTimeoutSeconds(), is(EntityUserOperatorSpec.DEFAULT_HEALTHCHECK_TIMEOUT));
        assertThat(entityUserOperator.getZookeeperConnect(), is(EntityUserOperator.defaultZookeeperConnect(cluster)));
        assertThat(entityUserOperator.getKafkaBootstrapServers(), is(EntityUserOperator.defaultBootstrapServers(cluster)));
        assertThat(entityUserOperator.getLogging(), is(nullValue()));
        assertThat(entityUserOperator.getSecretPrefix(), is(EntityUserOperatorSpec.DEFAULT_SECRET_PREFIX));
    }

    @Test
    public void testFromCrdNoEntityOperator() {
        Kafka resource = ResourceUtils.createKafka(namespace, cluster, replicas, image,
                healthDelay, healthTimeout);
        EntityUserOperator entityUserOperator = EntityUserOperator.fromCrd(resource);
        assertThat(entityUserOperator, is(nullValue()));
    }

    @Test
    public void testFromCrdNoUserOperatorInEntityOperator() {
        EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder().build();
        Kafka resource =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                        .withEntityOperator(entityOperatorSpec)
                        .endSpec()
                        .build();
        EntityUserOperator entityUserOperator = EntityUserOperator.fromCrd(resource);
        assertThat(entityUserOperator, is(nullValue()));
    }

    @Test
    public void testGetContainers() {
        List<Container> containers = entityUserOperator.getContainers(null);
        assertThat(containers.size(), is(1));

        Container container = containers.get(0);
        assertThat(container.getName(), is(EntityUserOperator.USER_OPERATOR_CONTAINER_NAME));
        assertThat(container.getImage(), is(entityUserOperator.getImage()));
        checkEnvVars(getExpectedEnvVars(), container.getEnv());
        assertThat(container.getLivenessProbe().getInitialDelaySeconds(), is(Integer.valueOf(livenessProbe.getInitialDelaySeconds())));
        assertThat(container.getLivenessProbe().getTimeoutSeconds(), is(Integer.valueOf(livenessProbe.getTimeoutSeconds())));
        assertThat(container.getReadinessProbe().getInitialDelaySeconds(), is(Integer.valueOf(readinessProbe.getInitialDelaySeconds())));
        assertThat(container.getReadinessProbe().getTimeoutSeconds(), is(Integer.valueOf(readinessProbe.getTimeoutSeconds())));
        assertThat(container.getPorts().size(), is(1));
        assertThat(container.getPorts().get(0).getContainerPort(), is(Integer.valueOf(EntityUserOperator.HEALTHCHECK_PORT)));
        assertThat(container.getPorts().get(0).getName(), is(EntityUserOperator.HEALTHCHECK_PORT_NAME));
        assertThat(container.getPorts().get(0).getProtocol(), is("TCP"));
        assertThat(container.getVolumeMounts().get(0).getMountPath(), is("/opt/user-operator/custom-config/"));
        assertThat(container.getVolumeMounts().get(0).getName(), is("entity-user-operator-metrics-and-logging"));
    }

    @Test
    public void testFromCrdCaValidityAndRenewal() {
        EntityUserOperatorSpec entityUserOperatorSpec = new EntityUserOperatorSpecBuilder()
                .build();
        EntityOperatorSpec entityOperatorSpec = new EntityOperatorSpecBuilder()
                .withUserOperator(entityUserOperatorSpec)
                .build();
        CertificateAuthority ca = new CertificateAuthority();
        ca.setValidityDays(42);
        ca.setRenewalDays(69);
        Kafka customValues =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                        .withEntityOperator(entityOperatorSpec)
                        .withClientsCa(ca)
                        .endSpec()
                        .build();
        EntityUserOperator entityUserOperator = EntityUserOperator.fromCrd(customValues);

        Kafka defaultValues =
                new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                        .editSpec()
                        .withEntityOperator(entityOperatorSpec)
                        .endSpec()
                        .build();
        EntityUserOperator entityUserOperator2 = EntityUserOperator.fromCrd(defaultValues);

        assertThat(entityUserOperator.getClientsCaValidityDays(), is(42L));
        assertThat(entityUserOperator.getClientsCaRenewalDays(), is(69L));
        assertThat(entityUserOperator2.getClientsCaValidityDays(), is(Long.valueOf(CertificateAuthority.DEFAULT_CERTS_VALIDITY_DAYS)));
        assertThat(entityUserOperator2.getClientsCaRenewalDays(), is(Long.valueOf(CertificateAuthority.DEFAULT_CERTS_RENEWAL_DAYS)));
    }

    @Test
    public void testEntityUserOperatorEnvVarValidityAndRenewal() {
        int validity = 100;
        int renewal = 42;

        Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas,
                image, healthDelay, healthTimeout, singletonMap("animal", "wombat"), jmxMetricsConfig, singletonMap("foo", "bar"), emptyMap()))
                .editSpec()
                .withNewClientsCa()
                .withRenewalDays(renewal)
                .withValidityDays(validity)
                .endClientsCa()
                .withNewEntityOperator()
                .withNewUserOperator()
                .endUserOperator()
                .endEntityOperator()
                .endSpec()
                .build();

        EntityUserOperator f = EntityUserOperator.fromCrd(kafkaAssembly);
        List<EnvVar> envvar = f.getEnvVars();
        assertThat(Integer.parseInt(envvar.stream().filter(a -> a.getName().equals(EntityUserOperator.ENV_VAR_CLIENTS_CA_VALIDITY)).findFirst().get().getValue()), is(validity));
        assertThat(Integer.parseInt(envvar.stream().filter(a -> a.getName().equals(EntityUserOperator.ENV_VAR_CLIENTS_CA_RENEWAL)).findFirst().get().getValue()), is(renewal));
    }

    @Test
    public void testRoleBindingInOtherNamespace()   {
        RoleBinding binding = entityUserOperator.generateRoleBinding(namespace, uoWatchedNamespace);

        assertThat(binding.getSubjects().get(0).getNamespace(), is(namespace));
        assertThat(binding.getMetadata().getNamespace(), is(uoWatchedNamespace));
        assertThat(binding.getMetadata().getOwnerReferences().size(), is(0));
    }

    @Test
    public void testRoleBindingInTheSameNamespace()   {
        RoleBinding binding = entityUserOperator.generateRoleBinding(namespace, namespace);

        assertThat(binding.getSubjects().get(0).getNamespace(), is(namespace));
        assertThat(binding.getMetadata().getNamespace(), is(namespace));
        assertThat(binding.getMetadata().getOwnerReferences().size(), is(1));
    }
}
