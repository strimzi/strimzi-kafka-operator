/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Affinity;
import io.fabric8.kubernetes.api.model.AffinityBuilder;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.NodeSelectorTermBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.SecurityContextBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.strimzi.api.kafka.model.ContainerEnvVar;
import io.strimzi.api.kafka.model.JmxTransResources;
import io.strimzi.api.kafka.model.JmxTransSpec;
import io.strimzi.api.kafka.model.JmxTransSpecBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaJmxAuthenticationPasswordBuilder;
import io.strimzi.api.kafka.model.KafkaJmxOptionsBuilder;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.template.ContainerTemplate;
import io.strimzi.api.kafka.model.template.JmxTransOutputDefinitionTemplateBuilder;
import io.strimzi.api.kafka.model.template.JmxTransQueryTemplateBuilder;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.components.JmxTransOutputWriter;
import io.strimzi.operator.cluster.model.components.JmxTransQueries;
import io.strimzi.operator.cluster.model.components.JmxTransServer;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.plugin.security.profiles.impl.RestrictedPodSecurityProvider;
import io.strimzi.test.TestUtils;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;
import io.vertx.core.json.JsonObject;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.strimzi.operator.cluster.CustomMatchers.hasEntries;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.hasProperty;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ParallelSuite
public class JmxTransTest {
    private final String namespace = "test";
    private final String cluster = "foo";
    private final int replicas = 3;
    private final String image = "image";
    private final int healthDelay = 120;
    private final int healthTimeout = 30;

    private final JmxTransSpec jmxTransSpec = new JmxTransSpecBuilder()
            .withImage(image)
            .withOutputDefinitions(new JmxTransOutputDefinitionTemplateBuilder()
                    .withName("standardOut")
                    .withOutputType("com.googlecode.jmxtrans.model.output.StdOutWriter")
                    .build())
            .withKafkaQueries(new JmxTransQueryTemplateBuilder()
                    .withOutputs("standardOut")
                    .withAttributes("Count")
                    .withTargetMBean("kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=*")
                    .build())
            .build();

    private final Kafka kafkaAssembly = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
            .editSpec()
                .withJmxTrans(jmxTransSpec)
                .editKafka()
                    .withJmxOptions(new KafkaJmxOptionsBuilder().withAuthentication(new KafkaJmxAuthenticationPasswordBuilder().build()).build())
                .endKafka()
            .endSpec()
            .build();

    private final JmxTrans jmxTrans = JmxTrans.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly);

    @ParallelTest
    public void testOutputDefinitionWriterDeserialization() {
        JmxTransOutputWriter outputWriter = new JmxTransOutputWriter();

        outputWriter.setAtClass("class");
        outputWriter.setHost("host");
        outputWriter.setPort(9999);
        outputWriter.setFlushDelayInSeconds(1);
        outputWriter.setTypeNames(Collections.singletonList("SingleType"));

        JsonObject targetJson = JsonObject.mapFrom(outputWriter);

        assertThat(targetJson.getString("host"), is("host"));
        assertThat(targetJson.getString("@class"), is("class"));
        assertThat(targetJson.getInteger("port"), is(9999));
        assertThat(targetJson.getInteger("flushDelayInSeconds"), is(1));
        assertThat(targetJson.getJsonArray("typeNames").size(), is(1));
        assertThat(targetJson.getJsonArray("typeNames").getList().get(0), is("SingleType"));

    }

    @ParallelTest
    public void testServersDeserialization() {
        JmxTransServer server = new JmxTransServer();

        server.setHost("host");
        server.setPort(9999);
        server.setUsername("username");
        server.setPassword("password");
        server.setQueries(Collections.emptyList());

        JsonObject targetJson = JsonObject.mapFrom(server);

        assertThat(targetJson.getString("host"), is("host"));
        assertThat(targetJson.getInteger("port"), is(9999));
        assertThat(targetJson.getString("username"), is("username"));
        assertThat(targetJson.getString("password"), is("password"));
        assertThat(targetJson.getJsonArray("queries").getList().size(), is(0));
    }

    @ParallelTest
    public void testQueriesDeserialization() {
        JmxTransOutputWriter outputWriter = new JmxTransOutputWriter();

        outputWriter.setAtClass("class");
        outputWriter.setHost("host");
        outputWriter.setPort(9999);
        outputWriter.setFlushDelayInSeconds(1);
        outputWriter.setTypeNames(Collections.singletonList("SingleType"));

        JmxTransQueries queries = new JmxTransQueries();

        queries.setObj("object");
        queries.setAttr(Collections.singletonList("attribute"));

        queries.setOutputWriters(Collections.singletonList(outputWriter));

        JsonObject targetJson = JsonObject.mapFrom(queries);
        JsonObject outputWriterJson = targetJson.getJsonArray("outputWriters").getJsonObject(0);

        assertThat(targetJson.getString("obj"), is("object"));
        assertThat(targetJson.getJsonArray("attr").size(), is(1));
        assertThat(targetJson.getJsonArray("attr").getString(0), is("attribute"));

        assertThat(outputWriterJson.getString("host"), is("host"));
        assertThat(outputWriterJson.getString("@class"), is("class"));
        assertThat(outputWriterJson.getInteger("port"), is(9999));
        assertThat(outputWriterJson.getInteger("flushDelayInSeconds"), is(1));
        assertThat(outputWriterJson.getJsonArray("typeNames").size(), is(1));
        assertThat(outputWriterJson.getJsonArray("typeNames").getList().get(0), is("SingleType"));
    }

    private List<EnvVar> getExpectedEnvVars() {
        List<EnvVar> expected = new ArrayList<>();
        expected.add(new EnvVarBuilder().withName(KafkaCluster.ENV_VAR_KAFKA_JMX_USERNAME).withNewValueFrom().withNewSecretKeyRef().withName(KafkaResources.kafkaJmxSecretName(cluster)).withKey(KafkaCluster.SECRET_JMX_USERNAME_KEY).endSecretKeyRef().endValueFrom().build());
        expected.add(new EnvVarBuilder().withName(KafkaCluster.ENV_VAR_KAFKA_JMX_PASSWORD).withNewValueFrom().withNewSecretKeyRef().withName(KafkaResources.kafkaJmxSecretName(cluster)).withKey(KafkaCluster.SECRET_JMX_PASSWORD_KEY).endSecretKeyRef().endValueFrom().build());
        expected.add(new EnvVarBuilder().withName(JmxTrans.ENV_VAR_JMXTRANS_LOGGING_LEVEL).withValue("INFO").build());
        return expected;
    }

    @ParallelTest
    public void testGenerateDeployment() {
        Deployment dep = jmxTrans.generateDeployment(null, null);

        List<Container> containers = dep.getSpec().getTemplate().getSpec().getContainers();

        assertThat(containers.size(), is(1));

        assertThat(dep.getMetadata().getName(), is(JmxTransResources.deploymentName(cluster)));
        assertThat(dep.getMetadata().getNamespace(), is(namespace));
        TestUtils.checkOwnerReference(dep, kafkaAssembly);

        // checks on the main Exporter container
        assertThat(containers.get(0).getImage(), is(image));
        assertThat(containers.get(0).getEnv(), is(getExpectedEnvVars()));
        assertThat(containers.get(0).getPorts(), is(Matchers.nullValue()));
        assertThat(dep.getSpec().getStrategy().getType(), is("RollingUpdate"));
        assertThat(dep.getSpec().getStrategy().getRollingUpdate().getMaxSurge(), is(new IntOrString(1)));
        assertThat(dep.getSpec().getStrategy().getRollingUpdate().getMaxUnavailable(), is(new IntOrString(0)));

        // Test healthchecks
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getLivenessProbe(), is(nullValue()));

        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getExec().getCommand(), is(List.of("/opt/jmx/jmxtrans_readiness_check.sh", KafkaResources.brokersServiceName(cluster), "9999")));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getInitialDelaySeconds(), is(JmxTransSpec.DEFAULT_HEALTHCHECK_DELAY));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getReadinessProbe().getTimeoutSeconds(), is(JmxTransSpec.DEFAULT_HEALTHCHECK_TIMEOUT));

        // Test volumes
        List<Volume> volumes = dep.getSpec().getTemplate().getSpec().getVolumes();
        assertThat(volumes.size(), is(2));

        Volume volume = volumes.stream().filter(vol -> VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volume, is(notNullValue()));
        assertThat(volume.getEmptyDir().getMedium(), is("Memory"));
        assertThat(volume.getEmptyDir().getSizeLimit(), is(new Quantity("5Mi")));

        volume = volumes.stream().filter(vol -> JmxTrans.JMXTRANS_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volume, is(notNullValue()));
        assertThat(volume.getConfigMap().getName(), is(JmxTransResources.configMapName(cluster)));

        // Test volume mounts
        List<VolumeMount> volumesMounts = dep.getSpec().getTemplate().getSpec().getContainers().get(0).getVolumeMounts();
        assertThat(volumesMounts.size(), is(2));

        VolumeMount volumeMount = volumesMounts.stream().filter(vol -> VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(VolumeUtils.STRIMZI_TMP_DIRECTORY_DEFAULT_MOUNT_PATH));

        volumeMount = volumesMounts.stream().filter(vol -> JmxTrans.JMXTRANS_VOLUME_NAME.equals(vol.getName())).findFirst().orElseThrow();
        assertThat(volumeMount, is(notNullValue()));
        assertThat(volumeMount.getMountPath(), is(JmxTrans.JMX_FILE_PATH));
    }

    @ParallelTest
    public void testImagePullPolicy() {
        Deployment dep = jmxTrans.generateDeployment(ImagePullPolicy.ALWAYS, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.ALWAYS.toString()));

        dep = jmxTrans.generateDeployment(ImagePullPolicy.IFNOTPRESENT, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getImagePullPolicy(), is(ImagePullPolicy.IFNOTPRESENT.toString()));
    }

    @ParallelTest
    public void testConfigMap() {
        ConfigMap cm = jmxTrans.generateConfigMap();

        assertThat(cm.getMetadata().getName(), is(JmxTransResources.configMapName(cluster)));
        assertThat(cm.getMetadata().getNamespace(), is(namespace));
        TestUtils.checkOwnerReference(cm, kafkaAssembly);

        assertThat(cm.getData().getOrDefault(JmxTrans.JMXTRANS_CONFIGMAP_KEY, ""), is("{\"servers\":[{\"host\":\"foo-kafka-0.foo-kafka-brokers\",\"port\":9999,\"username\":\"${kafka.username}\",\"password\":\"${kafka.password}\",\"queries\":[{\"obj\":\"kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=*\",\"attr\":[\"Count\"],\"outputWriters\":[{\"@class\":\"com.googlecode.jmxtrans.model.output.StdOutWriter\"}]}]},{\"host\":\"foo-kafka-1.foo-kafka-brokers\",\"port\":9999,\"username\":\"${kafka.username}\",\"password\":\"${kafka.password}\",\"queries\":[{\"obj\":\"kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=*\",\"attr\":[\"Count\"],\"outputWriters\":[{\"@class\":\"com.googlecode.jmxtrans.model.output.StdOutWriter\"}]}]},{\"host\":\"foo-kafka-2.foo-kafka-brokers\",\"port\":9999,\"username\":\"${kafka.username}\",\"password\":\"${kafka.password}\",\"queries\":[{\"obj\":\"kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec,topic=*\",\"attr\":[\"Count\"],\"outputWriters\":[{\"@class\":\"com.googlecode.jmxtrans.model.output.StdOutWriter\"}]}]}]}"));
    }

    @ParallelTest
    public void testNotDeployed() {
        Kafka resource = ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout);

        JmxTrans jmxTrans = JmxTrans.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource);

        assertThat(jmxTrans, is(nullValue()));
    }

    @ParallelTest
    public void testWithoutJmx() {
        Kafka resource = new KafkaBuilder(ResourceUtils.createKafka(namespace, cluster, replicas, image, healthDelay, healthTimeout))
                .editSpec()
                    .withJmxTrans(jmxTransSpec)
                .endSpec()
                .build();

        InvalidResourceException e = assertThrows(InvalidResourceException.class, () -> JmxTrans.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource));
        assertThat(e.getMessage(), is("Can't start up JmxTrans 'foo-kafka-jmx-trans' in 'test' as Kafka spec.kafka.jmxOptions is not specified"));
    }

    @ParallelTest
    public void testTemplate() {
        Map<String, String> depLabels = TestUtils.map("l1", "v1", "l2", "v2",
                Labels.KUBERNETES_PART_OF_LABEL, "custom-part",
                Labels.KUBERNETES_MANAGED_BY_LABEL, "custom-managed-by");
        Map<String, String> expectedDepLabels = new HashMap<>(depLabels);
        expectedDepLabels.remove(Labels.KUBERNETES_MANAGED_BY_LABEL);

        Map<String, String> depAnots = TestUtils.map("a1", "v1", "a2", "v2");

        Map<String, String> podLabels = TestUtils.map("l3", "v3", "l4", "v4");
        Map<String, String> podAnots = TestUtils.map("a3", "v3", "a4", "v4");

        Map<String, String> saLabels = TestUtils.map("l5", "v5", "l6", "v6");
        Map<String, String> saAnots = TestUtils.map("a5", "v5", "a6", "v6");

        Affinity affinity = new AffinityBuilder()
                .withNewNodeAffinity()
                    .withNewRequiredDuringSchedulingIgnoredDuringExecution()
                        .withNodeSelectorTerms(new NodeSelectorTermBuilder()
                                .addNewMatchExpression()
                                    .withKey("key1")
                                    .withOperator("In")
                                    .withValues("value1", "value2")
                                .endMatchExpression()
                                .build())
                        .endRequiredDuringSchedulingIgnoredDuringExecution()
                .endNodeAffinity()
                .build();

        List<Toleration> tolerations = singletonList(new TolerationBuilder()
                .withEffect("NoExecute")
                .withKey("key1")
                .withOperator("Equal")
                .withValue("value1")
                .build());

        Kafka resource = new KafkaBuilder(kafkaAssembly)
                .editSpec()
                    .editOrNewJmxTrans()
                        .editOrNewTemplate()
                            .withNewDeployment()
                                .withNewMetadata()
                                    .withLabels(depLabels)
                                    .withAnnotations(depAnots)
                                .endMetadata()
                            .endDeployment()
                            .withNewPod()
                                .withNewMetadata()
                                    .withLabels(podLabels)
                                    .withAnnotations(podAnots)
                                .endMetadata()
                                .withPriorityClassName("top-priority")
                                .withSchedulerName("my-scheduler")
                                .withAffinity(affinity)
                                .withTolerations(tolerations)
                                .withEnableServiceLinks(false)
                            .endPod()
                            .withNewServiceAccount()
                                .withNewMetadata()
                                    .withLabels(saLabels)
                                    .withAnnotations(saAnots)
                                .endMetadata()
                            .endServiceAccount()
                        .endTemplate()
                    .endJmxTrans()
                .endSpec()
                .build();
        JmxTrans jmxTrans = JmxTrans.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource);

        // Check Deployment
        Deployment dep = jmxTrans.generateDeployment(null, null);
        assertThat(dep.getMetadata().getLabels(), hasEntries(expectedDepLabels));
        assertThat(dep.getMetadata().getAnnotations(), hasEntries(depAnots));
        assertThat(dep.getSpec().getTemplate().getSpec().getPriorityClassName(), is("top-priority"));
        assertThat(dep.getSpec().getTemplate().getSpec().getAffinity(), is(affinity));
        assertThat(dep.getSpec().getTemplate().getSpec().getTolerations(), is(tolerations));

        // Check Pods
        assertThat(dep.getSpec().getTemplate().getMetadata().getLabels(), hasEntries(podLabels));
        assertThat(dep.getSpec().getTemplate().getMetadata().getAnnotations(), hasEntries(podAnots));
        assertThat(dep.getSpec().getTemplate().getSpec().getSchedulerName(), is("my-scheduler"));
        assertThat(dep.getSpec().getTemplate().getSpec().getEnableServiceLinks(), is(false));

        // Check Service Account
        ServiceAccount sa = jmxTrans.generateServiceAccount();
        assertThat(sa.getMetadata().getLabels().entrySet().containsAll(saLabels.entrySet()), is(true));
        assertThat(sa.getMetadata().getAnnotations().entrySet().containsAll(saAnots.entrySet()), is(true));
    }

    @ParallelTest
    public void testContainerEnvVars() {

        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = "TEST_ENV_1";
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        ContainerEnvVar envVar2 = new ContainerEnvVar();
        String testEnvTwoKey = "TEST_ENV_2";
        String testEnvTwoValue = "test.env.two";
        envVar2.setName(testEnvTwoKey);
        envVar2.setValue(testEnvTwoValue);

        List<ContainerEnvVar> testEnvs = new ArrayList<>();
        testEnvs.add(envVar1);
        testEnvs.add(envVar2);
        ContainerTemplate container = new ContainerTemplate();
        container.setEnv(testEnvs);

        Kafka resource = new KafkaBuilder(kafkaAssembly)
                .editSpec()
                    .editJmxTrans()
                        .withNewTemplate()
                            .withNewContainer()
                                .withEnv(testEnvs)
                            .endContainer()
                        .endTemplate()
                    .endJmxTrans()
                .endSpec()
                .build();

        List<EnvVar> envVars = JmxTrans.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource).getEnvVars();

        assertThat("Failed to correctly set container environment variable: " + testEnvOneKey,
                envVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(true));
        assertThat("Failed to correctly set container environment variable: " + testEnvTwoKey,
                envVars.stream().filter(env -> testEnvTwoKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvTwoValue), is(true));
    }

    @ParallelTest
    public void testContainerEnvVarsConflict() {
        ContainerEnvVar envVar1 = new ContainerEnvVar();
        String testEnvOneKey = JmxTrans.ENV_VAR_JMXTRANS_LOGGING_LEVEL;
        String testEnvOneValue = "test.env.one";
        envVar1.setName(testEnvOneKey);
        envVar1.setValue(testEnvOneValue);

        List<ContainerEnvVar> testEnvs = new ArrayList<>();
        testEnvs.add(envVar1);
        ContainerTemplate container = new ContainerTemplate();
        container.setEnv(testEnvs);

        Kafka resource = new KafkaBuilder(kafkaAssembly)
                .editSpec()
                    .editJmxTrans()
                        .withNewTemplate()
                            .withNewContainer()
                                .withEnv(testEnvs)
                            .endContainer()
                        .endTemplate()
                    .endJmxTrans()
                .endSpec()
                .build();

        List<EnvVar> envVars = JmxTrans.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource).getEnvVars();

        assertThat("Failed to prevent over writing existing container environment variable: " + testEnvOneKey,
                envVars.stream().filter(env -> testEnvOneKey.equals(env.getName()))
                        .map(EnvVar::getValue).findFirst().orElse("").equals(testEnvOneValue), is(false));
    }

    @ParallelTest
    public void testContainerSecurityContext() {

        SecurityContext securityContext = new SecurityContextBuilder()
                .withPrivileged(false)
                .withReadOnlyRootFilesystem(false)
                .withAllowPrivilegeEscalation(false)
                .withRunAsNonRoot(true)
                .withNewCapabilities()
                    .addToDrop("ALL")
                .endCapabilities()
                .build();

        Kafka resource = new KafkaBuilder(kafkaAssembly)
                .editSpec()
                    .editJmxTrans()
                        .withNewTemplate()
                            .withNewContainer()
                                .withSecurityContext(securityContext)
                            .endContainer()
                        .endTemplate()
                    .endJmxTrans()
                .endSpec()
                .build();

        JmxTrans jmxTrans = JmxTrans.fromCrd(Reconciliation.DUMMY_RECONCILIATION, resource);
        assertThat(jmxTrans.createContainer(null).getSecurityContext(), is(securityContext));

        Deployment deployment = jmxTrans.generateDeployment(null, null);

        assertThat(deployment.getSpec().getTemplate().getSpec().getContainers(),
                hasItem(allOf(
                        hasProperty("name", equalTo(cluster + "-kafka-jmx-trans")),
                        hasProperty("securityContext", equalTo(securityContext))
                )));
    }

    @ParallelTest
    public void testRestrictedSecurityContext() {
        JmxTrans jmxTrans = JmxTrans.fromCrd(Reconciliation.DUMMY_RECONCILIATION, kafkaAssembly);
        jmxTrans.securityProvider = new RestrictedPodSecurityProvider();
        jmxTrans.securityProvider.configure(new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION));

        Deployment dep = jmxTrans.generateDeployment(null, null);
        assertThat(dep.getSpec().getTemplate().getSpec().getSecurityContext(), is(nullValue()));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getAllowPrivilegeEscalation(), is(false));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getRunAsNonRoot(), is(true));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getSeccompProfile().getType(), is("RuntimeDefault"));
        assertThat(dep.getSpec().getTemplate().getSpec().getContainers().get(0).getSecurityContext().getCapabilities().getDrop(), is(List.of("ALL")));
    }
}
