/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.security;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.SecurityContext;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.skodjob.annotations.Desc;
import io.skodjob.annotations.Label;
import io.skodjob.annotations.Step;
import io.skodjob.annotations.SuiteDoc;
import io.skodjob.annotations.TestDoc;
import io.skodjob.kubetest4j.resources.KubeResourceManager;
import io.skodjob.kubetest4j.utils.KubeUtils;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.operator.common.Annotations;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.annotations.ParallelNamespaceTest;
import io.strimzi.systemtest.annotations.RequiredMinKubeOrOcpBasedKubeVersion;
import io.strimzi.systemtest.docs.TestDocsLabels;
import io.strimzi.systemtest.resources.crd.KafkaComponents;
import io.strimzi.systemtest.resources.operator.ClusterOperatorConfigurationBuilder;
import io.strimzi.systemtest.resources.operator.SetupClusterOperator;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaBridgeTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectTemplates;
import io.strimzi.systemtest.templates.crd.KafkaConnectorTemplates;
import io.strimzi.systemtest.templates.crd.KafkaMirrorMaker2Templates;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.utils.ClientUtils;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaConnectUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.PodUtils;
import io.strimzi.testclients.clients.kafka.KafkaProducerConsumer;
import io.strimzi.testclients.clients.kafka.KafkaProducerConsumerBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.ArrayList;
import java.util.List;

import static io.strimzi.systemtest.TestTags.REGRESSION;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag(REGRESSION)
@SuiteDoc(
    description = @Desc("Test suite for verifying Pod Security profiles with Strimzi operands, ensuring containers have properly set security contexts when using the restricted Pod Security provider class."),
    beforeTestSteps = {
        @Step(value = "Deploy Cluster Operator with STRIMZI_POD_SECURITY_PROVIDER_CLASS set to restricted.", expected = "Cluster Operator is deployed with the restricted Pod Security provider.")
    },
    labels = {
        @Label(value = TestDocsLabels.SECURITY)
    }
)
public class PodSecurityProfilesST extends AbstractST {

    private static final Logger LOGGER = LogManager.getLogger(PodSecurityProfilesST.class);

    @TestDoc(
        description = @Desc("Test verifying that all Strimzi operands function correctly under the Kubernetes restricted Pod Security profile, including proper security context generation for all containers and successful message exchange across Kafka, KafkaConnect, KafkaBridge, and KafkaMirrorMaker2 components."),
        steps = {
            @Step(value = "Label the test namespace with pod-security.kubernetes.io/enforce: restricted.", expected = "Namespace is labeled with restricted Pod Security profile."),
            @Step(value = "Deploy two Kafka clusters: one source and one target for MirrorMaker2.", expected = "Kafka clusters are deployed."),
            @Step(value = "Deploy KafkaConnect with file plugin, KafkaBridge, KafkaMirrorMaker2, and a FileSink KafkaConnector.", expected = "All additional operands are deployed."),
            @Step(value = "Produce and consume messages in the source Kafka cluster using clients with restricted security profile applied.", expected = "Messages are produced and consumed successfully."),
            @Step(value = "Verify that all Pod containers for Kafka, KafkaConnect, KafkaBridge, and KafkaMirrorMaker2 have proper security context settings.", expected = "All containers have expected security context properties."),
            @Step(value = "Verify KafkaConnect FileSink connector received messages.", expected = "Messages are present in the sink file."),
            @Step(value = "Verify KafkaMirrorMaker2 mirrored messages to the target cluster.", expected = "Messages are present in the target cluster."),
            @Step(value = "Deploy a producer without restricted security profile and verify it fails.", expected = "Producer fails due to Pod Security profile violation.")
        },
        labels = {
            @Label(value = TestDocsLabels.SECURITY)
        }
    )
    @ParallelNamespaceTest
    @RequiredMinKubeOrOcpBasedKubeVersion(kubeVersion = 1.23, ocpBasedKubeVersion = 1.24)
    void testOperandsWithRestrictedSecurityProfile() {

        final TestStorage testStorage = new TestStorage(KubeResourceManager.get().getTestContext());

        final String mm1TargetClusterName = testStorage.getTargetClusterName() + "-mm1";
        final String mm2TargetClusterName = testStorage.getTargetClusterName() + "-mm2";
        final String mm2SourceMirroredTopicName = testStorage.getClusterName() + "." + testStorage.getTopicName();

        // Label particular Namespace with pod-security.kubernetes.io/enforce: restricted
        KubeUtils.labelNamespace(testStorage.getNamespaceName(), "pod-security.kubernetes.io/enforce", "restricted");

        // 1 source Kafka Cluster, 2 target Kafka Cluster, 1 for MM1 and MM2 each having different target Kafka Cluster,

        LOGGER.info("Deploy Kafka Clusters resources");
        KubeResourceManager.get().createResourceWithWait(
            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getBrokerPoolName(), testStorage.getClusterName(), 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), testStorage.getControllerPoolName(), testStorage.getClusterName(), 1).build(),

            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), KafkaComponents.getBrokerPoolName(mm1TargetClusterName), mm1TargetClusterName, 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), KafkaComponents.getControllerPoolName(mm1TargetClusterName), mm1TargetClusterName, 1).build(),

            KafkaNodePoolTemplates.brokerPoolPersistentStorage(testStorage.getNamespaceName(), KafkaComponents.getBrokerPoolName(mm2TargetClusterName), mm2TargetClusterName, 1).build(),
            KafkaNodePoolTemplates.controllerPoolPersistentStorage(testStorage.getNamespaceName(), KafkaComponents.getControllerPoolName(mm2TargetClusterName), mm2TargetClusterName, 1).build()
        );
        KubeResourceManager.get().createResourceWithWait(
            KafkaTemplates.kafka(testStorage.getNamespaceName(), testStorage.getClusterName(), 1).build(),
            KafkaTemplates.kafka(testStorage.getNamespaceName(), mm1TargetClusterName, 1).build(),
            KafkaTemplates.kafka(testStorage.getNamespaceName(), mm2TargetClusterName, 1).build(),
            KafkaTopicTemplates.topic(testStorage).build()
        );

        // Kafka Bridge and KafkaConnect use main Kafka Cluster (one serving as source for MM1 and MM2)
        // MM1 and MM2 shares source Kafka Cluster and each have their own target kafka cluster

        LOGGER.info("Deploy all additional operands: MM1, MM2, Bridge, KafkaConnect");
        KubeResourceManager.get().createResourceWithWait(
            KafkaConnectTemplates.kafkaConnectWithFilePlugin(testStorage.getNamespaceName(), testStorage.getClusterName(), 1)
                .editMetadata()
                    .addToAnnotations(Annotations.STRIMZI_IO_USE_CONNECTOR_RESOURCES, "true")
                .endMetadata()
                .editSpec()
                    .addToConfig("key.converter.schemas.enable", false)
                    .addToConfig("value.converter.schemas.enable", false)
                    .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                    .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
                .endSpec()
                .build(),
            KafkaBridgeTemplates.kafkaBridge(testStorage.getNamespaceName(), testStorage.getClusterName(), KafkaResources.plainBootstrapAddress(testStorage.getClusterName()), 1).build(),
            KafkaMirrorMaker2Templates.kafkaMirrorMaker2(testStorage.getNamespaceName(), testStorage.getClusterName() + "-mm2", testStorage.getClusterName(), mm2TargetClusterName, 1, false)
                .editSpec()
                    .editFirstMirror()
                        .editSourceConnector()
                            .addToConfig("refresh.topics.interval.seconds", "1")
                        .endSourceConnector()
                    .endMirror()
                .endSpec()
                .build());

        LOGGER.info("Deploy File Sink Kafka Connector: {}/{}", testStorage.getNamespaceName(), testStorage.getClusterName());
        KubeResourceManager.get().createResourceWithWait(KafkaConnectorTemplates.kafkaConnector(testStorage.getNamespaceName(), testStorage.getClusterName())
            .editSpec()
                .withClassName("org.apache.kafka.connect.file.FileStreamSinkConnector")
                .addToConfig("topics", testStorage.getTopicName())
                .addToConfig("file", TestConstants.DEFAULT_SINK_FILE_PATH)
                .addToConfig("key.converter", "org.apache.kafka.connect.storage.StringConverter")
                .addToConfig("value.converter", "org.apache.kafka.connect.storage.StringConverter")
            .endSpec()
            .build());

        // Messages produced to Main Kafka Cluster (source) will be sinked to file, and mirrored into targeted Kafkas to later verify Operands work correctly.
        LOGGER.info("Transmit messages in Cluster: {}/{}", testStorage.getNamespaceName(), testStorage.getClusterName());
        final KafkaProducerConsumerBuilder kafkaProducerConsumerBuilder = new KafkaProducerConsumerBuilder()
            .withProducerName(testStorage.getProducerName())
            .withConsumerName(testStorage.getConsumerName())
            .withNamespaceName(testStorage.getNamespaceName())
            .withTopicName(testStorage.getTopicName())
            .withConsumerGroup(ClientUtils.generateRandomConsumerGroup())
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(testStorage.getClusterName()))
            .withMessageCount(testStorage.getMessageCount());

        final KafkaProducerConsumer kafkaProducerConsumer = kafkaProducerConsumerBuilder.build();

        KubeResourceManager.get().createResourceWithWait(
            applyRestrictedSecurityProfileToClientJob(kafkaProducerConsumer.getProducer().getJob()),
            applyRestrictedSecurityProfileToClientJob(kafkaProducerConsumer.getConsumer().getJob())
        );
        ClientUtils.waitForClientsSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getProducerName(), testStorage.getMessageCount());

        // verifies that Pods and Containers have proper generated SC
        final List<Pod> podsWithProperlyGeneratedSecurityContexts = new ArrayList<>(PodUtils.getKafkaClusterPods(testStorage));
        podsWithProperlyGeneratedSecurityContexts.addAll(KubeResourceManager.get().kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getBridgeSelector()));
        podsWithProperlyGeneratedSecurityContexts.addAll(KubeResourceManager.get().kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector()));
        podsWithProperlyGeneratedSecurityContexts.addAll(KubeResourceManager.get().kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getMM2Selector()));
        verifyPodAndContainerSecurityContext(podsWithProperlyGeneratedSecurityContexts);

        // verify KafkaConnect
        final String kafkaConnectPodName = KubeResourceManager.get().kubeClient().listPods(testStorage.getNamespaceName(), testStorage.getKafkaConnectSelector()).get(0).getMetadata().getName();
        KafkaConnectUtils.waitForMessagesInKafkaConnectFileSink(testStorage.getNamespaceName(), kafkaConnectPodName, TestConstants.DEFAULT_SINK_FILE_PATH, testStorage.getMessageCount());

        // verify MM2
        final KafkaProducerConsumer mm2Client = kafkaProducerConsumerBuilder
            .withBootstrapAddress(KafkaResources.plainBootstrapAddress(mm2TargetClusterName))
            .withTopicName(mm2SourceMirroredTopicName)
            .build();

        KubeResourceManager.get().createResourceWithWait(applyRestrictedSecurityProfileToClientJob(mm2Client.getConsumer().getJob()));
        ClientUtils.waitForClientSuccess(testStorage.getNamespaceName(), testStorage.getConsumerName(), testStorage.getMessageCount());

        // verify that client incorrectly configured Pod Security Profile wont successfully communicate.
        final Job invalidProducerJob = kafkaProducerConsumer.getProducer().getJob();

        // excepted failure
        // job_controller.go:1437 pods "..." is forbidden: violates PodSecurity "restricted:latest": allowPrivilegeEscalation != false
        // (container "..." must set securityContext.allowPrivilegeEscalation=false),
        // unrestricted capabilities (container "..." must set securityContext.capabilities.drop=["ALL"]),
        // runAsNonRoot != true (pod or container "..." must set securityContext.runAsNonRoot=true),
        // seccompProfile (pod or container "..." must set securityContext.seccompProfile.type to "RuntimeDefault" or "Localhost")
        KubeResourceManager.get().createResourceWithoutWait(invalidProducerJob);
        ClientUtils.waitForClientTimeout(testStorage.getNamespaceName(), testStorage.getProducerName(), testStorage.getMessageCount());
    }

    /**
     * Method that applies "restricted" Pod Security Profile to every client's Job.
     *
     * @param clientJob     Client's Job that should be updated.
     *
     * @return  updated client's Job with "restricted" Pod Security Profile.
     */
    private Job applyRestrictedSecurityProfileToClientJob(Job clientJob) {
        return new JobBuilder(clientJob)
            .editSpec()
                .editTemplate()
                    .editSpec()
                        .withNewSecurityContext()
                            .withRunAsNonRoot(true)
                            .withNewSeccompProfile()
                                .withType("RuntimeDefault")
                            .endSeccompProfile()
                        .endSecurityContext()
                        .editFirstContainer()
                            .withNewSecurityContext()
                                .withAllowPrivilegeEscalation(false)
                                .withNewCapabilities()
                                    .withDrop("ALL")
                                .endCapabilities()
                                .withRunAsNonRoot(true)
                                .withNewSeccompProfile()
                                    .withType("RuntimeDefault")
                                .endSeccompProfile()
                            .endSecurityContext()
                        .endContainer()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build();
    }

    @BeforeAll
    void beforeAll() {
        // we configure Pod Security via provider class, which sets SecurityContext to all containers (e.g., Kafka,
        // Entity Operator, Bridge). Another alternative but more complicated is to set it via .template section inside each CR.
        SetupClusterOperator
            .getInstance()
            .withCustomConfiguration(new ClusterOperatorConfigurationBuilder()
                .withExtraEnvVars(new EnvVarBuilder()
                    .withName("STRIMZI_POD_SECURITY_PROVIDER_CLASS")
                    // default is `baseline` and thus other tests suites are testing it
                    .withValue("restricted")
                    .build())
                .build()
            )
            .install();
    }

    private void verifyPodAndContainerSecurityContext(final Iterable<? extends Pod> brokerPods) {
        for (final Pod pod : brokerPods) {
            // verify Container SecurityContext
            if (pod.getSpec().getContainers() != null) {
                verifyContainerSecurityContext(pod.getSpec().getContainers());
            }
        }
    }

    /**
     * Provides a check for Container SecurityContext inside Pod resource.
     *
     * securityContext:
     *     allowPrivilegeEscalation: false
     *     capabilities:
     *      drop:
     *       - ALL
     *     runAsNonRoot: true
     *     seccompProfile:
     *      type: RuntimeDefault
     * @param containers    containers, with injected SecurityContext by Cluster Operator for restricted profile
     */
    private void verifyContainerSecurityContext(final Iterable<? extends Container> containers) {
        for (final Container c : containers) {
            final SecurityContext sc = c.getSecurityContext();
            LOGGER.debug("Verifying Container: {} with SecurityContext: {}", c.getName(), sc);

            assertThat(sc.getAllowPrivilegeEscalation(), CoreMatchers.is(false));
            assertThat(sc.getCapabilities().getDrop(), CoreMatchers.hasItem("ALL"));
            assertThat(sc.getRunAsNonRoot(), CoreMatchers.is(true));
            assertThat(sc.getSeccompProfile().getType(), CoreMatchers.is("RuntimeDefault"));
        }
    }
}
