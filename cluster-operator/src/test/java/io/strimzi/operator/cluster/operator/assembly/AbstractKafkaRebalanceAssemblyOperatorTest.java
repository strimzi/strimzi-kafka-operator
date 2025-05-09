/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.common.ConditionBuilder;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceBuilder;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceSpec;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceSpecBuilder;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceState;
import io.strimzi.certs.Subject;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlApi;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.CruiseControlApiImpl;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.MockCruiseControl;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.test.ReadWriteUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.mockkube3.MockKube3;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(VertxExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractKafkaRebalanceAssemblyOperatorTest {
    protected static final String HOST = "localhost";
    protected static final String RESOURCE_NAME = "my-rebalance";
    protected static final String CLUSTER_NAME = "kafka-cruise-control-test-cluster";
    protected static final Kafka KAFKA = new KafkaBuilder()
            .withNewMetadata()
            .withName(CLUSTER_NAME)
            .withAnnotations(Map.of(Annotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled", Annotations.ANNO_STRIMZI_IO_KRAFT, "enabled"))
            .endMetadata()
            .withNewSpec()
            .withNewKafka()
            .withListeners(new GenericKafkaListenerBuilder()
                    .withName("plain")
                    .withPort(9092)
                    .withType(KafkaListenerType.INTERNAL)
                    .withTls(false)
                    .build())
            .endKafka()
            .withNewCruiseControl()
            .endCruiseControl()
            .endSpec()
            .build();
    protected static final KafkaRebalanceSpec EMPTY_KAFKA_REBALANCE_SPEC = new KafkaRebalanceSpecBuilder().build();
    protected static final PlatformFeaturesAvailability PFA = new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION);
    protected static KubernetesClient client;
    protected static MockKube3 mockKube;
    protected static Vertx vertx;
    protected static WorkerExecutor sharedWorkerExecutor;
    protected String namespace;
    protected KafkaRebalanceAssemblyOperator krao;
    protected ResourceOperatorSupplier supplier;

    protected static int cruiseControlPort;
    protected static File tlsKeyFile;
    protected static File tlsCrtFile;
    protected static MockCruiseControl cruiseControlServer;

    @BeforeAll
    public void beforeAll() throws IOException {
        // Configure the Kubernetes Mock
        mockKube = new MockKube3.MockKube3Builder()
                .withKafkaCrd()
                .withKafkaRebalanceCrd()
                .withDeletionController()
                .build();
        mockKube.start();
        client = mockKube.client();

        vertx = Vertx.vertx();
        sharedWorkerExecutor = vertx.createSharedWorkerExecutor("kubernetes-ops-pool");

        // Configure Cruise Control mock
        cruiseControlPort = TestUtils.getFreePort();
        tlsKeyFile = ReadWriteUtils.tempFile(KafkaRebalanceAssemblyOperatorTest.class.getSimpleName(), ".key");
        tlsCrtFile = ReadWriteUtils.tempFile(KafkaRebalanceAssemblyOperatorTest.class.getSimpleName(), ".crt");

        new MockCertManager().generateSelfSignedCert(tlsKeyFile, tlsCrtFile,
                new Subject.Builder().withCommonName("Trusted Test CA").build(), 365);

        cruiseControlServer = new MockCruiseControl(cruiseControlPort, tlsKeyFile, tlsCrtFile);
    }

    @AfterAll
    public static void afterAll() {
        sharedWorkerExecutor.close();
        vertx.close();
        mockKube.stop();
        if (cruiseControlServer != null && cruiseControlServer.isRunning()) {
            cruiseControlServer.stop();
        }
    }

    @BeforeEach
    public void beforeEach(TestInfo testInfo) {
        namespace = testInfo.getTestMethod().orElseThrow().getName().toLowerCase(Locale.ROOT);
        // handling Kubernetes constraint for namespace limit name (63 characters)
        if (namespace.length() > 63) {
            namespace = namespace.substring(0, 63);
        }
        mockKube.prepareNamespace(namespace);

        if (cruiseControlServer != null && cruiseControlServer.isRunning()) {
            cruiseControlServer.reset();
        }

        supplier = new ResourceOperatorSupplier(vertx, client, ResourceUtils.adminClientProvider(),
                ResourceUtils.kafkaAgentClientProvider(), ResourceUtils.metricsProvider(), PFA);

        // Override to inject mocked cruise control address so real cruise control not required
        krao = createKafkaRebalanceAssemblyOperator(ResourceUtils.dummyClusterOperatorConfig());
    }

    @AfterEach
    public void afterEach() {
        client.namespaces().withName(namespace).delete();
    }

    protected KafkaRebalanceAssemblyOperator createKafkaRebalanceAssemblyOperator(ClusterOperatorConfig config) {
        return new KafkaRebalanceAssemblyOperator(vertx, supplier, config, cruiseControlPort) {
            @Override
            public String cruiseControlHost(String clusterName, String clusterNamespace) {
                return HOST;
            }

            @Override
            public CruiseControlApi cruiseControlClientProvider(Secret ccSecret, Secret ccApiSecret, boolean apiAuthEnabled, boolean apiSslEnabled) {
                return new CruiseControlApiImpl(1, ccSecret, ccApiSecret, true, true);
            }
        };
    }

    protected void crdCreateKafka() {
        Kafka kafka = new KafkaBuilder(KAFKA)
                .withNewStatus()
                .withObservedGeneration(1L)
                .withConditions(new ConditionBuilder()
                        .withType("Ready")
                        .withStatus("True")
                        .build())
                .endStatus()
                .build();

        Crds.kafkaOperation(client).inNamespace(namespace).resource(kafka).create();
        Crds.kafkaOperation(client).inNamespace(namespace).resource(kafka).updateStatus();
    }

    protected void crdCreateCruiseControlSecrets() {
        Secret ccSecret = new SecretBuilder(MockCruiseControl.CC_SECRET)
                .editMetadata()
                .withName(CruiseControlResources.secretName(CLUSTER_NAME))
                .withNamespace(namespace)
                .endMetadata()
                .build();

        Secret ccApiSecret = new SecretBuilder(MockCruiseControl.CC_API_SECRET)
                .editMetadata()
                .withName(CruiseControlResources.apiSecretName(CLUSTER_NAME))
                .withNamespace(namespace)
                .endMetadata()
                .build();

        client.secrets().inNamespace(namespace).resource(ccSecret).create();
        client.secrets().inNamespace(namespace).resource(ccApiSecret).create();
    }

    protected KafkaRebalance createKafkaRebalance(String namespace, String clusterName, String resourceName,
                                                KafkaRebalanceSpec kafkaRebalanceSpec, boolean isAutoApproval) {
        return new KafkaRebalanceBuilder()
                .withNewMetadata()
                .withNamespace(namespace)
                .withName(resourceName)
                .withLabels(clusterName != null ? Collections.singletonMap(Labels.STRIMZI_CLUSTER_LABEL, CLUSTER_NAME) : null)
                .withAnnotations(isAutoApproval ? Collections.singletonMap(Annotations.ANNO_STRIMZI_IO_REBALANCE_AUTOAPPROVAL, "true") : null)
                .endMetadata()
                .withSpec(kafkaRebalanceSpec)
                .build();
    }

    protected void assertState(VertxTestContext context, KubernetesClient kubernetesClient, String namespace, String resource, KafkaRebalanceState state) {
        context.verify(() -> {
            KafkaRebalance kafkaRebalance = Crds.kafkaRebalanceOperation(kubernetesClient).inNamespace(namespace).withName(resource).get();
            assertThat(kafkaRebalance, KafkaRebalanceAssemblyOperatorTest.StateMatchers.hasState());
            Condition condition = KafkaRebalanceUtils.rebalanceStateCondition(kafkaRebalance.getStatus());
            assertThat(Collections.singletonList(condition), KafkaRebalanceAssemblyOperatorTest.StateMatchers.hasStateInConditions(state));
        });
    }

    protected static class StateMatchers extends AbstractResourceStateMatchers {

    }
}
