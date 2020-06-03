/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.AdminClientProvider;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Timeout;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class DynamicUpdateTest {

    private io.debezium.kafka.KafkaCluster kafkaCluster;
    Vertx vertx = Vertx.vertx();

    @BeforeEach
    public void before() throws IOException {
        kafkaCluster = new io.debezium.kafka.KafkaCluster()
                .addBrokers(1)
                .deleteDataPriorToStartup(true)
                .deleteDataUponShutdown(true)
                .usingDirectory(Files.createTempDirectory(getClass().getName()).toFile())
                .withKafkaConfiguration(new Properties());
        kafkaCluster.startup();
    }

    @AfterEach
    public void after() {
        kafkaCluster.shutdown();
    }

    @Test
    @Timeout(value = 2, timeUnit = TimeUnit.MINUTES)
    public void simpleTest(VertxTestContext context) throws InterruptedException {
        Kafka kafka = new KafkaBuilder().withNewMetadata()
                .withName("k")
                .withNamespace("ns")
                .endMetadata()
                .withNewSpec()
                .withNewKafka()
                .withReplicas(1)
                .endKafka()
                .endSpec()
                .build();
        ClusterOperatorConfig config = ResourceUtils.dummyClusterOperatorConfig(KafkaVersionTestUtils.getKafkaVersionLookup());
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        Future<KafkaAssemblyOperator.ReconciliationState> f = new KafkaAssemblyOperator(vertx, null, null, null,
                supplier,
                config) {

        }.new ReconciliationState(null, kafka) {
            {
                oldKafkaReplicas = 1;
                kafkaCluster = KafkaCluster.fromCrd(kafka, config.versions());
                when(supplier.podOperations.readiness(any(), any(), anyLong(), anyLong())).thenReturn(Future.succeededFuture());
            }

            @Override
            protected Future<Admin> adminClient(AdminClientProvider adminClientProvider, String namespace, String cluster, int podId) {
                Properties p = new Properties();
                String bootstrap = DynamicUpdateTest.this.kafkaCluster.brokerList();
                p.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
                p.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
                return Future.succeededFuture(Admin.create(p));
            }
        }.kafkaBrokerDynamicConfiguration();
        CountDownLatch l = new CountDownLatch(1);
        f.onComplete(ar -> {
            assertThat(ar.succeeded(), is(notNullValue()));
            l.countDown();
        });
        l.await();
        context.completeNow();
    }
}