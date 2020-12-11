/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.certs.OpenSslCertManager;
import io.strimzi.operator.KubernetesVersion;
import io.strimzi.operator.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.common.PasswordGenerator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.ClusterRoleBindingOperator;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaAssemblyOperatorNonParametrizedTest {

    public static final String NAMESPACE = "test";
    public static final String NAME = "my-kafka";
    private static Vertx vertx;
    private OpenSslCertManager certManager = new OpenSslCertManager();
    private PasswordGenerator passwordGenerator = new PasswordGenerator(12,
            "abcdefghijklmnopqrstuvwxyz" +
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ",
            "abcdefghijklmnopqrstuvwxyz" +
                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
                    "0123456789");

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    @Test
    public void testDeleteClusterRoleBindings(VertxTestContext context) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(false);
        ClusterRoleBindingOperator mockCrbOps = supplier.clusterRoleBindingOperator;
        ArgumentCaptor<ClusterRoleBinding> desiredCrb = ArgumentCaptor.forClass(ClusterRoleBinding.class);
        when(mockCrbOps.reconcile(eq(KafkaResources.initContainerClusterRoleBindingName(NAME, NAMESPACE)), desiredCrb.capture())).thenReturn(Future.succeededFuture());

        KafkaAssemblyOperator op = new KafkaAssemblyOperator(vertx, new PlatformFeaturesAvailability(false, KubernetesVersion.V1_9), certManager, passwordGenerator,
                supplier, ResourceUtils.dummyClusterOperatorConfig(1L));
        Reconciliation reconciliation = new Reconciliation("test-trigger", Kafka.RESOURCE_KIND, NAMESPACE, NAME);

        Checkpoint async = context.checkpoint();

        op.delete(reconciliation)
                .onComplete(context.succeeding(c -> context.verify(() -> {
                    assertThat(desiredCrb.getValue(), is(nullValue()));
                    Mockito.verify(mockCrbOps, times(1)).reconcile(any(), any());

                    async.flag();
                })));
    }
}