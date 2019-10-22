/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractRollerTest {
    public static final Predicate<Pod> ROLL_ALL_PODS = pod -> true;
    protected static Vertx vertx;
    List<String> restarted;

    @BeforeClass
    public static void startVertx() {
        vertx = Vertx.vertx();
    }

    public List<Integer> restarted() {
        return restarted.stream().map(AbstractRollerTest::podName2Number).collect(Collectors.toList());
    }

    @AfterClass
    public static void stopVertx() {
        vertx.close();
    }

    @Before
    public void clearRestarted() {
        restarted = new ArrayList<>();
    }

    PodOperator mockPodOps(Function<Integer, Future<Void>> readiness) {
        PodOperator podOps = mock(PodOperator.class);
        when(podOps.get(any(), any())).thenAnswer(
            invocation -> new PodBuilder()
                    .withNewMetadata()
                        .withNamespace(invocation.getArgument(0))
                        .withName(invocation.getArgument(1))
                    .endMetadata()
                .build()
        );
        when(podOps.readiness(any(), any(), anyLong(), anyLong())).thenAnswer(
            invocationOnMock ->  {
                String podName = invocationOnMock.getArgument(1);
                return readiness.apply(podName2Number(podName));
            });
        return podOps;
    }

    static int podName2Number(String podName) {
        return Integer.parseInt(podName.substring(ssName().length() + 1));
    }

    StatefulSet buildStatefulSet() {
        return new StatefulSetBuilder()
                .withNewMetadata()
                .withName(ssName())
                .withNamespace(ssNamespace())
                .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, clusterName())
                .endMetadata()
                .withNewSpec()
                .withReplicas(5)
                .endSpec()
                .build();
    }

    static final String clusterName() {
        return "c";
    }

    static String ssName() {
        return "c-kafka";
    }

    static final String ssNamespace() {
        return "ns";
    };
}
