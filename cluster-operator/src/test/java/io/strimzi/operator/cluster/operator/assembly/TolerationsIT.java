/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.LabelSelectorBuilder;
import io.fabric8.kubernetes.api.model.Toleration;
import io.fabric8.kubernetes.api.model.TolerationBuilder;
import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.apps.StatefulSetBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.operator.resource.StatefulSetDiff;
import io.strimzi.test.k8s.KubeClusterResource;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@ExtendWith(VertxExtension.class)
public class TolerationsIT {

    protected KubeClusterResource cluster = KubeClusterResource.getInstance();
    private String namespace = "kafka-it-2";

    @BeforeEach
    public void beforeEach() {
        cluster.createNamespace(namespace);
    }

    @AfterEach
    public void afterEach() {
        cluster.deleteNamespaces();
    }

    @Test
    public void testEmptyStringValueIntoleration(VertxTestContext context) {
        Toleration t1 = new TolerationBuilder()
                .withEffect("NoSchedule")
                .withValue("")
                .build();

        List<Toleration> tolerationList = new ArrayList<>();
        tolerationList.add(t1);

        // CO does this over the generated STS
        tolerationList = ModelUtils.removeEmptyValuesFromTolerations(tolerationList);

        StatefulSet ss = new StatefulSetBuilder()
                .withNewMetadata()
                .withNamespace(namespace)
                .withName("foo")
                .endMetadata()
                .withNewSpec()
                    .withSelector(new LabelSelectorBuilder().withMatchLabels(Collections.singletonMap("app", "test")).build())
                    .withNewTemplate()
                        .withNewMetadata()
                            .withLabels(Collections.singletonMap("app", "test"))
                        .endMetadata()
                        .withNewSpec()
                            .withTolerations(tolerationList)
                            .withDnsPolicy("ClusterFirst")
                            .withRestartPolicy("Always")
                            .withSchedulerName("default-scheduler")
                            .withSecurityContext(null)
                            .withTerminationGracePeriodSeconds(30L)
                        .endSpec()
                    .endTemplate()
                .endSpec()
                .build();

        KubernetesClient client = new DefaultKubernetesClient();

        client.apps().statefulSets().inNamespace(namespace).create(ss);
        StatefulSet stsk8s = client.apps().statefulSets().inNamespace(namespace).withName("foo").get();
        StatefulSetDiff diff = new StatefulSetDiff(ss, stsk8s);
        Checkpoint checkpoint = context.checkpoint();
        context.verify(() -> {
                assertThat(diff.changesSpecTemplate(), is(false));
                assertThat(stsk8s.getSpec().getTemplate().getSpec().getTolerations().get(0).getValue(), is(nullValue()));
                assertThat(ss.getSpec().getTemplate().getSpec().getTolerations().get(0).getValue(), is(nullValue()));
                checkpoint.flag();
            }
        );
    }
}
