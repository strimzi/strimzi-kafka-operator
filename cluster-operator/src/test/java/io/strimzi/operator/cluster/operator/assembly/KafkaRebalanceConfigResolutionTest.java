/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.strimzi.api.kafka.model.rebalance.KafkaRebalance;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceMode;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceSpec;
import io.strimzi.api.kafka.model.rebalance.KafkaRebalanceSpecBuilder;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.cruisecontrol.RebalanceOptions;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.cruisecontrol.CruiseControlParameters;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

@ExtendWith(VertxExtension.class)
public class KafkaRebalanceConfigResolutionTest {

    private static final String CLUSTER_NAMESPACE = "cruise-control-namespace";
    private static final String RESOURCE_NAME = "my-rebalance";

    private KafkaRebalanceAssemblyOperator createOperator(Vertx vertx) {
        ResourceOperatorSupplier supplier = ResourceUtils.supplierWithMocks(true);
        return new KafkaRebalanceAssemblyOperator(vertx, supplier, ResourceUtils.dummyClusterOperatorConfig());
    }

    private Reconciliation reconciliation() {
        return new Reconciliation("test", KafkaRebalance.RESOURCE_KIND, CLUSTER_NAMESPACE, RESOURCE_NAME);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testDeprecatedFieldsMappedToConfig(Vertx vertx) {
        KafkaRebalanceSpec spec = new KafkaRebalanceSpecBuilder()
                .withSkipHardGoalCheck(true)
                .withExcludedTopics("my-topic")
                .build();

        RebalanceOptions options = createOperator(vertx).convertRebalanceSpecToRebalanceOptions(reconciliation(), spec).build();

        assertThat(options.getConfig().get(CruiseControlParameters.SKIP_HARD_GOAL_CHECK.toString()), is("true"));
        assertThat(options.getConfig().get(CruiseControlParameters.EXCLUDED_TOPICS.toString()), is("my-topic"));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testConfigTakesPriorityOverDeprecatedFields(Vertx vertx) {
        KafkaRebalanceSpec spec = new KafkaRebalanceSpecBuilder()
                .withConfig(Map.of(
                        CruiseControlParameters.SKIP_HARD_GOAL_CHECK.toString(), "false"))
                .withSkipHardGoalCheck(true)
                .build();

        RebalanceOptions options = createOperator(vertx).convertRebalanceSpecToRebalanceOptions(reconciliation(), spec).build();

        assertThat(options.getConfig().get(CruiseControlParameters.SKIP_HARD_GOAL_CHECK.toString()), is("false"));
    }

    @Test
    public void testForbiddenConfigKeysFiltered(Vertx vertx) {
        KafkaRebalanceSpec spec = new KafkaRebalanceSpecBuilder()
                .withConfig(Map.of(
                        CruiseControlParameters.DRY_RUN.toString(), "true",
                        CruiseControlParameters.EXCLUDED_TOPICS.toString(), "my-topic"
                ))
                .build();

        RebalanceOptions options = createOperator(vertx).convertRebalanceSpecToRebalanceOptions(reconciliation(), spec).build();

        assertThat(options.getConfig().containsKey(CruiseControlParameters.DRY_RUN.toString()), is(false));
        assertThat(options.getConfig().get(CruiseControlParameters.EXCLUDED_TOPICS.toString()), is("my-topic"));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testDeprecatedBrokersConvertedToNodes(Vertx vertx) {
        KafkaRebalanceSpec spec = new KafkaRebalanceSpecBuilder()
                .withMode(KafkaRebalanceMode.ADD_BROKERS)
                .withBrokers(3, 4)
                .build();

        RebalanceOptions options = createOperator(vertx).convertRebalanceSpecToRebalanceOptions(reconciliation(), spec).build();

        assertThat(options.getNodes(), hasSize(2));
        assertThat(options.getNodes().get(0).getBrokerId(), is(3));
        assertThat(options.getNodes().get(1).getBrokerId(), is(4));
        assertThat(options.getNodes().get(0).getVolumeIds(), is(nullValue()));
    }
}
