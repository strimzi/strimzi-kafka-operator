/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBuilder;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleList;
import io.fabric8.kubernetes.api.model.rbac.PolicyRule;
import io.fabric8.kubernetes.api.model.rbac.PolicyRuleBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.extension.ExtendWith;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

@ExtendWith(VertxExtension.class)
public class ClusterRoleOperatorIT extends AbstractNonNamespacedResourceOperatorIT<KubernetesClient,
        ClusterRole, ClusterRoleList, Resource<ClusterRole>> {

    @Override
    protected AbstractNonNamespacedResourceOperator<KubernetesClient,
            ClusterRole, ClusterRoleList, Resource<ClusterRole>> operator() {
        return new ClusterRoleOperator(vertx, client);
    }

    @Override
    protected ClusterRole getOriginal()  {
        PolicyRule rule = new PolicyRuleBuilder()
                .withApiGroups("")
                .withResources("nodes")
                .withVerbs("get")
                .build();

        return new ClusterRoleBuilder()
                .withNewMetadata()
                    .withName(resourceName)
                    .withLabels(singletonMap("state", "new"))
                .endMetadata()
                .withRules(rule)
                .build();
    }

    @Override
    protected ClusterRole getModified()  {
        PolicyRule rule = new PolicyRuleBuilder()
                .withApiGroups("")
                .withResources("nodes")
                .withVerbs("get", "list")
                .build();

        return new ClusterRoleBuilder()
                .withNewMetadata()
                .withName(resourceName)
                .withLabels(singletonMap("state", "modified"))
                .endMetadata()
                .withRules(rule)
                .build();
    }

    @Override
    protected void assertResources(VertxTestContext context, ClusterRole expected, ClusterRole actual)   {
        context.verify(() -> {
            assertThat(actual.getMetadata().getName(), is(expected.getMetadata().getName()));
            assertThat(actual.getMetadata().getLabels(), is(expected.getMetadata().getLabels()));
            assertThat(actual.getRules(), hasSize(expected.getRules().size()));
            assertThat(actual.getRules().get(0).getApiGroups(), is(expected.getRules().get(0).getApiGroups()));
            assertThat(actual.getRules().get(0).getResources(), is(expected.getRules().get(0).getResources()));
            assertThat(actual.getRules().get(0).getVerbs(), is(expected.getRules().get(0).getVerbs()));
        });

    }
}
