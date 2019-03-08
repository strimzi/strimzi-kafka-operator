/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBuilder;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleList;
import io.fabric8.kubernetes.api.model.rbac.DoneableClusterRole;
import io.fabric8.kubernetes.api.model.rbac.PolicyRule;
import io.fabric8.kubernetes.api.model.rbac.PolicyRuleBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.runner.RunWith;

import static java.util.Collections.singletonMap;

@RunWith(VertxUnitRunner.class)
public class ClusterRoleOperatorIT extends AbstractNonNamespacedResourceOperatorIT<KubernetesClient,
        ClusterRole, ClusterRoleList, DoneableClusterRole,
        Resource<ClusterRole, DoneableClusterRole>> {

    @Override
    protected AbstractNonNamespacedResourceOperator<KubernetesClient,
            ClusterRole, ClusterRoleList, DoneableClusterRole,
            Resource<ClusterRole, DoneableClusterRole>> operator() {
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
                    .withName(RESOURCE_NAME)
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
                .withName(RESOURCE_NAME)
                .withLabels(singletonMap("state", "modified"))
                .endMetadata()
                .withRules(rule)
                .build();
    }

    @Override
    protected void assertResources(TestContext context, ClusterRole expected, ClusterRole actual)   {
        context.assertEquals(expected.getMetadata().getName(), actual.getMetadata().getName());
        context.assertEquals(expected.getMetadata().getLabels(), actual.getMetadata().getLabels());
        context.assertEquals(expected.getRules().size(), actual.getRules().size());
        context.assertEquals(expected.getRules().get(0).getApiGroups(), actual.getRules().get(0).getApiGroups());
        context.assertEquals(expected.getRules().get(0).getResources(), actual.getRules().get(0).getResources());
        context.assertEquals(expected.getRules().get(0).getVerbs(), actual.getRules().get(0).getVerbs());
    }
}
