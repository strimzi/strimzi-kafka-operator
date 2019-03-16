/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.rbac.DoneableKubernetesClusterRole;
import io.fabric8.kubernetes.api.model.rbac.KubernetesClusterRole;
import io.fabric8.kubernetes.api.model.rbac.KubernetesClusterRoleBuilder;
import io.fabric8.kubernetes.api.model.rbac.KubernetesClusterRoleList;
import io.fabric8.kubernetes.api.model.rbac.KubernetesPolicyRule;
import io.fabric8.kubernetes.api.model.rbac.KubernetesPolicyRuleBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.runner.RunWith;

import static java.util.Collections.singletonMap;

@RunWith(VertxUnitRunner.class)
public class ClusterRoleOperatorIT extends AbstractNonNamespacedResourceOperatorIT<KubernetesClient,
        KubernetesClusterRole, KubernetesClusterRoleList, DoneableKubernetesClusterRole,
        Resource<KubernetesClusterRole, DoneableKubernetesClusterRole>> {

    @Override
    protected AbstractNonNamespacedResourceOperator<KubernetesClient,
            KubernetesClusterRole, KubernetesClusterRoleList, DoneableKubernetesClusterRole,
            Resource<KubernetesClusterRole, DoneableKubernetesClusterRole>> operator() {
        return new ClusterRoleOperator(vertx, client);
    }

    @Override
    protected KubernetesClusterRole getOriginal()  {
        KubernetesPolicyRule rule = new KubernetesPolicyRuleBuilder()
                .withApiGroups("")
                .withResources("nodes")
                .withVerbs("get")
                .build();

        return new KubernetesClusterRoleBuilder()
                .withNewMetadata()
                    .withName(RESOURCE_NAME)
                    .withLabels(singletonMap("state", "new"))
                .endMetadata()
                .withRules(rule)
                .build();
    }

    @Override
    protected KubernetesClusterRole getModified()  {
        KubernetesPolicyRule rule = new KubernetesPolicyRuleBuilder()
                .withApiGroups("")
                .withResources("nodes")
                .withVerbs("get", "list")
                .build();

        return new KubernetesClusterRoleBuilder()
                .withNewMetadata()
                .withName(RESOURCE_NAME)
                .withLabels(singletonMap("state", "modified"))
                .endMetadata()
                .withRules(rule)
                .build();
    }

    @Override
    protected void assertResources(TestContext context, KubernetesClusterRole expected, KubernetesClusterRole actual)   {
        context.assertEquals(expected.getMetadata().getName(), actual.getMetadata().getName());
        context.assertEquals(expected.getMetadata().getLabels(), actual.getMetadata().getLabels());
        context.assertEquals(expected.getRules().size(), actual.getRules().size());
        context.assertEquals(expected.getRules().get(0).getApiGroups(), actual.getRules().get(0).getApiGroups());
        context.assertEquals(expected.getRules().get(0).getResources(), actual.getRules().get(0).getResources());
        context.assertEquals(expected.getRules().get(0).getVerbs(), actual.getRules().get(0).getVerbs());
    }
}
