/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.operator.resource;

import io.fabric8.kubernetes.api.model.rbac.PolicyRule;
import io.fabric8.kubernetes.api.model.rbac.PolicyRuleBuilder;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.extension.ExtendWith;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(VertxExtension.class)
public class RoleOperatorIT extends AbstractNamespacedResourceOperatorIT<
        KubernetesClient,
        Role,
        RoleList,
        Resource<Role>> {

    @Override
    protected AbstractNamespacedResourceOperator<
                KubernetesClient,
                Role,
                RoleList,
                Resource<Role>> operator() {
        return new RoleOperator(vertx, client);
    }

    @Override
    protected Role getOriginal()  {
        PolicyRule rule = new PolicyRuleBuilder()
                .withApiGroups("")
                .withResources("pods")
                .withVerbs("get")
                .build();

        return new RoleBuilder()
                .withNewMetadata()
                    .withName(resourceName)
                    .withNamespace(namespace)
                    .withLabels(singletonMap("state", "new"))
                .endMetadata()
                .withRules(rule)
                .build();
    }

    @Override
    protected Role getModified()  {
        PolicyRule rule = new PolicyRuleBuilder()
                .withApiGroups("")
                .withResources("pods")
                .withVerbs("get", "list")
                .build();

        return new RoleBuilder()
                .withNewMetadata()
                    .withName(resourceName)
                    .withNamespace(namespace)
                    .withLabels(singletonMap("state", "modified"))
                .endMetadata()
                .withRules(rule)
                .build();
    }

    @Override
    protected void assertResources(VertxTestContext context, Role expected, Role actual)   {
        context.verify(() -> assertThat(actual.getMetadata().getName(), is(expected.getMetadata().getName())));
        context.verify(() -> assertThat(actual.getMetadata().getNamespace(), is(expected.getMetadata().getNamespace())));
        context.verify(() -> assertThat(actual.getMetadata().getLabels(), is(expected.getMetadata().getLabels())));
        context.verify(() -> assertThat(actual.getRules().size(), is(expected.getRules().size())));
        context.verify(() -> assertThat(actual.getRules().get(0).getApiGroups(), is(expected.getRules().get(0).getApiGroups())));
        context.verify(() -> assertThat(actual.getRules().get(0).getResources(), is(expected.getRules().get(0).getResources())));
        context.verify(() -> assertThat(actual.getRules().get(0).getVerbs(), is(expected.getRules().get(0).getVerbs())));
    }
}
