/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.api.kafka.model.user.acl.AclOperation;
import io.strimzi.api.kafka.model.user.acl.AclResourcePatternType;
import io.strimzi.api.kafka.model.user.acl.AclRuleType;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.user.ResourceUtils;
import io.strimzi.operator.user.model.acl.SimpleAclRule;
import io.strimzi.operator.user.model.acl.SimpleAclRuleResource;
import io.strimzi.operator.user.model.acl.SimpleAclRuleResourceType;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateAclsResult;
import org.apache.kafka.clients.admin.DeleteAclsResult;
import org.apache.kafka.clients.admin.DescribeAclsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SimpleAclOperatorTest {
    private final static ExecutorService EXECUTOR = Executors.newSingleThreadExecutor();

    @Test
    public void testGetAllUsers() throws ExecutionException, InterruptedException {
        Admin mockAdminClient = mock(AdminClient.class);

        ResourcePattern res1 = new ResourcePattern(ResourceType.TOPIC, "my-topic", PatternType.LITERAL);
        ResourcePattern res2 = new ResourcePattern(ResourceType.GROUP, "my-group", PatternType.LITERAL);

        KafkaPrincipal foo = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "CN=foo");
        AclBinding fooAclBinding = new AclBinding(res1, new AccessControlEntry(foo.toString(), "*",
                org.apache.kafka.common.acl.AclOperation.READ, AclPermissionType.ALLOW));
        KafkaPrincipal bar = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "CN=bar");
        AclBinding barAclBinding = new AclBinding(res1, new AccessControlEntry(bar.toString(), "*",
                org.apache.kafka.common.acl.AclOperation.READ, AclPermissionType.ALLOW));
        KafkaPrincipal baz = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "baz");
        AclBinding bazAclBinding = new AclBinding(res2, new AccessControlEntry(baz.toString(), "*",
                org.apache.kafka.common.acl.AclOperation.READ, AclPermissionType.ALLOW));
        KafkaPrincipal all = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "*");
        AclBinding allAclBinding = new AclBinding(res1, new AccessControlEntry(all.toString(), "*",
                org.apache.kafka.common.acl.AclOperation.READ, AclPermissionType.ALLOW));
        KafkaPrincipal anonymous = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "ANONYMOUS");
        AclBinding anonymousAclBinding = new AclBinding(res2, new AccessControlEntry(anonymous.toString(), "*",
                org.apache.kafka.common.acl.AclOperation.READ, AclPermissionType.ALLOW));

        Collection<AclBinding> aclBindings =
                asList(fooAclBinding, barAclBinding, bazAclBinding, allAclBinding, anonymousAclBinding);

        assertDoesNotThrow(() -> mockDescribeAcls(mockAdminClient, AclBindingFilter.ANY, aclBindings));

        SimpleAclOperator aclOp = new SimpleAclOperator(mockAdminClient, ResourceUtils.createUserOperatorConfig(), EXECUTOR);
        aclOp.start();

        try {
            Set<String> users = aclOp.getAllUsers().toCompletableFuture().get();
            assertThat(users, is(new HashSet<>(asList("foo", "bar", "baz"))));
        } finally {
            aclOp.stop();
        }
    }

    @Test
    public void testReconcileInternalCreateAddsAclsToAuthorizer() throws ExecutionException, InterruptedException {
        Admin mockAdminClient = mock(AdminClient.class);

        ResourcePattern resource1 = new ResourcePattern(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL);
        ResourcePattern resource2 = new ResourcePattern(ResourceType.TOPIC, "my-topic", PatternType.LITERAL);

        KafkaPrincipal foo = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "CN=foo");
        AclBinding describeAclBinding = new AclBinding(resource1, new AccessControlEntry(foo.toString(), "*",
                org.apache.kafka.common.acl.AclOperation.DESCRIBE, AclPermissionType.ALLOW));
        AclBinding readAclBinding = new AclBinding(resource2, new AccessControlEntry(foo.toString(), "*",
                org.apache.kafka.common.acl.AclOperation.READ, AclPermissionType.ALLOW));
        AclBinding writeAclBinding = new AclBinding(resource2, new AccessControlEntry(foo.toString(), "*",
                org.apache.kafka.common.acl.AclOperation.WRITE, AclPermissionType.ALLOW));

        SimpleAclRuleResource ruleResource1 = new SimpleAclRuleResource("kafka-cluster", SimpleAclRuleResourceType.CLUSTER, AclResourcePatternType.LITERAL);
        SimpleAclRuleResource ruleResource2 = new SimpleAclRuleResource("my-topic", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.LITERAL);
        SimpleAclRule resource1DescribeRule = new SimpleAclRule(AclRuleType.ALLOW, ruleResource1, "*", AclOperation.DESCRIBE);
        SimpleAclRule resource2ReadRule = new SimpleAclRule(AclRuleType.ALLOW, ruleResource2, "*", AclOperation.READ);
        SimpleAclRule resource2WriteRule = new SimpleAclRule(AclRuleType.ALLOW, ruleResource2, "*", AclOperation.WRITE);

        ArgumentCaptor<Collection<AclBinding>> aclBindingsCaptor = ArgumentCaptor.forClass(Collection.class);
        assertDoesNotThrow(() -> {
            mockDescribeAcls(mockAdminClient, null, emptyList());
            mockCreateAcls(mockAdminClient, aclBindingsCaptor);
        });

        SimpleAclOperator aclOp = new SimpleAclOperator(mockAdminClient, ResourceUtils.createUserOperatorConfig(), EXECUTOR);
        aclOp.start();

        try {
            ReconcileResult<Set<SimpleAclRule>> result = aclOp.reconcile(Reconciliation.DUMMY_RECONCILIATION, "CN=foo", new LinkedHashSet<>(asList(resource2ReadRule, resource2WriteRule, resource1DescribeRule)))
                    .toCompletableFuture().get();

            assertThat(result, is(notNullValue()));
            assertThat(result.getType(), is(ReconcileResult.Type.CREATED));

            Collection<AclBinding> capturedAclBindings = aclBindingsCaptor.getValue();
            assertThat(capturedAclBindings, hasSize(3));
            assertThat(capturedAclBindings, hasItems(describeAclBinding, readAclBinding, writeAclBinding));

            Set<ResourcePattern> capturedResourcePatterns = capturedAclBindings.stream().map(AclBinding::pattern).collect(Collectors.toSet());
            assertThat(capturedResourcePatterns, hasSize(2));
            assertThat(capturedResourcePatterns, hasItems(resource1, resource2));
        } finally {
            aclOp.stop();
        }
    }

    @Test
    public void testReconcileInternalUpdateCreatesNewAclsAndDeletesOldAcls() throws ExecutionException, InterruptedException {
        Admin mockAdminClient = mock(AdminClient.class);

        ResourcePattern resource1 = new ResourcePattern(ResourceType.TOPIC, "my-topic", PatternType.LITERAL);
        ResourcePattern resource2 = new ResourcePattern(ResourceType.TOPIC, "my-topic2", PatternType.LITERAL);

        KafkaPrincipal foo = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "CN=foo");
        AclBinding readAclBinding = new AclBinding(resource1, new AccessControlEntry(foo.toString(), "*", org.apache.kafka.common.acl.AclOperation.READ, AclPermissionType.ALLOW));
        AclBinding writeAclBinding = new AclBinding(resource2, new AccessControlEntry(foo.toString(), "*", org.apache.kafka.common.acl.AclOperation.WRITE, AclPermissionType.ALLOW));

        SimpleAclRuleResource resource = new SimpleAclRuleResource("my-topic2", SimpleAclRuleResourceType.TOPIC, AclResourcePatternType.LITERAL);
        SimpleAclRule rule1 = new SimpleAclRule(AclRuleType.ALLOW, resource, "*", AclOperation.WRITE);

        ArgumentCaptor<Collection<AclBinding>> aclBindingsCaptor = ArgumentCaptor.forClass(Collection.class);
        ArgumentCaptor<Collection<AclBindingFilter>> aclBindingFiltersCaptor = ArgumentCaptor.forClass(Collection.class);
        assertDoesNotThrow(() -> {
            mockDescribeAcls(mockAdminClient, null, Collections.singleton(readAclBinding));
            mockCreateAcls(mockAdminClient, aclBindingsCaptor);
            mockDeleteAcls(mockAdminClient, Collections.singleton(readAclBinding), aclBindingFiltersCaptor);
        });

        SimpleAclOperator aclOp = new SimpleAclOperator(mockAdminClient, ResourceUtils.createUserOperatorConfig(), EXECUTOR);
        aclOp.start();

        try {
            ReconcileResult<Set<SimpleAclRule>> result = aclOp.reconcile(Reconciliation.DUMMY_RECONCILIATION, "CN=foo", Set.of(rule1))
                    .toCompletableFuture().get();

            assertThat(result, is(notNullValue()));
            assertThat(result.getType(), is(ReconcileResult.Type.PATCHED));

            // Create Write rule for resource 2
            Collection<AclBinding> capturedAclBindings = aclBindingsCaptor.getValue();
            assertThat(capturedAclBindings, hasSize(1));
            assertThat(capturedAclBindings, hasItem(writeAclBinding));
            Set<ResourcePattern> capturedResourcePatterns = capturedAclBindings.stream().map(AclBinding::pattern).collect(Collectors.toSet());
            assertThat(capturedResourcePatterns, hasSize(1));
            assertThat(capturedResourcePatterns, hasItem(resource2));

            // Delete read rule for resource 1
            Collection<AclBindingFilter> capturedAclBindingFilters = aclBindingFiltersCaptor.getValue();
            assertThat(capturedAclBindingFilters, hasSize(1));
            assertThat(capturedAclBindingFilters, hasItem(readAclBinding.toFilter()));

            Set<ResourcePatternFilter> capturedResourcePatternFilters = capturedAclBindingFilters.stream().map(AclBindingFilter::patternFilter).collect(Collectors.toSet());
            assertThat(capturedResourcePatternFilters, hasSize(1));
            assertThat(capturedResourcePatternFilters, hasItem(resource1.toFilter()));
        } finally {
            aclOp.stop();
        }
    }

    @Test
    public void testReconcileInternalDelete() throws ExecutionException, InterruptedException {
        Admin mockAdminClient = mock(AdminClient.class);

        ResourcePattern resource = new ResourcePattern(ResourceType.TOPIC, "my-topic", PatternType.LITERAL);

        KafkaPrincipal foo = new KafkaPrincipal("User", "CN=foo");
        AclBinding readAclBinding = new AclBinding(resource, new AccessControlEntry(foo.toString(), "*", org.apache.kafka.common.acl.AclOperation.READ, AclPermissionType.ALLOW));

        ArgumentCaptor<Collection<AclBindingFilter>> aclBindingFiltersCaptor = ArgumentCaptor.forClass(Collection.class);
        assertDoesNotThrow(() -> {
            mockDescribeAcls(mockAdminClient, null, Collections.singleton(readAclBinding));
            mockDeleteAcls(mockAdminClient, Collections.singleton(readAclBinding), aclBindingFiltersCaptor);
        });

        SimpleAclOperator aclOp = new SimpleAclOperator(mockAdminClient, ResourceUtils.createUserOperatorConfig(), EXECUTOR);
        aclOp.start();

        try {
            ReconcileResult<Set<SimpleAclRule>> result = aclOp.reconcile(Reconciliation.DUMMY_RECONCILIATION, "CN=foo", null)
                    .toCompletableFuture().get();

            assertThat(result, is(notNullValue()));
            assertThat(result.getType(), is(ReconcileResult.Type.DELETED));

            Collection<AclBindingFilter> capturedAclBindingFilters = aclBindingFiltersCaptor.getValue();
            assertThat(capturedAclBindingFilters, hasSize(1));
            assertThat(capturedAclBindingFilters, hasItem(readAclBinding.toFilter()));

            Set<ResourcePatternFilter> capturedResourcePatternFilters = capturedAclBindingFilters.stream().map(AclBindingFilter::patternFilter).collect(Collectors.toSet());
            assertThat(capturedResourcePatternFilters, hasSize(1));
            assertThat(capturedResourcePatternFilters, hasItem(resource.toFilter()));
        } finally {
            aclOp.stop();
        }
    }

    private void mockDescribeAcls(Admin mockAdminClient, AclBindingFilter aclBindingFilter, Collection<AclBinding> aclBindings) throws ExecutionException, InterruptedException, TimeoutException {
        DescribeAclsResult result = mock(DescribeAclsResult.class);
        KafkaFuture<Collection<AclBinding>> future = mock(KafkaFuture.class);
        when(future.get(anyLong(), any())).thenReturn(aclBindings);
        when(result.values()).thenReturn(future);
        when(mockAdminClient.describeAcls(aclBindingFilter != null ? aclBindingFilter : any())).thenReturn(result);
    }

    private void mockCreateAcls(Admin mockAdminClient, ArgumentCaptor<Collection<AclBinding>> aclBindingsCaptor) {
        CreateAclsResult result = mock(CreateAclsResult.class);
        KafkaFuture<Void> future = mock(KafkaFuture.class);
        when(future.toCompletionStage()).thenReturn(CompletableFuture.completedStage(null));
        when(result.all()).thenReturn(future);
        when(mockAdminClient.createAcls(aclBindingsCaptor.capture())).thenReturn(result);
    }

    private void mockDeleteAcls(Admin mockAdminClient, Collection<AclBinding> aclBindings, ArgumentCaptor<Collection<AclBindingFilter>> aclBindingFiltersCaptor) {
        DeleteAclsResult result = mock(DeleteAclsResult.class);
        KafkaFuture<Collection<AclBinding>> future = mock(KafkaFuture.class);
        when(future.toCompletionStage()).thenReturn(CompletableFuture.completedStage(aclBindings));
        when(result.all()).thenReturn(future);
        when(mockAdminClient.deleteAcls(aclBindingFiltersCaptor.capture())).thenReturn(result);
    }
}