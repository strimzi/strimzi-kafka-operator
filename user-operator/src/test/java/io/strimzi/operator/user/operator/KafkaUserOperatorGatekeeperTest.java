/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserList;
import io.strimzi.api.kafka.model.user.KafkaUserQuotas;
import io.strimzi.api.kafka.model.user.KafkaUserStatus;
import io.strimzi.certs.CertIssuer;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.gatekeeper.GatekeeperPluginFactory;
import io.strimzi.operator.common.operator.MockCertIssuer;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.kubernetes.CrdOperator;
import io.strimzi.operator.common.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.user.ResourceUtils;
import io.strimzi.operator.user.model.acl.SimpleAclRule;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaUserDeletionContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaUserEntryContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaUserExitContext;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaUserMutatingPlugin;
import io.strimzi.plugin.gatekeeper.GatekeeperKafkaUserValidatingPlugin;
import io.strimzi.plugin.gatekeeper.GatekeeperPluginConfigurationContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests focusing on the Gatekeeper integration in {@link KafkaUserOperator}. All the Kafka Admin API operators and the
 * Kubernetes operators are mocked to return already-completed stages, so the whole {@code reconcile(...)} chain runs
 * synchronously on the test thread. This is important because {@link GatekeeperPluginFactory#initializeForTests(List)}
 * stores the plugins in a thread-local field, which is only visible to the code running on the same thread. Keeping the
 * whole reconciliation on the test thread makes the injected test plugins visible during the entry, create/update and
 * exit phases.
 */
public class KafkaUserOperatorGatekeeperTest {
    private final CertIssuer mockCertIssuer = new MockCertIssuer();

    // The order in which the plugin hooks are invoked
    private final List<String> order = new ArrayList<>();

    private SecretOperator secretOps;
    @SuppressWarnings("unchecked")
    private final CrdOperator<KubernetesClient, KafkaUser, KafkaUserList> kafkaUserOps = mock(CrdOperator.class);

    private ScramCredentialsOperator scramOps;
    private QuotasOperator quotasOps;
    private ArgumentCaptor<KafkaUserQuotas> quotasCaptor;
    private SimpleAclOperator aclOps;

    @BeforeEach
    public void beforeEach() {
        mockKafkaOperators();
    }

    @AfterEach
    public void afterEach() {
        GatekeeperPluginFactory.clearTestPlugins();
    }

    private void mockKafkaOperators() {
        secretOps = mock(SecretOperator.class);
        when(secretOps.reconcile(any(), any(), any(), any(), any()))
                .thenAnswer(invocation -> CompletableFuture.completedStage(ReconcileResult.patched((Secret) invocation.getArgument(4))));
        when(secretOps.deleteAsync(any(), any(), any(), anyBoolean()))
                .thenReturn(CompletableFuture.completedStage(null));

        scramOps = mock(ScramCredentialsOperator.class);
        when(scramOps.reconcile(any(), any(), any())).thenAnswer(invocation -> {
            String desired = invocation.getArgument(2);
            return CompletableFuture.completedStage(desired == null ? ReconcileResult.deleted() : ReconcileResult.patched(desired));
        });

        quotasOps = mock(QuotasOperator.class);
        quotasCaptor = ArgumentCaptor.forClass(KafkaUserQuotas.class);
        when(quotasOps.reconcile(any(), any(), quotasCaptor.capture())).thenAnswer(invocation -> {
            KafkaUserQuotas desired = invocation.getArgument(2);
            return CompletableFuture.completedStage(desired == null ? ReconcileResult.deleted() : ReconcileResult.patched(desired));
        });

        aclOps = mock(SimpleAclOperator.class);
        when(aclOps.reconcile(any(), any(), any())).thenAnswer(invocation -> {
            Set<SimpleAclRule> desired = invocation.getArgument(2);
            return CompletableFuture.completedStage(desired == null ? ReconcileResult.deleted() : ReconcileResult.created(desired));
        });
    }

    private KafkaUserOperator operator() {
        return new KafkaUserOperator(ResourceUtils.createUserOperatorConfig(ResourceUtils.NAMESPACE), mockCertIssuer, secretOps, kafkaUserOps, scramOps, quotasOps, aclOps);
    }

    private static Reconciliation reconciliation() {
        return new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME);
    }

    // A plain SCRAM-SHA-512 user without any quotas or authorization. It keeps the reconciliation simple (no certificate
    // generation, no extra secrets to fetch) so the tests can focus on the Gatekeeper hooks.
    private static KafkaUser scramUser() {
        return new KafkaUserBuilder()
                .withNewMetadata()
                    .withName(ResourceUtils.NAME)
                    .withNamespace(ResourceUtils.NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafkaUserScramSha512ClientAuthentication()
                    .endKafkaUserScramSha512ClientAuthentication()
                .endSpec()
                .build();
    }

    @Test
    public void testEntryAndExitHooksInvokedOnCreateOrUpdate() {
        RecordingUserPlugin plugin = new RecordingUserPlugin();
        GatekeeperPluginFactory.initializeForTests(List.of(plugin));

        KafkaUserStatus status = operator()
                .reconcile(reconciliation(), scramUser(), null)
                .toCompletableFuture().join();

        assertThat(status, is(notNullValue()));
        assertThat(plugin.entryInvoked, is(true));
        assertThat(plugin.exitInvoked, is(true));
        assertThat(plugin.deletionInvoked, is(false));
        // The entry hook runs before the reconciliation, the exit hook after it
        assertThat(order, contains("entry", "exit"));
    }

    @Test
    public void testEntryMutationFlowsIntoReconciliation() {
        // The entry plugin adds a consumer byte rate quota to a user which originally has none. If the mutated resource
        // is really the one being reconciled, the quota operator must receive the added quota.
        GatekeeperPluginFactory.initializeForTests(List.of(new QuotaAddingEntryPlugin(4242)));

        operator()
                .reconcile(reconciliation(), scramUser(), null)
                .toCompletableFuture().join();

        // One of the quotas passed to the quota operator must carry the byte rate added by the entry plugin
        boolean quotaFromPluginReconciled = quotasCaptor.getAllValues().stream()
                .anyMatch(quotas -> quotas != null && Integer.valueOf(4242).equals(quotas.getConsumerByteRate()));
        assertThat(quotaFromPluginReconciled, is(true));
    }

    @Test
    public void testExitMutationReachesReturnedStatus() {
        // The exit plugin mutates the status. The mutated status must be the one returned by the reconciliation.
        GatekeeperPluginFactory.initializeForTests(List.of(new ObservedGenerationExitPlugin(1874L)));

        KafkaUserStatus status = operator()
                .reconcile(reconciliation(), scramUser(), null)
                .toCompletableFuture().join();

        assertThat(status.getObservedGeneration(), is(1874L));
    }

    @Test
    public void testEntryRejectionFailsReconciliationAndSkipsWork() {
        // The recording plugin is first, so its entry hook records the invocation. The rejecting validating plugin is
        // second and fails, which stops the chain before the reconciliation runs.
        RecordingUserPlugin recording = new RecordingUserPlugin();
        GatekeeperPluginFactory.initializeForTests(List.of(recording, new RejectingEntryPlugin()));

        CompletableFuture<KafkaUserStatus> result = operator()
                .reconcile(reconciliation(), scramUser(), null)
                .toCompletableFuture();

        assertThrows(CompletionException.class, result::join);

        // The entry hook of the recording plugin ran, but the reconciliation (and therefore the exit hook) did not
        assertThat(recording.entryInvoked, is(true));
        assertThat(recording.exitInvoked, is(false));
        // The actual reconciliation work was skipped because the entry plugin rejected the resource
        verify(quotasOps, never()).reconcile(any(), any(), any());
        verify(scramOps, never()).reconcile(any(), any(), any());
        verify(secretOps, never()).reconcile(any(), any(), any(), any(), any());
    }

    @Test
    public void testDeletionHookInvokedOnDeleteAndEntryExitSkipped() {
        RecordingUserPlugin plugin = new RecordingUserPlugin();
        GatekeeperPluginFactory.initializeForTests(List.of(plugin));

        // A null KafkaUser triggers the deletion path
        KafkaUserStatus status = operator()
                .reconcile(reconciliation(), null, null)
                .toCompletableFuture().join();

        assertThat(status, is(nullValue()));
        assertThat(plugin.deletionInvoked, is(true));
        assertThat(plugin.deletionNamespace, is(ResourceUtils.NAMESPACE));
        assertThat(plugin.deletionName, is(ResourceUtils.NAME));

        // The entry and exit hooks are only invoked for create/update, not for deletion
        assertThat(plugin.entryInvoked, is(false));
        assertThat(plugin.exitInvoked, is(false));
    }

    @Test
    public void testDeletionRejectionFailsReconciliation() {
        GatekeeperPluginFactory.initializeForTests(List.of(new RejectingDeletionPlugin()));

        CompletableFuture<KafkaUserStatus> result = operator()
                .reconcile(reconciliation(), null, null)
                .toCompletableFuture();

        assertThrows(CompletionException.class, result::join);
    }

    @Test
    public void testReconcileWithoutPluginsSucceeds() {
        // With no plugins configured, the reconciliation must behave exactly as before - the Gatekeeper hooks are pure
        // pass-throughs.
        GatekeeperPluginFactory.initializeForTests(List.of());

        KafkaUserStatus status = operator()
                .reconcile(reconciliation(), scramUser(), null)
                .toCompletableFuture().join();

        assertThat(status, is(notNullValue()));
        assertThat(status.getConditions(), hasSize(1));
        assertThat(status.getConditions().get(0).getType(), is("Ready"));
        assertThat(status.getConditions().get(0).getStatus(), is("True"));
    }

    /**
     * Records which hooks were invoked without mutating anything. Used to assert which phases fire for create/update
     * versus deletion.
     */
    private final class RecordingUserPlugin implements GatekeeperKafkaUserMutatingPlugin {
        private boolean entryInvoked;
        private boolean exitInvoked;
        private boolean deletionInvoked;
        private String deletionNamespace;
        private String deletionName;

        @Override
        public void configure(GatekeeperPluginConfigurationContext context) {
            // not used in these tests
        }

        @Override
        public CompletionStage<KafkaUser> kafkaUserEntry(GatekeeperKafkaUserEntryContext context, KafkaUser kafkaUser) {
            entryInvoked = true;
            order.add("entry");
            return CompletableFuture.completedFuture(kafkaUser);
        }

        @Override
        public CompletionStage<KafkaUserStatus> kafkaUserExit(GatekeeperKafkaUserExitContext context, KafkaUser kafkaUser, KafkaUserStatus newKafkaUserStatus) {
            exitInvoked = true;
            order.add("exit");
            return CompletableFuture.completedFuture(newKafkaUserStatus);
        }

        @Override
        public CompletionStage<Void> kafkaUserDeletion(GatekeeperKafkaUserDeletionContext context, String namespace, String name) {
            deletionInvoked = true;
            deletionNamespace = namespace;
            deletionName = name;
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Mutating entry plugin that adds a consumer byte rate quota to the KafkaUser. Used to verify that the mutation
     * flows into the reconciliation.
     */
    private static final class QuotaAddingEntryPlugin implements GatekeeperKafkaUserMutatingPlugin {
        private final int consumerByteRate;

        private QuotaAddingEntryPlugin(int consumerByteRate) {
            this.consumerByteRate = consumerByteRate;
        }

        @Override
        public CompletionStage<KafkaUser> kafkaUserEntry(GatekeeperKafkaUserEntryContext context, KafkaUser kafkaUser) {
            return CompletableFuture.completedFuture(new KafkaUserBuilder(kafkaUser)
                    .editSpec()
                        .withNewQuotas()
                            .withConsumerByteRate(consumerByteRate)
                        .endQuotas()
                    .endSpec()
                    .build());
        }
    }

    /**
     * Mutating exit plugin that sets the observed generation on the status.
     */
    private static final class ObservedGenerationExitPlugin implements GatekeeperKafkaUserMutatingPlugin {
        private final long observedGeneration;

        private ObservedGenerationExitPlugin(long observedGeneration) {
            this.observedGeneration = observedGeneration;
        }

        @Override
        public CompletionStage<KafkaUserStatus> kafkaUserExit(GatekeeperKafkaUserExitContext context, KafkaUser kafkaUser, KafkaUserStatus newKafkaUserStatus) {
            newKafkaUserStatus.setObservedGeneration(observedGeneration);
            return CompletableFuture.completedFuture(newKafkaUserStatus);
        }
    }

    /**
     * Validating entry plugin that rejects the reconciliation by completing exceptionally.
     */
    private static final class RejectingEntryPlugin implements GatekeeperKafkaUserValidatingPlugin {
        @Override
        public CompletionStage<Void> kafkaUserEntry(GatekeeperKafkaUserEntryContext context, KafkaUser kafkaUser) {
            return CompletableFuture.failedFuture(new RuntimeException("rejected by the entry plugin"));
        }
    }

    /**
     * Validating a deletion plugin that completes exceptionally
     */
    private static final class RejectingDeletionPlugin implements GatekeeperKafkaUserValidatingPlugin {
        @Override
        public CompletionStage<Void> kafkaUserDeletion(GatekeeperKafkaUserDeletionContext context, String namespace, String name) {
            return CompletableFuture.failedFuture(new RuntimeException("deletion gatekeeping failed"));
        }
    }
}
