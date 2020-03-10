/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.fabric8.kubernetes.api.model.LabelSelector;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.user.ResourceUtils;
import io.strimzi.operator.user.model.KafkaUserModel;
import io.strimzi.operator.user.model.acl.SimpleAclRule;
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

import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
public class KafkaUserOperatorTest {
    protected static Vertx vertx;
    private final CertManager mockCertManager = new MockCertManager();

    @BeforeAll
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterAll
    public static void after() {
        vertx.close();
    }

    @Test
    public void testCreateTlsUser(VertxTestContext context)    {
        CrdOperator mockCrdOps = mock(CrdOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        SimpleAclOperator aclOps = mock(SimpleAclOperator.class);
        ScramShaCredentialsOperator scramOps = mock(ScramShaCredentialsOperator.class);
        KafkaUserQuotasOperator quotasOps = mock(KafkaUserQuotasOperator.class);

        ArgumentCaptor<String> secretNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> secretNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Secret> secretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(secretNamespaceCaptor.capture(), secretNameCaptor.capture(), secretCaptor.capture())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<String> aclNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Set<SimpleAclRule>> aclRulesCaptor = ArgumentCaptor.forClass(Set.class);
        when(aclOps.reconcile(aclNameCaptor.capture(), aclRulesCaptor.capture())).thenReturn(Future.succeededFuture());

        when(scramOps.reconcile(any(), any())).thenReturn(Future.succeededFuture());
        when(quotasOps.reconcile(any(), any())).thenReturn(Future.succeededFuture());

        KafkaUserOperator op = new KafkaUserOperator(vertx, mockCertManager, mockCrdOps, Labels.EMPTY, mockSecretOps, scramOps, quotasOps, aclOps, ResourceUtils.CA_CERT_NAME, ResourceUtils.CA_KEY_NAME, ResourceUtils.NAMESPACE);
        KafkaUser user = ResourceUtils.createKafkaUserTls();
        Secret clientsCa = ResourceUtils.createClientsCaCertSecret();
        Secret clientsCaKey = ResourceUtils.createClientsCaKeySecret();
        when(mockSecretOps.get(anyString(), eq("user-cert"))).thenReturn(clientsCa);
        when(mockSecretOps.get(anyString(), eq("user-key"))).thenReturn(clientsCaKey);

        when(mockCrdOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(user));
        when(mockCrdOps.updateStatusAsync(any(KafkaUser.class))).thenReturn(Future.succeededFuture());

        Checkpoint async = context.checkpoint();
        op.createOrUpdate(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME), user).setHandler(res -> {
            context.verify(() -> assertThat(res.succeeded(), is(true)));

            List<String> capturedNames = secretNameCaptor.getAllValues();
            context.verify(() -> assertThat(capturedNames.size(), is(1)));
            context.verify(() -> assertThat(capturedNames.get(0), is(ResourceUtils.NAME)));

            List<String> capturedNamespaces = secretNamespaceCaptor.getAllValues();
            context.verify(() -> assertThat(capturedNamespaces.size(), is(1)));
            context.verify(() -> assertThat(capturedNamespaces.get(0), is(ResourceUtils.NAMESPACE)));

            List<Secret> capturedSecrets = secretCaptor.getAllValues();

            context.verify(() -> assertThat(capturedSecrets.size(), is(1)));

            Secret captured = capturedSecrets.get(0);
            context.verify(() -> assertThat(captured.getMetadata().getName(), is(user.getMetadata().getName())));
            context.verify(() -> assertThat(captured.getMetadata().getNamespace(), is(user.getMetadata().getNamespace())));
            context.verify(() -> assertThat(captured.getMetadata().getLabels(),
                    is(Labels.userLabels(user.getMetadata().getLabels())
                            .withKind(KafkaUser.RESOURCE_KIND)
                            .withKubernetesName()
                            .withKubernetesInstance(ResourceUtils.NAME)
                            .withKubernetesPartOf(ResourceUtils.NAME)
                            .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                            .toMap())));
            context.verify(() -> assertThat(new String(Base64.getDecoder().decode(captured.getData().get("ca.crt"))), is("clients-ca-crt")));
            context.verify(() -> assertThat(new String(Base64.getDecoder().decode(captured.getData().get("user.crt"))), is("crt file")));
            context.verify(() -> assertThat(new String(Base64.getDecoder().decode(captured.getData().get("user.key"))), is("key file")));

            List<String> capturedAclNames = aclNameCaptor.getAllValues();
            context.verify(() -> assertThat(capturedAclNames.size(), is(2)));
            context.verify(() -> assertThat(capturedAclNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME))));
            context.verify(() -> assertThat(capturedAclNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME))));

            List<Set<SimpleAclRule>> capturedAcls = aclRulesCaptor.getAllValues();

            context.verify(() -> assertThat(capturedAcls.size(), is(2)));
            Set<SimpleAclRule> aclRules = capturedAcls.get(0);

            context.verify(() -> assertThat(aclRules.size(), is(ResourceUtils.createExpectedSimpleAclRules(user).size())));
            context.verify(() -> assertThat(aclRules, is(ResourceUtils.createExpectedSimpleAclRules(user))));
            context.verify(() -> assertThat(capturedAcls.get(1), is(nullValue())));

            async.flag();
        });
    }

    @Test
    public void testUpdateUserNoChange(VertxTestContext context)    {
        CrdOperator mockCrdOps = mock(CrdOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        SimpleAclOperator aclOps = mock(SimpleAclOperator.class);
        ScramShaCredentialsOperator scramOps = mock(ScramShaCredentialsOperator.class);
        KafkaUserQuotasOperator quotasOps = mock(KafkaUserQuotasOperator.class);

        ArgumentCaptor<String> secretNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> secretNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Secret> secretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(secretNamespaceCaptor.capture(), secretNameCaptor.capture(), secretCaptor.capture())).thenReturn(Future.succeededFuture());

        when(scramOps.reconcile(any(), any())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> aclNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Set<SimpleAclRule>> aclRulesCaptor = ArgumentCaptor.forClass(Set.class);
        when(aclOps.reconcile(aclNameCaptor.capture(), aclRulesCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaUser user = ResourceUtils.createKafkaUserTls();

        when(mockCrdOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(user));
        when(mockCrdOps.updateStatusAsync(any(KafkaUser.class))).thenReturn(Future.succeededFuture());

        KafkaUserOperator op = new KafkaUserOperator(vertx, mockCertManager, mockCrdOps, Labels.EMPTY, mockSecretOps, scramOps, quotasOps, aclOps, ResourceUtils.CA_CERT_NAME, ResourceUtils.CA_KEY_NAME, ResourceUtils.NAMESPACE);
        Secret clientsCa = ResourceUtils.createClientsCaCertSecret();
        Secret clientsCaKey = ResourceUtils.createClientsCaKeySecret();
        Secret userCert = ResourceUtils.createUserSecretTls();
        when(mockSecretOps.get(anyString(), eq("user-cert"))).thenReturn(clientsCa);
        when(mockSecretOps.get(anyString(), eq("user-key"))).thenReturn(clientsCaKey);
        when(mockSecretOps.get(anyString(), eq(KafkaUserModel.getSecretName(user.getMetadata().getName())))).thenReturn(userCert);

        when(quotasOps.reconcile(any(), any())).thenReturn(Future.succeededFuture());

        Checkpoint async = context.checkpoint();
        op.createOrUpdate(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME), user).setHandler(res -> {
            context.verify(() -> assertThat(res.succeeded(), is(true)));

            List<String> capturedNames = secretNameCaptor.getAllValues();
            context.verify(() -> assertThat(capturedNames.size(), is(1)));
            context.verify(() -> assertThat(capturedNames.get(0), is(ResourceUtils.NAME)));

            List<String> capturedNamespaces = secretNamespaceCaptor.getAllValues();
            context.verify(() -> assertThat(capturedNamespaces.size(), is(1)));
            context.verify(() -> assertThat(capturedNamespaces.get(0), is(ResourceUtils.NAMESPACE)));

            List<Secret> capturedSecrets = secretCaptor.getAllValues();

            context.verify(() -> assertThat(capturedSecrets.size(), is(1)));

            Secret captured = capturedSecrets.get(0);
            context.verify(() -> assertThat(captured.getMetadata().getName(), is(userCert.getMetadata().getName())));
            context.verify(() -> assertThat(captured.getMetadata().getNamespace(), is(userCert.getMetadata().getNamespace())));
            context.verify(() -> assertThat(captured.getMetadata().getLabels(), is(userCert.getMetadata().getLabels())));
            context.verify(() -> assertThat(captured.getData().get("ca.crt"), is(userCert.getData().get("ca.crt"))));
            context.verify(() -> assertThat(captured.getData().get("user.crt"), is(userCert.getData().get("user.crt"))));
            context.verify(() -> assertThat(captured.getData().get("user.key"), is(userCert.getData().get("user.key"))));

            List<String> capturedAclNames = aclNameCaptor.getAllValues();
            context.verify(() -> assertThat(capturedAclNames.size(), is(2)));
            context.verify(() -> assertThat(capturedAclNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME))));
            context.verify(() -> assertThat(capturedAclNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME))));

            List<Set<SimpleAclRule>> capturedAcls = aclRulesCaptor.getAllValues();

            context.verify(() -> assertThat(capturedAcls.size(), is(2)));
            Set<SimpleAclRule> aclRules = capturedAcls.get(0);

            context.verify(() -> assertThat(aclRules.size(), is(ResourceUtils.createExpectedSimpleAclRules(user).size())));
            context.verify(() -> assertThat(aclRules, is(ResourceUtils.createExpectedSimpleAclRules(user))));
            context.verify(() -> assertThat(capturedAcls.get(1), is(nullValue())));

            async.flag();
        });
    }

    /**
     * Tests what happens when the TlsClisteAuth and SimpleAuthorization are suddenyl desiabled for the user (delete from the KafkaUser resource)
     *
     * @param context
     */
    @Test
    public void testUpdateUserNoAuthnAuthz(VertxTestContext context)    {
        CrdOperator mockCrdOps = mock(CrdOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        SimpleAclOperator aclOps = mock(SimpleAclOperator.class);
        ScramShaCredentialsOperator scramOps = mock(ScramShaCredentialsOperator.class);
        KafkaUserQuotasOperator quotasOps = mock(KafkaUserQuotasOperator.class);

        ArgumentCaptor<String> secretNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> secretNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Secret> secretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(secretNamespaceCaptor.capture(), secretNameCaptor.capture(), secretCaptor.capture())).thenReturn(Future.succeededFuture());

        when(scramOps.reconcile(any(), any())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> aclNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Set<SimpleAclRule>> aclRulesCaptor = ArgumentCaptor.forClass(Set.class);
        when(aclOps.reconcile(aclNameCaptor.capture(), aclRulesCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaUser user = ResourceUtils.createKafkaUserTls();
        user.getSpec().setAuthorization(null);
        user.getSpec().setAuthentication(null);

        when(mockCrdOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(user));
        when(mockCrdOps.updateStatusAsync(any(KafkaUser.class))).thenReturn(Future.succeededFuture());

        KafkaUserOperator op = new KafkaUserOperator(vertx, mockCertManager, mockCrdOps, Labels.EMPTY, mockSecretOps, scramOps, quotasOps, aclOps, ResourceUtils.CA_CERT_NAME, ResourceUtils.CA_KEY_NAME, ResourceUtils.NAMESPACE);
        Secret clientsCa = ResourceUtils.createClientsCaCertSecret();
        Secret clientsCaKey = ResourceUtils.createClientsCaKeySecret();
        Secret userCert = ResourceUtils.createUserSecretTls();

        when(quotasOps.reconcile(any(), any())).thenReturn(Future.succeededFuture());

        Checkpoint async = context.checkpoint();
        op.createOrUpdate(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME), user).setHandler(res -> {
            context.verify(() -> assertThat(res.succeeded(), is(true)));

            List<String> capturedNames = secretNameCaptor.getAllValues();
            context.verify(() -> assertThat(capturedNames.size(), is(1)));
            context.verify(() -> assertThat(capturedNames.get(0), is(ResourceUtils.NAME)));

            List<String> capturedNamespaces = secretNamespaceCaptor.getAllValues();
            context.verify(() -> assertThat(capturedNamespaces.size(), is(1)));
            context.verify(() -> assertThat(capturedNamespaces.get(0), is(ResourceUtils.NAMESPACE)));

            List<Secret> capturedSecrets = secretCaptor.getAllValues();

            context.verify(() -> assertThat(capturedSecrets.size(), is(1)));

            Secret captured = capturedSecrets.get(0);
            context.verify(() -> assertThat(captured, is(nullValue())));

            List<String> capturedAclNames = aclNameCaptor.getAllValues();
            context.verify(() -> assertThat(capturedAclNames.size(), is(2)));
            context.verify(() -> assertThat(capturedAclNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME))));
            context.verify(() -> assertThat(capturedAclNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME))));

            List<Set<SimpleAclRule>> capturedAcls = aclRulesCaptor.getAllValues();

            context.verify(() -> assertThat(capturedAcls.size(), is(2)));
            context.verify(() -> assertThat(capturedAcls.get(0), is(nullValue())));
            context.verify(() -> assertThat(capturedAcls.get(1), is(nullValue())));

            async.flag();
        });
    }

    @Test
    public void testUpdateUserNewCert(VertxTestContext context)    {
        CrdOperator mockCrdOps = mock(CrdOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        SimpleAclOperator aclOps = mock(SimpleAclOperator.class);
        ScramShaCredentialsOperator scramOps = mock(ScramShaCredentialsOperator.class);
        KafkaUserQuotasOperator quotasOps = mock(KafkaUserQuotasOperator.class);

        ArgumentCaptor<String> secretNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> secretNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Secret> secretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(secretNamespaceCaptor.capture(), secretNameCaptor.capture(), secretCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> aclNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Set<SimpleAclRule>> aclRulesCaptor = ArgumentCaptor.forClass(Set.class);
        when(aclOps.reconcile(aclNameCaptor.capture(), aclRulesCaptor.capture())).thenReturn(Future.succeededFuture());

        when(scramOps.reconcile(any(), any())).thenReturn(Future.succeededFuture());
        when(quotasOps.reconcile(any(), any())).thenReturn(Future.succeededFuture());
        when(mockCrdOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture());

        KafkaUserOperator op = new KafkaUserOperator(vertx, mockCertManager, mockCrdOps, Labels.EMPTY, mockSecretOps, scramOps, quotasOps, aclOps, ResourceUtils.CA_CERT_NAME, ResourceUtils.CA_KEY_NAME, ResourceUtils.NAMESPACE);
        KafkaUser user = ResourceUtils.createKafkaUserTls();

        Secret clientsCa = ResourceUtils.createClientsCaCertSecret();
        clientsCa.getData().put("ca.crt", Base64.getEncoder().encodeToString("different-clients-ca-crt".getBytes()));
        Secret clientsCaKey = ResourceUtils.createClientsCaKeySecret();
        clientsCaKey.getData().put("ca.key", Base64.getEncoder().encodeToString("different-clients-ca-key".getBytes()));
        Secret userCert = ResourceUtils.createUserSecretTls();

        when(mockSecretOps.get(anyString(), eq("user-cert"))).thenReturn(clientsCa);
        when(mockSecretOps.get(anyString(), eq("user-key"))).thenReturn(clientsCaKey);
        when(mockSecretOps.get(anyString(), eq(KafkaUserModel.getSecretName(user.getMetadata().getName())))).thenReturn(userCert);

        when(mockCrdOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(user));
        when(mockCrdOps.updateStatusAsync(any(KafkaUser.class))).thenReturn(Future.succeededFuture());

        Checkpoint async = context.checkpoint();
        op.createOrUpdate(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME), user).setHandler(res -> {
            context.verify(() -> assertThat(res.succeeded(), is(true)));

            List<String> capturedNames = secretNameCaptor.getAllValues();
            context.verify(() -> assertThat(capturedNames.size(), is(1)));
            context.verify(() -> assertThat(capturedNames.get(0), is(ResourceUtils.NAME)));

            List<String> capturedNamespaces = secretNamespaceCaptor.getAllValues();
            context.verify(() -> assertThat(capturedNamespaces.size(), is(1)));
            context.verify(() -> assertThat(capturedNamespaces.get(0), is(ResourceUtils.NAMESPACE)));

            List<Secret> capturedSecrets = secretCaptor.getAllValues();

            context.verify(() -> assertThat(capturedSecrets.size(), is(1)));

            Secret captured = capturedSecrets.get(0);
            context.verify(() -> assertThat(captured.getMetadata().getName(), is(userCert.getMetadata().getName())));
            context.verify(() -> assertThat(captured.getMetadata().getNamespace(), is(userCert.getMetadata().getNamespace())));
            context.verify(() -> assertThat(captured.getMetadata().getLabels(), is(userCert.getMetadata().getLabels())));
            context.verify(() -> assertThat(new String(Base64.getDecoder().decode(captured.getData().get("ca.crt"))), is("different-clients-ca-crt")));
            context.verify(() -> assertThat(new String(Base64.getDecoder().decode(captured.getData().get("user.crt"))), is("crt file")));
            context.verify(() -> assertThat(new String(Base64.getDecoder().decode(captured.getData().get("user.key"))), is("key file")));

            async.flag();
        });
    }

    @Test
    public void testDeleteTlsUser(VertxTestContext context)    {
        CrdOperator mockCrdOps = mock(CrdOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        SimpleAclOperator aclOps = mock(SimpleAclOperator.class);
        ScramShaCredentialsOperator scramOps = mock(ScramShaCredentialsOperator.class);
        KafkaUserQuotasOperator quotasOps = mock(KafkaUserQuotasOperator.class);

        ArgumentCaptor<String> secretNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> secretNameCaptor = ArgumentCaptor.forClass(String.class);
        when(mockSecretOps.reconcile(secretNamespaceCaptor.capture(), secretNameCaptor.capture(), isNull())).thenReturn(Future.succeededFuture());

        when(scramOps.reconcile(any(), any())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> aclNameCaptor = ArgumentCaptor.forClass(String.class);
        when(aclOps.reconcile(aclNameCaptor.capture(), isNull())).thenReturn(Future.succeededFuture());

        when(quotasOps.reconcile(anyString(), eq(null))).thenReturn(Future.succeededFuture());

        KafkaUserOperator op = new KafkaUserOperator(vertx, mockCertManager, mockCrdOps, Labels.EMPTY, mockSecretOps, scramOps, quotasOps, aclOps, ResourceUtils.CA_CERT_NAME, ResourceUtils.CA_KEY_NAME, ResourceUtils.NAMESPACE);

        Checkpoint async = context.checkpoint();
        op.delete(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME)).setHandler(res -> {
            context.verify(() -> assertThat(res.succeeded(), is(true)));

            List<String> capturedNames = secretNameCaptor.getAllValues();
            context.verify(() -> assertThat(capturedNames.size(), is(1)));
            context.verify(() -> assertThat(capturedNames.get(0), is(ResourceUtils.NAME)));

            List<String> capturedNamespaces = secretNamespaceCaptor.getAllValues();
            context.verify(() -> assertThat(capturedNamespaces.size(), is(1)));
            context.verify(() -> assertThat(capturedNamespaces.get(0), is(ResourceUtils.NAMESPACE)));

            List<String> capturedAclNames = aclNameCaptor.getAllValues();
            context.verify(() -> assertThat(capturedAclNames.size(), is(2)));
            context.verify(() -> assertThat(capturedAclNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME))));
            context.verify(() -> assertThat(capturedAclNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME))));

            async.flag();
        });
    }

    @Test
    public void testReconcileNewTlsUser(VertxTestContext context)    {
        CrdOperator mockCrdOps = mock(CrdOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        SimpleAclOperator aclOps = mock(SimpleAclOperator.class);
        ScramShaCredentialsOperator scramOps = mock(ScramShaCredentialsOperator.class);
        KafkaUserQuotasOperator quotasOps = mock(KafkaUserQuotasOperator.class);

        KafkaUserOperator op = new KafkaUserOperator(vertx, mockCertManager, mockCrdOps, Labels.EMPTY, mockSecretOps, scramOps, quotasOps, aclOps,
                ResourceUtils.CA_CERT_NAME, ResourceUtils.CA_KEY_NAME, ResourceUtils.NAMESPACE);
        KafkaUser user = ResourceUtils.createKafkaUserTls();
        Secret clientsCa = ResourceUtils.createClientsCaCertSecret();
        Secret clientsCaKey = ResourceUtils.createClientsCaKeySecret();

        ArgumentCaptor<String> secretNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> secretNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Secret> secretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(secretNamespaceCaptor.capture(), secretNameCaptor.capture(), secretCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> aclNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Set<SimpleAclRule>> aclRulesCaptor = ArgumentCaptor.forClass(Set.class);
        when(aclOps.reconcile(aclNameCaptor.capture(), aclRulesCaptor.capture())).thenReturn(Future.succeededFuture());

        when(scramOps.reconcile(any(), any())).thenReturn(Future.succeededFuture());

        when(mockSecretOps.get(eq(clientsCa.getMetadata().getNamespace()), eq(ResourceUtils.CA_CERT_NAME))).thenReturn(clientsCa);
        when(mockSecretOps.get(eq(clientsCa.getMetadata().getNamespace()), eq(ResourceUtils.CA_KEY_NAME))).thenReturn(clientsCaKey);
        when(mockSecretOps.get(eq(user.getMetadata().getNamespace()), eq(user.getMetadata().getName()))).thenReturn(null);

        when(mockCrdOps.get(eq(user.getMetadata().getNamespace()), eq(user.getMetadata().getName()))).thenReturn(user);
        when(mockCrdOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(user));
        when(mockCrdOps.updateStatusAsync(any(KafkaUser.class))).thenReturn(Future.succeededFuture());
        when(quotasOps.reconcile(any(), any())).thenReturn(Future.succeededFuture());

        Checkpoint async = context.checkpoint();
        op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME)).setHandler(res -> {
            context.verify(() -> assertThat(res.succeeded(), is(true)));

            List<String> capturedNames = secretNameCaptor.getAllValues();
            context.verify(() -> assertThat(capturedNames.size(), is(1)));
            context.verify(() -> assertThat(capturedNames.get(0), is(ResourceUtils.NAME)));

            List<String> capturedNamespaces = secretNamespaceCaptor.getAllValues();
            context.verify(() -> assertThat(capturedNamespaces.size(), is(1)));
            context.verify(() -> assertThat(capturedNamespaces.get(0), is(ResourceUtils.NAMESPACE)));

            List<Secret> capturedSecrets = secretCaptor.getAllValues();

            context.verify(() -> assertThat(capturedSecrets.size(), is(1)));

            Secret captured = capturedSecrets.get(0);
            context.verify(() -> assertThat(captured.getMetadata().getName(), is(user.getMetadata().getName())));
            context.verify(() -> assertThat(captured.getMetadata().getNamespace(), is(user.getMetadata().getNamespace())));
            context.verify(() -> assertThat(captured.getMetadata().getLabels(),
                            is(Labels.userLabels(user.getMetadata().getLabels())
                            .withKind(KafkaUser.RESOURCE_KIND)
                            .withKubernetesName()
                            .withKubernetesInstance(ResourceUtils.NAME)
                            .withKubernetesPartOf(ResourceUtils.NAME)
                            .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                            .toMap())));
            context.verify(() -> assertThat(new String(Base64.getDecoder().decode(captured.getData().get("ca.crt"))), is("clients-ca-crt")));
            context.verify(() -> assertThat(new String(Base64.getDecoder().decode(captured.getData().get("user.crt"))), is("crt file")));
            context.verify(() -> assertThat(new String(Base64.getDecoder().decode(captured.getData().get("user.key"))), is("key file")));


            List<String> capturedAclNames = aclNameCaptor.getAllValues();
            context.verify(() -> assertThat(capturedAclNames.size(), is(2)));
            context.verify(() -> assertThat(capturedAclNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME))));
            context.verify(() -> assertThat(capturedAclNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME))));

            List<Set<SimpleAclRule>> capturedAcls = aclRulesCaptor.getAllValues();

            context.verify(() -> assertThat(capturedAcls.size(), is(2)));
            Set<SimpleAclRule> aclRules = capturedAcls.get(0);

            context.verify(() -> assertThat(aclRules.size(), is(ResourceUtils.createExpectedSimpleAclRules(user).size())));
            context.verify(() -> assertThat(aclRules, is(ResourceUtils.createExpectedSimpleAclRules(user))));
            context.verify(() -> assertThat(capturedAcls.get(1), is(nullValue())));

            async.flag();
        });
    }

    @Test
    public void testReconcileExistingTlsUser(VertxTestContext context)    {
        CrdOperator mockCrdOps = mock(CrdOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        SimpleAclOperator aclOps = mock(SimpleAclOperator.class);
        ScramShaCredentialsOperator scramOps = mock(ScramShaCredentialsOperator.class);
        KafkaUserQuotasOperator quotasOps = mock(KafkaUserQuotasOperator.class);

        KafkaUserOperator op = new KafkaUserOperator(vertx, mockCertManager, mockCrdOps, Labels.EMPTY, mockSecretOps, scramOps, quotasOps, aclOps, ResourceUtils.CA_CERT_NAME, ResourceUtils.CA_KEY_NAME, ResourceUtils.NAMESPACE);
        KafkaUser user = ResourceUtils.createKafkaUserTls();
        Secret clientsCa = ResourceUtils.createClientsCaCertSecret();
        Secret clientsCaKey = ResourceUtils.createClientsCaKeySecret();
        Secret userCert = ResourceUtils.createUserSecretTls();

        ArgumentCaptor<String> secretNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> secretNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Secret> secretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(secretNamespaceCaptor.capture(), secretNameCaptor.capture(), secretCaptor.capture())).thenReturn(Future.succeededFuture());

        when(scramOps.reconcile(any(), any())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> aclNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Set<SimpleAclRule>> aclRulesCaptor = ArgumentCaptor.forClass(Set.class);
        when(aclOps.reconcile(aclNameCaptor.capture(), aclRulesCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockSecretOps.get(eq(clientsCa.getMetadata().getNamespace()), eq(clientsCa.getMetadata().getName()))).thenReturn(clientsCa);
        when(mockSecretOps.get(eq(clientsCa.getMetadata().getNamespace()), eq(clientsCaKey.getMetadata().getName()))).thenReturn(clientsCaKey);
        when(mockSecretOps.get(eq(user.getMetadata().getNamespace()), eq(user.getMetadata().getName()))).thenReturn(userCert);

        when(mockCrdOps.get(eq(user.getMetadata().getNamespace()), eq(user.getMetadata().getName()))).thenReturn(user);
        when(mockCrdOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(user));
        when(mockCrdOps.updateStatusAsync(any(KafkaUser.class))).thenReturn(Future.succeededFuture());
        when(quotasOps.reconcile(any(), any())).thenReturn(Future.succeededFuture());

        Checkpoint async = context.checkpoint();
        op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME)).setHandler(res -> {
            context.verify(() -> assertThat(res.succeeded(), is(true)));

            List<String> capturedNames = secretNameCaptor.getAllValues();
            context.verify(() -> assertThat(capturedNames.size(), is(1)));
            context.verify(() -> assertThat(ResourceUtils.NAME, is(capturedNames.get(0))));

            List<String> capturedNamespaces = secretNamespaceCaptor.getAllValues();
            context.verify(() -> assertThat(capturedNamespaces.size(), is(1)));
            context.verify(() -> assertThat(capturedNamespaces.get(0), is(ResourceUtils.NAMESPACE)));

            List<Secret> capturedSecrets = secretCaptor.getAllValues();

            context.verify(() -> assertThat(capturedSecrets.size(), is(1)));

            Secret captured = capturedSecrets.get(0);
            context.verify(() -> assertThat(captured.getMetadata().getName(), is(user.getMetadata().getName())));
            context.verify(() -> assertThat(captured.getMetadata().getNamespace(), is(user.getMetadata().getNamespace())));
            context.verify(() -> assertThat(captured.getMetadata().getLabels(),
                    is(Labels.userLabels(user.getMetadata().getLabels())
                            .withKubernetesName()
                            .withKubernetesInstance(ResourceUtils.NAME)
                            .withKubernetesPartOf(ResourceUtils.NAME)
                            .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                            .withKind(KafkaUser.RESOURCE_KIND)
                            .toMap())));
            context.verify(() -> assertThat(captured.getData().get("ca.crt"), is(userCert.getData().get("ca.crt"))));
            context.verify(() -> assertThat(captured.getData().get("user.crt"), is(userCert.getData().get("user.crt"))));
            context.verify(() -> assertThat(captured.getData().get("user.key"), is(userCert.getData().get("user.key"))));

            List<String> capturedAclNames = aclNameCaptor.getAllValues();
            context.verify(() -> assertThat(capturedAclNames.size(), is(2)));
            context.verify(() -> assertThat(capturedAclNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME))));
            context.verify(() -> assertThat(capturedAclNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME))));

            List<Set<SimpleAclRule>> capturedAcls = aclRulesCaptor.getAllValues();

            context.verify(() -> assertThat(capturedAcls.size(), is(2)));
            Set<SimpleAclRule> aclRules = capturedAcls.get(0);

            context.verify(() -> assertThat(aclRules.size(), is(ResourceUtils.createExpectedSimpleAclRules(user).size())));
            context.verify(() -> assertThat(aclRules, is(ResourceUtils.createExpectedSimpleAclRules(user))));
            context.verify(() -> assertThat(capturedAcls.get(1), is(nullValue())));

            async.flag();
        });
    }

    @Test
    public void testReconcileDeleteTlsUser(VertxTestContext context)    {
        CrdOperator mockCrdOps = mock(CrdOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        SimpleAclOperator aclOps = mock(SimpleAclOperator.class);
        ScramShaCredentialsOperator scramOps = mock(ScramShaCredentialsOperator.class);
        KafkaUserQuotasOperator quotasOps = mock(KafkaUserQuotasOperator.class);

        KafkaUserOperator op = new KafkaUserOperator(vertx, mockCertManager, mockCrdOps, Labels.EMPTY, mockSecretOps, scramOps, quotasOps, aclOps, ResourceUtils.CA_CERT_NAME, ResourceUtils.CA_KEY_NAME, ResourceUtils.NAMESPACE);
        KafkaUser user = ResourceUtils.createKafkaUserTls();
        Secret clientsCa = ResourceUtils.createClientsCaCertSecret();
        Secret userCert = ResourceUtils.createUserSecretTls();

        ArgumentCaptor<String> secretNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> secretNameCaptor = ArgumentCaptor.forClass(String.class);
        when(mockSecretOps.reconcile(secretNamespaceCaptor.capture(), secretNameCaptor.capture(), isNull())).thenReturn(Future.succeededFuture());

        when(scramOps.reconcile(any(), any())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> aclNameCaptor = ArgumentCaptor.forClass(String.class);
        when(aclOps.reconcile(aclNameCaptor.capture(), isNull())).thenReturn(Future.succeededFuture());

        when(mockSecretOps.get(eq(clientsCa.getMetadata().getNamespace()), eq(clientsCa.getMetadata().getName()))).thenReturn(clientsCa);
        when(mockSecretOps.get(eq(user.getMetadata().getNamespace()), eq(user.getMetadata().getName()))).thenReturn(userCert);

        when(mockCrdOps.get(eq(user.getMetadata().getNamespace()), eq(user.getMetadata().getName()))).thenReturn(null);

        when(quotasOps.reconcile(anyString(), eq(null))).thenReturn(Future.succeededFuture());

        Checkpoint async = context.checkpoint();
        op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME)).setHandler(res -> {
            context.verify(() -> assertThat(res.succeeded(), is(true)));

            List<String> capturedNames = secretNameCaptor.getAllValues();
            context.verify(() -> assertThat(capturedNames.size(), is(1)));
            context.verify(() -> assertThat(capturedNames.get(0), is(ResourceUtils.NAME)));

            List<String> capturedNamespaces = secretNamespaceCaptor.getAllValues();
            context.verify(() -> assertThat(capturedNamespaces.size(), is(1)));
            context.verify(() -> assertThat(capturedNamespaces.get(0), is(ResourceUtils.NAMESPACE)));

            List<String> capturedAclNames = aclNameCaptor.getAllValues();
            context.verify(() -> assertThat(capturedAclNames.size(), is(2)));
            context.verify(() -> assertThat(capturedAclNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME))));
            context.verify(() -> assertThat(capturedAclNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME))));

            async.flag();
        });
    }

    @Test
    public void testReconcileAll(VertxTestContext context) throws InterruptedException {
        CrdOperator mockCrdOps = mock(CrdOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        SimpleAclOperator aclOps = mock(SimpleAclOperator.class);
        ScramShaCredentialsOperator scramOps = mock(ScramShaCredentialsOperator.class);
        KafkaUserQuotasOperator quotasOps = mock(KafkaUserQuotasOperator.class);

        KafkaUser newTlsUser = ResourceUtils.createKafkaUserTls();
        newTlsUser.getMetadata().setName("new-tls-user");
        KafkaUser newScramShaUser = ResourceUtils.createKafkaUserScramSha();
        newScramShaUser.getMetadata().setName("new-scram-sha-user");
        KafkaUser existingTlsUser = ResourceUtils.createKafkaUserTls();
        existingTlsUser.getMetadata().setName("existing-tls-user");
        Secret clientsCa = ResourceUtils.createClientsCaCertSecret();
        Secret existingTlsUserSecret = ResourceUtils.createUserSecretTls();
        existingTlsUserSecret.getMetadata().setName("existing-tls-user");
        Secret existingScramShaUserSecret = ResourceUtils.createUserSecretScramSha();
        existingScramShaUserSecret.getMetadata().setName("existing-scram-sha-user");
        KafkaUser existingScramShaUser = ResourceUtils.createKafkaUserTls();
        existingScramShaUser.getMetadata().setName("existing-scram-sha-user");

        when(mockCrdOps.listAsync(eq(ResourceUtils.NAMESPACE), eq(Optional.of(new LabelSelector(null, Labels.userLabels(ResourceUtils.LABELS).toMap()))))).thenReturn(
                Future.succeededFuture(Arrays.asList(newTlsUser, newScramShaUser, existingTlsUser, existingScramShaUser)));
        when(mockSecretOps.list(eq(ResourceUtils.NAMESPACE), eq(Labels.userLabels(ResourceUtils.LABELS).withKind(KafkaUser.RESOURCE_KIND)))).thenReturn(Arrays.asList(existingTlsUserSecret, existingScramShaUserSecret));
        when(aclOps.getUsersWithAcls()).thenReturn(new HashSet<String>(Arrays.asList("existing-tls-user", "second-deleted-user")));
        when(scramOps.list()).thenReturn(asList("existing-tls-user", "deleted-scram-sha-user"));

        when(mockCrdOps.get(eq(newTlsUser.getMetadata().getNamespace()), eq(newTlsUser.getMetadata().getName()))).thenReturn(newTlsUser);
        when(mockCrdOps.get(eq(newScramShaUser.getMetadata().getNamespace()), eq(newScramShaUser.getMetadata().getName()))).thenReturn(newScramShaUser);
        when(mockCrdOps.get(eq(existingTlsUser.getMetadata().getNamespace()), eq(existingTlsUser.getMetadata().getName()))).thenReturn(existingTlsUser);
        when(mockCrdOps.get(eq(existingTlsUser.getMetadata().getNamespace()), eq(existingScramShaUser.getMetadata().getName()))).thenReturn(existingScramShaUser);
        when(mockSecretOps.get(eq(clientsCa.getMetadata().getNamespace()), eq(clientsCa.getMetadata().getName()))).thenReturn(clientsCa);
        when(mockSecretOps.get(eq(newTlsUser.getMetadata().getNamespace()), eq(newTlsUser.getMetadata().getName()))).thenReturn(null);
        when(mockSecretOps.get(eq(newScramShaUser.getMetadata().getNamespace()), eq(newScramShaUser.getMetadata().getName()))).thenReturn(null);
        when(mockSecretOps.get(eq(existingTlsUser.getMetadata().getNamespace()), eq(existingTlsUser.getMetadata().getName()))).thenReturn(existingTlsUserSecret);
        when(mockSecretOps.get(eq(existingScramShaUser.getMetadata().getNamespace()), eq(existingScramShaUser.getMetadata().getName()))).thenReturn(existingScramShaUserSecret);

        Set<String> createdOrUpdated = new CopyOnWriteArraySet<>();
        Set<String> deleted = new CopyOnWriteArraySet<>();

        CountDownLatch async = new CountDownLatch(6);
        KafkaUserOperator op = new KafkaUserOperator(vertx,
                mockCertManager,
                mockCrdOps,
                Labels.userLabels(ResourceUtils.LABELS),
                mockSecretOps, scramOps, quotasOps,
                aclOps, ResourceUtils.CA_CERT_NAME, ResourceUtils.CA_KEY_NAME, ResourceUtils.NAMESPACE) {

            @Override
            public Future<Void> createOrUpdate(Reconciliation reconciliation, KafkaUser resource) {
                Future<Void> h = Future.future();
                createdOrUpdated.add(resource.getMetadata().getName());
                async.countDown();
                h.handle(Future.succeededFuture());
                return h;
            }
            @Override
            public Future<Boolean> delete(Reconciliation reconciliation) {
                deleted.add(reconciliation.name());
                async.countDown();
                return Future.succeededFuture(Boolean.TRUE);
            }
        };

        // Now try to reconcile all the Kafka Connect clusters
        op.reconcileAll("test", ResourceUtils.NAMESPACE, ignored -> { });

        async.await();

        context.verify(() -> assertThat(createdOrUpdated, is(new HashSet(asList("new-tls-user", "existing-tls-user",
                "new-scram-sha-user", "existing-scram-sha-user")))));
        context.verify(() -> assertThat(deleted, is(new HashSet(asList("second-deleted-user", "deleted-scram-sha-user")))));

        context.completeNow();
    }

    @Test
    public void testReconcileNewScramShaUser(VertxTestContext context)    {
        CrdOperator mockCrdOps = mock(CrdOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        SimpleAclOperator aclOps = mock(SimpleAclOperator.class);
        ScramShaCredentialsOperator scramOps = mock(ScramShaCredentialsOperator.class);
        KafkaUserQuotasOperator quotasOps = mock(KafkaUserQuotasOperator.class);

        KafkaUserOperator op = new KafkaUserOperator(vertx, mockCertManager, mockCrdOps, Labels.EMPTY, mockSecretOps, scramOps, quotasOps, aclOps, ResourceUtils.CA_CERT_NAME, ResourceUtils.CA_KEY_NAME, ResourceUtils.NAMESPACE);
        KafkaUser user = ResourceUtils.createKafkaUserScramSha();

        ArgumentCaptor<String> secretNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> secretNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Secret> secretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(secretNamespaceCaptor.capture(), secretNameCaptor.capture(), secretCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> aclNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Set<SimpleAclRule>> aclRulesCaptor = ArgumentCaptor.forClass(Set.class);
        when(aclOps.reconcile(aclNameCaptor.capture(), aclRulesCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> scramUserCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> scramPasswordCaptor = ArgumentCaptor.forClass(String.class);
        when(scramOps.reconcile(scramUserCaptor.capture(), scramPasswordCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockSecretOps.get(eq(user.getMetadata().getNamespace()), eq(user.getMetadata().getName()))).thenReturn(null);

        when(mockCrdOps.get(eq(user.getMetadata().getNamespace()), eq(user.getMetadata().getName()))).thenReturn(user);
        when(mockCrdOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(user));
        when(mockCrdOps.updateStatusAsync(any(KafkaUser.class))).thenReturn(Future.succeededFuture());
        when(quotasOps.reconcile(any(), any())).thenReturn(Future.succeededFuture());

        Checkpoint async = context.checkpoint();
        op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME)).setHandler(res -> {
            context.verify(() -> assertThat(res.succeeded(), is(true)));

            List<String> capturedNames = secretNameCaptor.getAllValues();
            context.verify(() -> assertThat(capturedNames.size(), is(1)));
            context.verify(() -> assertThat(capturedNames.get(0), is(ResourceUtils.NAME)));

            List<String> capturedNamespaces = secretNamespaceCaptor.getAllValues();
            context.verify(() -> assertThat(capturedNamespaces.size(), is(1)));
            context.verify(() -> assertThat(capturedNamespaces.get(0), is(ResourceUtils.NAMESPACE)));

            List<Secret> capturedSecrets = secretCaptor.getAllValues();

            context.verify(() -> assertThat(capturedSecrets.size(), is(1)));

            Secret captured = capturedSecrets.get(0);
            context.verify(() -> assertThat(captured.getMetadata().getName(), is(user.getMetadata().getName())));
            context.verify(() -> assertThat(captured.getMetadata().getNamespace(), is(user.getMetadata().getNamespace())));
            context.verify(() -> assertThat(captured.getMetadata().getLabels(),
                    is(Labels.userLabels(user.getMetadata().getLabels())
                            .withKubernetesName()
                            .withKubernetesInstance(ResourceUtils.NAME)
                            .withKubernetesPartOf(ResourceUtils.NAME)
                            .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                            .withKind(KafkaUser.RESOURCE_KIND)
                            .toMap())));

            context.verify(() -> assertThat(scramPasswordCaptor.getValue(), is(new String(Base64.getDecoder().decode(captured.getData().get(KafkaUserModel.KEY_PASSWORD))))));
            context.verify(() -> assertThat(new String(Base64.getDecoder().decode(captured.getData().get(KafkaUserModel.KEY_PASSWORD))).matches("[a-zA-Z0-9]{12}"), is(true)));

            List<String> capturedAclNames = aclNameCaptor.getAllValues();
            context.verify(() -> assertThat(capturedAclNames.size(), is(2)));
            context.verify(() -> assertThat(capturedAclNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME))));
            context.verify(() -> assertThat(capturedAclNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME))));

            List<Set<SimpleAclRule>> capturedAcls = aclRulesCaptor.getAllValues();

            context.verify(() -> assertThat(capturedAcls.size(), is(2)));
            Set<SimpleAclRule> aclRules = capturedAcls.get(1);

            context.verify(() -> assertThat(aclRules.size(), is(ResourceUtils.createExpectedSimpleAclRules(user).size())));
            context.verify(() -> assertThat(aclRules, is(ResourceUtils.createExpectedSimpleAclRules(user))));
            context.verify(() -> assertThat(capturedAcls.get(0), is(nullValue())));

            async.flag();
        });
    }

    @Test
    public void testReconcileExistingScramShaUser(VertxTestContext context)    {
        CrdOperator mockCrdOps = mock(CrdOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        SimpleAclOperator aclOps = mock(SimpleAclOperator.class);
        ScramShaCredentialsOperator scramOps = mock(ScramShaCredentialsOperator.class);
        KafkaUserQuotasOperator quotasOps = mock(KafkaUserQuotasOperator.class);

        KafkaUserOperator op = new KafkaUserOperator(vertx, mockCertManager, mockCrdOps,
                Labels.userLabels(ResourceUtils.LABELS),
                mockSecretOps, scramOps, quotasOps, aclOps, ResourceUtils.CA_CERT_NAME, ResourceUtils.CA_KEY_NAME, ResourceUtils.NAMESPACE);
        KafkaUser user = ResourceUtils.createKafkaUserScramSha();
        Secret userCert = ResourceUtils.createUserSecretScramSha();
        String password = new String(Base64.getDecoder().decode(userCert.getData().get(KafkaUserModel.KEY_PASSWORD)));

        ArgumentCaptor<String> secretNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> secretNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Secret> secretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(secretNamespaceCaptor.capture(), secretNameCaptor.capture(), secretCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> scramUserCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> scramPasswordCaptor = ArgumentCaptor.forClass(String.class);
        when(scramOps.reconcile(scramUserCaptor.capture(), scramPasswordCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> aclNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Set<SimpleAclRule>> aclRulesCaptor = ArgumentCaptor.forClass(Set.class);
        when(aclOps.reconcile(aclNameCaptor.capture(), aclRulesCaptor.capture())).thenReturn(Future.succeededFuture());

        when(mockSecretOps.get(eq(user.getMetadata().getNamespace()), eq(user.getMetadata().getName()))).thenReturn(userCert);

        when(mockCrdOps.get(eq(user.getMetadata().getNamespace()), eq(user.getMetadata().getName()))).thenReturn(user);
        when(mockCrdOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(user));
        when(mockCrdOps.updateStatusAsync(any(KafkaUser.class))).thenReturn(Future.succeededFuture());
        when(quotasOps.reconcile(any(), any())).thenReturn(Future.succeededFuture());

        Checkpoint async = context.checkpoint();
        op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME)).setHandler(res -> {
            context.verify(() -> assertThat(res.succeeded(), is(true)));

            List<String> capturedNames = secretNameCaptor.getAllValues();
            context.verify(() -> assertThat(capturedNames.size(), is(1)));
            context.verify(() -> assertThat(capturedNames.get(0), is(ResourceUtils.NAME)));

            List<String> capturedNamespaces = secretNamespaceCaptor.getAllValues();
            context.verify(() -> assertThat(capturedNamespaces.size(), is(1)));
            context.verify(() -> assertThat(capturedNamespaces.get(0), is(ResourceUtils.NAMESPACE)));

            List<Secret> capturedSecrets = secretCaptor.getAllValues();

            context.verify(() -> assertThat(capturedSecrets.size(), is(1)));

            Secret captured = capturedSecrets.get(0);
            context.verify(() -> assertThat(captured.getMetadata().getName(), is(user.getMetadata().getName())));
            context.verify(() -> assertThat(captured.getMetadata().getNamespace(), is(user.getMetadata().getNamespace())));
            context.verify(() -> assertThat(captured.getMetadata().getLabels(),
                    is(Labels.userLabels(user.getMetadata().getLabels())
                            .withKubernetesName()
                            .withKubernetesInstance(ResourceUtils.NAME)
                            .withKubernetesPartOf(ResourceUtils.NAME)
                            .withKubernetesManagedBy(KafkaUserModel.KAFKA_USER_OPERATOR_NAME)
                            .withKind(KafkaUser.RESOURCE_KIND)
                            .toMap())));
            context.verify(() -> assertThat(new String(Base64.getDecoder().decode(captured.getData().get(KafkaUserModel.KEY_PASSWORD))), is(password)));
            context.verify(() -> assertThat(scramPasswordCaptor.getValue(), is(password)));

            List<String> capturedAclNames = aclNameCaptor.getAllValues();
            context.verify(() -> assertThat(capturedAclNames.size(), is(2)));
            context.verify(() -> assertThat(capturedAclNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME))));
            context.verify(() -> assertThat(capturedAclNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME))));

            List<Set<SimpleAclRule>> capturedAcls = aclRulesCaptor.getAllValues();

            context.verify(() -> assertThat(capturedAcls.size(), is(2)));
            Set<SimpleAclRule> aclRules = capturedAcls.get(1);

            context.verify(() -> assertThat(aclRules.size(), is(ResourceUtils.createExpectedSimpleAclRules(user).size())));
            context.verify(() -> assertThat(aclRules, is(ResourceUtils.createExpectedSimpleAclRules(user))));
            context.verify(() -> assertThat(capturedAcls.get(0), is(nullValue())));

            async.flag();
        });
    }

    @Test
    public void testReconcileDeleteScramShaUser(VertxTestContext context)    {
        CrdOperator mockCrdOps = mock(CrdOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        SimpleAclOperator aclOps = mock(SimpleAclOperator.class);
        ScramShaCredentialsOperator scramOps = mock(ScramShaCredentialsOperator.class);
        KafkaUserQuotasOperator quotasOps = mock(KafkaUserQuotasOperator.class);

        KafkaUserOperator op = new KafkaUserOperator(vertx, mockCertManager, mockCrdOps,
                Labels.userLabels(ResourceUtils.LABELS),
                mockSecretOps, scramOps, quotasOps, aclOps, ResourceUtils.CA_CERT_NAME, ResourceUtils.CA_KEY_NAME, ResourceUtils.NAMESPACE);
        KafkaUser user = ResourceUtils.createKafkaUserScramSha();
        Secret userCert = ResourceUtils.createUserSecretTls();

        ArgumentCaptor<String> secretNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> secretNameCaptor = ArgumentCaptor.forClass(String.class);
        when(mockSecretOps.reconcile(secretNamespaceCaptor.capture(), secretNameCaptor.capture(), isNull())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> scramUserCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> scramPasswordCaptor = ArgumentCaptor.forClass(String.class);
        when(scramOps.reconcile(scramUserCaptor.capture(), scramPasswordCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> aclNameCaptor = ArgumentCaptor.forClass(String.class);
        when(aclOps.reconcile(aclNameCaptor.capture(), isNull())).thenReturn(Future.succeededFuture());

        when(mockSecretOps.get(eq(user.getMetadata().getNamespace()), eq(user.getMetadata().getName()))).thenReturn(userCert);

        when(mockCrdOps.get(eq(user.getMetadata().getNamespace()), eq(user.getMetadata().getName()))).thenReturn(null);

        when(quotasOps.reconcile(anyString(), eq(null))).thenReturn(Future.succeededFuture());

        Checkpoint async = context.checkpoint();
        op.reconcile(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME)).setHandler(res -> {
            context.verify(() -> assertThat(res.succeeded(), is(true)));

            List<String> capturedNames = secretNameCaptor.getAllValues();
            context.verify(() -> assertThat(capturedNames.size(), is(1)));
            context.verify(() -> assertThat(capturedNames.get(0), is(ResourceUtils.NAME)));

            List<String> capturedNamespaces = secretNamespaceCaptor.getAllValues();
            context.verify(() -> assertThat(capturedNamespaces.size(), is(1)));
            context.verify(() -> assertThat(capturedNamespaces.get(0), is(ResourceUtils.NAMESPACE)));

            List<String> capturedAclNames = aclNameCaptor.getAllValues();
            context.verify(() -> assertThat(capturedAclNames.size(), is(2)));
            context.verify(() -> assertThat(capturedAclNames.get(0), is(KafkaUserModel.getTlsUserName(ResourceUtils.NAME))));
            context.verify(() -> assertThat(capturedAclNames.get(1), is(KafkaUserModel.getScramUserName(ResourceUtils.NAME))));

            context.verify(() -> assertThat(scramUserCaptor.getAllValues(), is(singletonList(ResourceUtils.NAME))));
            context.verify(() -> assertThat(scramPasswordCaptor.getAllValues(), is(singletonList(null))));

            async.flag();
        });
    }

    @Test
    public void testUserStatusNotReady(VertxTestContext context) {
        String failureMsg = "failure";
        CrdOperator mockCrdOps = mock(CrdOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        SimpleAclOperator aclOps = mock(SimpleAclOperator.class);
        ScramShaCredentialsOperator scramOps = mock(ScramShaCredentialsOperator.class);
        KafkaUserQuotasOperator quotasOps = mock(KafkaUserQuotasOperator.class);

        KafkaUser user = ResourceUtils.createKafkaUserTls();
        Secret clientsCa = ResourceUtils.createClientsCaCertSecret();
        Secret clientsCaKey = ResourceUtils.createClientsCaKeySecret();
        when(mockSecretOps.get(anyString(), eq("user-cert"))).thenReturn(clientsCa);
        when(mockSecretOps.get(anyString(), eq("user-key"))).thenReturn(clientsCaKey);

        when(mockCrdOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(user));

        when(mockSecretOps.reconcile(anyString(), anyString(), any(Secret.class))).thenReturn(Future.failedFuture(failureMsg));
        when(aclOps.reconcile(anyString(), any())).thenReturn(Future.succeededFuture());
        when(scramOps.reconcile(any(), any())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<KafkaUser> userCaptor = ArgumentCaptor.forClass(KafkaUser.class);
        when(mockCrdOps.updateStatusAsync(userCaptor.capture())).thenReturn(Future.succeededFuture());
        when(quotasOps.reconcile(any(), any())).thenReturn(Future.succeededFuture());

        KafkaUserOperator op = new KafkaUserOperator(vertx, mockCertManager, mockCrdOps,
                Labels.userLabels(ResourceUtils.LABELS),
                mockSecretOps, scramOps, quotasOps, aclOps, ResourceUtils.CA_CERT_NAME, ResourceUtils.CA_KEY_NAME, ResourceUtils.NAMESPACE);

        Checkpoint async = context.checkpoint();
        op.createOrUpdate(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME), user).setHandler(res -> {
            context.verify(() -> assertThat(res.succeeded(), is(false)));

            List<KafkaUser> capturedStatuses = userCaptor.getAllValues();
            context.verify(() -> assertThat(capturedStatuses.get(0).getStatus().getUsername(), is("CN=user")));
            context.verify(() -> assertThat(capturedStatuses.get(0).getStatus().getConditions().get(0).getStatus(), is("True")));
            context.verify(() -> assertThat(capturedStatuses.get(0).getStatus().getConditions().get(0).getMessage(), is(failureMsg)));
            context.verify(() -> assertThat(capturedStatuses.get(0).getStatus().getConditions().get(0).getType(), is("NotReady")));
            async.flag();
        });
    }

    @Test
    public void testUserStatusReady(VertxTestContext context) {
        CrdOperator mockCrdOps = mock(CrdOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        SimpleAclOperator aclOps = mock(SimpleAclOperator.class);
        ScramShaCredentialsOperator scramOps = mock(ScramShaCredentialsOperator.class);
        KafkaUserQuotasOperator quotasOps = mock(KafkaUserQuotasOperator.class);

        KafkaUser user = ResourceUtils.createKafkaUserTls();
        Secret clientsCa = ResourceUtils.createClientsCaCertSecret();
        Secret clientsCaKey = ResourceUtils.createClientsCaKeySecret();
        when(mockSecretOps.get(anyString(), eq("user-cert"))).thenReturn(clientsCa);
        when(mockSecretOps.get(anyString(), eq("user-key"))).thenReturn(clientsCaKey);
        when(mockCrdOps.getAsync(anyString(), anyString())).thenReturn(Future.succeededFuture(user));

        when(mockSecretOps.reconcile(anyString(), anyString(), any(Secret.class))).thenReturn(Future.succeededFuture());
        when(aclOps.reconcile(anyString(), any())).thenReturn(Future.succeededFuture());
        when(scramOps.reconcile(any(), any())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<KafkaUser> userCaptor = ArgumentCaptor.forClass(KafkaUser.class);
        when(mockCrdOps.updateStatusAsync(userCaptor.capture())).thenReturn(Future.succeededFuture());
        when(quotasOps.reconcile(any(), any())).thenReturn(Future.succeededFuture());

        KafkaUserOperator op = new KafkaUserOperator(vertx, mockCertManager, mockCrdOps,
                Labels.userLabels(ResourceUtils.LABELS),
                mockSecretOps, scramOps, quotasOps, aclOps, ResourceUtils.CA_CERT_NAME, ResourceUtils.CA_KEY_NAME, ResourceUtils.NAMESPACE);

        Checkpoint async = context.checkpoint();
        op.createOrUpdate(new Reconciliation("test-trigger", KafkaUser.RESOURCE_KIND, ResourceUtils.NAMESPACE, ResourceUtils.NAME), user).setHandler(res -> {
            context.verify(() -> assertThat(res.succeeded(), is(true)));

            List<KafkaUser> capturedStatuses = userCaptor.getAllValues();
            context.verify(() -> assertThat(capturedStatuses.get(0).getStatus().getUsername(), is("CN=user")));
            context.verify(() -> assertThat(capturedStatuses.get(0).getStatus().getConditions().get(0).getStatus(), is("True")));
            context.verify(() -> assertThat(capturedStatuses.get(0).getStatus().getConditions().get(0).getType(), is("Ready")));
            async.flag();
        });
    }
}
