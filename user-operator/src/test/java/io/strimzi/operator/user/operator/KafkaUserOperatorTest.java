/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.ResourceType;
import io.strimzi.operator.common.operator.MockCertManager;
import io.strimzi.operator.common.operator.resource.CrdOperator;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.strimzi.operator.user.ResourceUtils;
import io.strimzi.operator.user.model.KafkaUserModel;
import io.strimzi.operator.user.model.acl.SimpleAclRule;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import static java.util.Arrays.asList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class KafkaUserOperatorTest {
    protected static Vertx vertx;
    private final CertManager mockCertManager = new MockCertManager();

    @BeforeClass
    public static void before() {
        vertx = Vertx.vertx();
    }

    @AfterClass
    public static void after() {
        vertx.close();
    }

    @Test
    public void testCreateTlsUser(TestContext context)    {
        CrdOperator mockCrdOps = mock(CrdOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        SimpleAclOperator aclOps = mock(SimpleAclOperator.class);
        ScramShaCredentialsOperator scramOps = mock(ScramShaCredentialsOperator.class);

        ArgumentCaptor<String> secretNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> secretNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Secret> secretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(secretNamespaceCaptor.capture(), secretNameCaptor.capture(), secretCaptor.capture())).thenReturn(Future.succeededFuture());
        ArgumentCaptor<String> aclNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Set<SimpleAclRule>> aclRulesCaptor = ArgumentCaptor.forClass(Set.class);
        when(aclOps.reconcile(aclNameCaptor.capture(), aclRulesCaptor.capture())).thenReturn(Future.succeededFuture());

        when(scramOps.reconcile(any(), any())).thenReturn(Future.succeededFuture());

        KafkaUserOperator op = new KafkaUserOperator(vertx, mockCertManager, mockCrdOps, mockSecretOps, scramOps, aclOps, ResourceUtils.CA_NAME, ResourceUtils.NAMESPACE);
        KafkaUser user = ResourceUtils.createKafkaUserTls();
        Secret clientsCa = ResourceUtils.createClientsCa();

        Async async = context.async();
        op.createOrUpdate(new Reconciliation("test-trigger", ResourceType.USER, ResourceUtils.NAMESPACE, ResourceUtils.NAME), user, clientsCa, null, res -> {
            context.assertTrue(res.succeeded());

            List<String> capturedNames = secretNameCaptor.getAllValues();
            context.assertEquals(1, capturedNames.size());
            context.assertEquals(ResourceUtils.NAME, capturedNames.get(0));

            List<String> capturedNamespaces = secretNamespaceCaptor.getAllValues();
            context.assertEquals(1, capturedNamespaces.size());
            context.assertEquals(ResourceUtils.NAMESPACE, capturedNamespaces.get(0));

            List<Secret> capturedSecrets = secretCaptor.getAllValues();

            context.assertEquals(1, capturedSecrets.size());

            Secret captured = capturedSecrets.get(0);
            context.assertEquals(user.getMetadata().getName(), captured.getMetadata().getName());
            context.assertEquals(user.getMetadata().getNamespace(), captured.getMetadata().getNamespace());
            context.assertEquals(Labels.userLabels(user.getMetadata().getLabels()).withKind(KafkaUser.RESOURCE_KIND).toMap(), captured.getMetadata().getLabels());
            context.assertEquals("clients-ca-crt", new String(Base64.getDecoder().decode(captured.getData().get("ca.crt"))));
            context.assertEquals("crt file", new String(Base64.getDecoder().decode(captured.getData().get("user.crt"))));
            context.assertEquals("key file", new String(Base64.getDecoder().decode(captured.getData().get("user.key"))));

            List<String> capturedAclNames = aclNameCaptor.getAllValues();
            context.assertEquals(1, capturedAclNames.size());
            context.assertEquals(KafkaUserModel.getUserName(ResourceUtils.NAME), capturedAclNames.get(0));

            List<Set<SimpleAclRule>> capturedAcls = aclRulesCaptor.getAllValues();

            context.assertEquals(1, capturedAcls.size());
            Set<SimpleAclRule> aclRules = capturedAcls.get(0);

            context.assertEquals(ResourceUtils.createExpectedSimpleAclRules(user).size(), aclRules.size());
            context.assertEquals(ResourceUtils.createExpectedSimpleAclRules(user), aclRules);

            async.complete();
        });
    }

    @Test
    public void testUpdateUserNoChange(TestContext context)    {
        CrdOperator mockCrdOps = mock(CrdOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        SimpleAclOperator aclOps = mock(SimpleAclOperator.class);
        ScramShaCredentialsOperator scramOps = mock(ScramShaCredentialsOperator.class);

        ArgumentCaptor<String> secretNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> secretNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Secret> secretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(secretNamespaceCaptor.capture(), secretNameCaptor.capture(), secretCaptor.capture())).thenReturn(Future.succeededFuture());

        when(scramOps.reconcile(any(), any())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> aclNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Set<SimpleAclRule>> aclRulesCaptor = ArgumentCaptor.forClass(Set.class);
        when(aclOps.reconcile(aclNameCaptor.capture(), aclRulesCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaUserOperator op = new KafkaUserOperator(vertx, mockCertManager, mockCrdOps, mockSecretOps, scramOps, aclOps, ResourceUtils.CA_NAME, ResourceUtils.NAMESPACE);
        KafkaUser user = ResourceUtils.createKafkaUserTls();
        Secret clientsCa = ResourceUtils.createClientsCa();
        Secret userCert = ResourceUtils.createUserSecretTls();

        Async async = context.async();
        op.createOrUpdate(new Reconciliation("test-trigger", ResourceType.USER, ResourceUtils.NAMESPACE, ResourceUtils.NAME), user, clientsCa, userCert, res -> {
            context.assertTrue(res.succeeded());

            List<String> capturedNames = secretNameCaptor.getAllValues();
            context.assertEquals(1, capturedNames.size());
            context.assertEquals(ResourceUtils.NAME, capturedNames.get(0));

            List<String> capturedNamespaces = secretNamespaceCaptor.getAllValues();
            context.assertEquals(1, capturedNamespaces.size());
            context.assertEquals(ResourceUtils.NAMESPACE, capturedNamespaces.get(0));

            List<Secret> capturedSecrets = secretCaptor.getAllValues();

            context.assertEquals(1, capturedSecrets.size());

            Secret captured = capturedSecrets.get(0);
            context.assertEquals(userCert.getMetadata().getName(), captured.getMetadata().getName());
            context.assertEquals(userCert.getMetadata().getNamespace(), captured.getMetadata().getNamespace());
            context.assertEquals(userCert.getMetadata().getLabels(), captured.getMetadata().getLabels());
            context.assertEquals(userCert.getData().get("ca.crt"), captured.getData().get("ca.crt"));
            context.assertEquals(userCert.getData().get("user.crt"), captured.getData().get("user.crt"));
            context.assertEquals(userCert.getData().get("user.key"), captured.getData().get("user.key"));

            List<String> capturedAclNames = aclNameCaptor.getAllValues();
            context.assertEquals(1, capturedAclNames.size());
            context.assertEquals(KafkaUserModel.getUserName(ResourceUtils.NAME), capturedAclNames.get(0));

            List<Set<SimpleAclRule>> capturedAcls = aclRulesCaptor.getAllValues();

            context.assertEquals(1, capturedAcls.size());
            Set<SimpleAclRule> aclRules = capturedAcls.get(0);

            context.assertEquals(ResourceUtils.createExpectedSimpleAclRules(user).size(), aclRules.size());
            context.assertEquals(ResourceUtils.createExpectedSimpleAclRules(user), aclRules);

            async.complete();
        });
    }

    /**
     * Tests what happens when the TlsClisteAuth and SimpleAuthorization are suddenyl desiabled for the user (delete from the KafkaUser resource)
     *
     * @param context
     */
    @Test
    public void testUpdateUserNoAuthnAuthz(TestContext context)    {
        CrdOperator mockCrdOps = mock(CrdOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        SimpleAclOperator aclOps = mock(SimpleAclOperator.class);
        ScramShaCredentialsOperator scramOps = mock(ScramShaCredentialsOperator.class);

        ArgumentCaptor<String> secretNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> secretNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Secret> secretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(secretNamespaceCaptor.capture(), secretNameCaptor.capture(), secretCaptor.capture())).thenReturn(Future.succeededFuture());

        when(scramOps.reconcile(any(), any())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> aclNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Set<SimpleAclRule>> aclRulesCaptor = ArgumentCaptor.forClass(Set.class);
        when(aclOps.reconcile(aclNameCaptor.capture(), aclRulesCaptor.capture())).thenReturn(Future.succeededFuture());

        KafkaUserOperator op = new KafkaUserOperator(vertx, mockCertManager, mockCrdOps, mockSecretOps, scramOps, aclOps, ResourceUtils.CA_NAME, ResourceUtils.NAMESPACE);
        KafkaUser user = ResourceUtils.createKafkaUserTls();
        user.getSpec().setAuthorization(null);
        user.getSpec().setAuthentication(null);
        Secret clientsCa = ResourceUtils.createClientsCa();
        Secret userCert = ResourceUtils.createUserSecretTls();

        Async async = context.async();
        op.createOrUpdate(new Reconciliation("test-trigger", ResourceType.USER, ResourceUtils.NAMESPACE, ResourceUtils.NAME), user, clientsCa, userCert, res -> {
            context.assertTrue(res.succeeded());

            List<String> capturedNames = secretNameCaptor.getAllValues();
            context.assertEquals(1, capturedNames.size());
            context.assertEquals(ResourceUtils.NAME, capturedNames.get(0));

            List<String> capturedNamespaces = secretNamespaceCaptor.getAllValues();
            context.assertEquals(1, capturedNamespaces.size());
            context.assertEquals(ResourceUtils.NAMESPACE, capturedNamespaces.get(0));

            List<Secret> capturedSecrets = secretCaptor.getAllValues();

            context.assertEquals(1, capturedSecrets.size());

            Secret captured = capturedSecrets.get(0);
            context.assertNull(captured);

            List<String> capturedAclNames = aclNameCaptor.getAllValues();
            context.assertEquals(1, capturedAclNames.size());
            context.assertEquals(KafkaUserModel.getUserName(ResourceUtils.NAME), capturedAclNames.get(0));

            List<Set<SimpleAclRule>> capturedAcls = aclRulesCaptor.getAllValues();

            context.assertEquals(1, capturedAcls.size());
            Set<SimpleAclRule> aclRules = capturedAcls.get(0);
            context.assertNull(aclRules);

            async.complete();
        });
    }

    @Test
    public void testUpdateUserNewCert(TestContext context)    {
        CrdOperator mockCrdOps = mock(CrdOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        SimpleAclOperator aclOps = mock(SimpleAclOperator.class);
        ScramShaCredentialsOperator scramOps = mock(ScramShaCredentialsOperator.class);

        ArgumentCaptor<String> secretNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> secretNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Secret> secretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(secretNamespaceCaptor.capture(), secretNameCaptor.capture(), secretCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> aclNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Set<SimpleAclRule>> aclRulesCaptor = ArgumentCaptor.forClass(Set.class);
        when(aclOps.reconcile(aclNameCaptor.capture(), aclRulesCaptor.capture())).thenReturn(Future.succeededFuture());

        when(scramOps.reconcile(any(), any())).thenReturn(Future.succeededFuture());

        KafkaUserOperator op = new KafkaUserOperator(vertx, mockCertManager, mockCrdOps, mockSecretOps, scramOps, aclOps, ResourceUtils.CA_NAME, ResourceUtils.NAMESPACE);
        KafkaUser user = ResourceUtils.createKafkaUserTls();
        Secret clientsCa = ResourceUtils.createClientsCa();
        clientsCa.getData().put("clients-ca.key", Base64.getEncoder().encodeToString("different-clients-ca-key".getBytes()));
        clientsCa.getData().put("clients-ca.crt", Base64.getEncoder().encodeToString("different-clients-ca-crt".getBytes()));
        Secret userCert = ResourceUtils.createUserSecretTls();

        Async async = context.async();
        op.createOrUpdate(new Reconciliation("test-trigger", ResourceType.USER, ResourceUtils.NAMESPACE, ResourceUtils.NAME), user, clientsCa, userCert, res -> {
            context.assertTrue(res.succeeded());

            List<String> capturedNames = secretNameCaptor.getAllValues();
            context.assertEquals(1, capturedNames.size());
            context.assertEquals(ResourceUtils.NAME, capturedNames.get(0));

            List<String> capturedNamespaces = secretNamespaceCaptor.getAllValues();
            context.assertEquals(1, capturedNamespaces.size());
            context.assertEquals(ResourceUtils.NAMESPACE, capturedNamespaces.get(0));

            List<Secret> capturedSecrets = secretCaptor.getAllValues();

            context.assertEquals(1, capturedSecrets.size());

            Secret captured = capturedSecrets.get(0);
            context.assertEquals(userCert.getMetadata().getName(), captured.getMetadata().getName());
            context.assertEquals(userCert.getMetadata().getNamespace(), captured.getMetadata().getNamespace());
            context.assertEquals(userCert.getMetadata().getLabels(), captured.getMetadata().getLabels());
            context.assertEquals("different-clients-ca-crt", new String(Base64.getDecoder().decode(captured.getData().get("ca.crt"))));
            context.assertEquals("crt file", new String(Base64.getDecoder().decode(captured.getData().get("user.crt"))));
            context.assertEquals("key file", new String(Base64.getDecoder().decode(captured.getData().get("user.key"))));

            async.complete();
        });
    }

    @Test
    public void testDeleteTlsUser(TestContext context)    {
        CrdOperator mockCrdOps = mock(CrdOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        SimpleAclOperator aclOps = mock(SimpleAclOperator.class);
        ScramShaCredentialsOperator scramOps = mock(ScramShaCredentialsOperator.class);

        ArgumentCaptor<String> secretNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> secretNameCaptor = ArgumentCaptor.forClass(String.class);
        when(mockSecretOps.reconcile(secretNamespaceCaptor.capture(), secretNameCaptor.capture(), isNull())).thenReturn(Future.succeededFuture());

        when(scramOps.reconcile(any(), any())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> aclNameCaptor = ArgumentCaptor.forClass(String.class);
        when(aclOps.reconcile(aclNameCaptor.capture(), isNull())).thenReturn(Future.succeededFuture());

        KafkaUserOperator op = new KafkaUserOperator(vertx, mockCertManager, mockCrdOps, mockSecretOps, scramOps, aclOps, ResourceUtils.CA_NAME, ResourceUtils.NAMESPACE);

        Async async = context.async();
        op.delete(new Reconciliation("test-trigger", ResourceType.USER, ResourceUtils.NAMESPACE, ResourceUtils.NAME), res -> {
            context.assertTrue(res.succeeded());

            List<String> capturedNames = secretNameCaptor.getAllValues();
            context.assertEquals(1, capturedNames.size());
            context.assertEquals(ResourceUtils.NAME, capturedNames.get(0));

            List<String> capturedNamespaces = secretNamespaceCaptor.getAllValues();
            context.assertEquals(1, capturedNamespaces.size());
            context.assertEquals(ResourceUtils.NAMESPACE, capturedNamespaces.get(0));

            List<String> capturedAclNames = aclNameCaptor.getAllValues();
            context.assertEquals(1, capturedAclNames.size());
            context.assertEquals(KafkaUserModel.getUserName(ResourceUtils.NAME), capturedAclNames.get(0));

            async.complete();
        });
    }

    @Test
    public void testReconcileNewTlsUser(TestContext context)    {
        CrdOperator mockCrdOps = mock(CrdOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        SimpleAclOperator aclOps = mock(SimpleAclOperator.class);
        ScramShaCredentialsOperator scramOps = mock(ScramShaCredentialsOperator.class);

        KafkaUserOperator op = new KafkaUserOperator(vertx, mockCertManager, mockCrdOps, mockSecretOps, scramOps, aclOps, ResourceUtils.CA_NAME, ResourceUtils.NAMESPACE);
        KafkaUser user = ResourceUtils.createKafkaUserTls();
        Secret clientsCa = ResourceUtils.createClientsCa();

        ArgumentCaptor<String> secretNamespaceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> secretNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Secret> secretCaptor = ArgumentCaptor.forClass(Secret.class);
        when(mockSecretOps.reconcile(secretNamespaceCaptor.capture(), secretNameCaptor.capture(), secretCaptor.capture())).thenReturn(Future.succeededFuture());

        ArgumentCaptor<String> aclNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Set<SimpleAclRule>> aclRulesCaptor = ArgumentCaptor.forClass(Set.class);
        when(aclOps.reconcile(aclNameCaptor.capture(), aclRulesCaptor.capture())).thenReturn(Future.succeededFuture());

        when(scramOps.reconcile(any(), any())).thenReturn(Future.succeededFuture());

        when(mockSecretOps.get(eq(clientsCa.getMetadata().getNamespace()), eq(clientsCa.getMetadata().getName()))).thenReturn(clientsCa);
        when(mockSecretOps.get(eq(user.getMetadata().getNamespace()), eq(user.getMetadata().getName()))).thenReturn(null);

        when(mockCrdOps.get(eq(user.getMetadata().getNamespace()), eq(user.getMetadata().getName()))).thenReturn(user);

        Async async = context.async();
        op.reconcile(new Reconciliation("test-trigger", ResourceType.USER, ResourceUtils.NAMESPACE, ResourceUtils.NAME), res -> {
            context.assertTrue(res.succeeded());

            List<String> capturedNames = secretNameCaptor.getAllValues();
            context.assertEquals(1, capturedNames.size());
            context.assertEquals(ResourceUtils.NAME, capturedNames.get(0));

            List<String> capturedNamespaces = secretNamespaceCaptor.getAllValues();
            context.assertEquals(1, capturedNamespaces.size());
            context.assertEquals(ResourceUtils.NAMESPACE, capturedNamespaces.get(0));

            List<Secret> capturedSecrets = secretCaptor.getAllValues();

            context.assertEquals(1, capturedSecrets.size());

            Secret captured = capturedSecrets.get(0);
            context.assertEquals(user.getMetadata().getName(), captured.getMetadata().getName());
            context.assertEquals(user.getMetadata().getNamespace(), captured.getMetadata().getNamespace());
            context.assertEquals(Labels.userLabels(user.getMetadata().getLabels()).withKind(KafkaUser.RESOURCE_KIND).toMap(), captured.getMetadata().getLabels());
            context.assertEquals("clients-ca-crt", new String(Base64.getDecoder().decode(captured.getData().get("ca.crt"))));
            context.assertEquals("crt file", new String(Base64.getDecoder().decode(captured.getData().get("user.crt"))));
            context.assertEquals("key file", new String(Base64.getDecoder().decode(captured.getData().get("user.key"))));

            List<String> capturedAclNames = aclNameCaptor.getAllValues();
            context.assertEquals(1, capturedAclNames.size());
            context.assertEquals(KafkaUserModel.getUserName(ResourceUtils.NAME), capturedAclNames.get(0));

            List<Set<SimpleAclRule>> capturedAcls = aclRulesCaptor.getAllValues();

            context.assertEquals(1, capturedAcls.size());
            Set<SimpleAclRule> aclRules = capturedAcls.get(0);

            context.assertEquals(ResourceUtils.createExpectedSimpleAclRules(user).size(), aclRules.size());
            context.assertEquals(ResourceUtils.createExpectedSimpleAclRules(user), aclRules);

            async.complete();
        });
    }

    @Test
    public void testReconcileExistingTlsUser(TestContext context)    {
        CrdOperator mockCrdOps = mock(CrdOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        SimpleAclOperator aclOps = mock(SimpleAclOperator.class);
        ScramShaCredentialsOperator scramOps = mock(ScramShaCredentialsOperator.class);

        KafkaUserOperator op = new KafkaUserOperator(vertx, mockCertManager, mockCrdOps, mockSecretOps, scramOps, aclOps, ResourceUtils.CA_NAME, ResourceUtils.NAMESPACE);
        KafkaUser user = ResourceUtils.createKafkaUserTls();
        Secret clientsCa = ResourceUtils.createClientsCa();
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
        when(mockSecretOps.get(eq(user.getMetadata().getNamespace()), eq(user.getMetadata().getName()))).thenReturn(userCert);

        when(mockCrdOps.get(eq(user.getMetadata().getNamespace()), eq(user.getMetadata().getName()))).thenReturn(user);

        Async async = context.async();
        op.reconcile(new Reconciliation("test-trigger", ResourceType.USER, ResourceUtils.NAMESPACE, ResourceUtils.NAME), res -> {
            context.assertTrue(res.succeeded());

            List<String> capturedNames = secretNameCaptor.getAllValues();
            context.assertEquals(1, capturedNames.size());
            context.assertEquals(ResourceUtils.NAME, capturedNames.get(0));

            List<String> capturedNamespaces = secretNamespaceCaptor.getAllValues();
            context.assertEquals(1, capturedNamespaces.size());
            context.assertEquals(ResourceUtils.NAMESPACE, capturedNamespaces.get(0));

            List<Secret> capturedSecrets = secretCaptor.getAllValues();

            context.assertEquals(1, capturedSecrets.size());

            Secret captured = capturedSecrets.get(0);
            context.assertEquals(user.getMetadata().getName(), captured.getMetadata().getName());
            context.assertEquals(user.getMetadata().getNamespace(), captured.getMetadata().getNamespace());
            context.assertEquals(Labels.userLabels(user.getMetadata().getLabels()).withKind(KafkaUser.RESOURCE_KIND).toMap(), captured.getMetadata().getLabels());
            context.assertEquals(userCert.getData().get("ca.crt"), captured.getData().get("ca.crt"));
            context.assertEquals(userCert.getData().get("user.crt"), captured.getData().get("user.crt"));
            context.assertEquals(userCert.getData().get("user.key"), captured.getData().get("user.key"));

            List<String> capturedAclNames = aclNameCaptor.getAllValues();
            context.assertEquals(1, capturedAclNames.size());
            context.assertEquals(KafkaUserModel.getUserName(ResourceUtils.NAME), capturedAclNames.get(0));

            List<Set<SimpleAclRule>> capturedAcls = aclRulesCaptor.getAllValues();

            context.assertEquals(1, capturedAcls.size());
            Set<SimpleAclRule> aclRules = capturedAcls.get(0);

            context.assertEquals(ResourceUtils.createExpectedSimpleAclRules(user).size(), aclRules.size());
            context.assertEquals(ResourceUtils.createExpectedSimpleAclRules(user), aclRules);

            async.complete();
        });
    }

    @Test
    public void testReconcileDeleteTlsUser(TestContext context)    {
        CrdOperator mockCrdOps = mock(CrdOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        SimpleAclOperator aclOps = mock(SimpleAclOperator.class);
        ScramShaCredentialsOperator scramOps = mock(ScramShaCredentialsOperator.class);

        KafkaUserOperator op = new KafkaUserOperator(vertx, mockCertManager, mockCrdOps, mockSecretOps, scramOps, aclOps, ResourceUtils.CA_NAME, ResourceUtils.NAMESPACE);
        KafkaUser user = ResourceUtils.createKafkaUserTls();
        Secret clientsCa = ResourceUtils.createClientsCa();
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

        Async async = context.async();
        op.reconcile(new Reconciliation("test-trigger", ResourceType.USER, ResourceUtils.NAMESPACE, ResourceUtils.NAME), res -> {
            context.assertTrue(res.succeeded());

            List<String> capturedNames = secretNameCaptor.getAllValues();
            context.assertEquals(1, capturedNames.size());
            context.assertEquals(ResourceUtils.NAME, capturedNames.get(0));

            List<String> capturedNamespaces = secretNamespaceCaptor.getAllValues();
            context.assertEquals(1, capturedNamespaces.size());
            context.assertEquals(ResourceUtils.NAMESPACE, capturedNamespaces.get(0));

            List<String> capturedAclNames = aclNameCaptor.getAllValues();
            context.assertEquals(1, capturedAclNames.size());
            context.assertEquals(KafkaUserModel.getUserName(ResourceUtils.NAME), capturedAclNames.get(0));

            async.complete();
        });
    }

    @Test
    public void testReconcileAll(TestContext context)    {
        CrdOperator mockCrdOps = mock(CrdOperator.class);
        SecretOperator mockSecretOps = mock(SecretOperator.class);
        SimpleAclOperator aclOps = mock(SimpleAclOperator.class);
        ScramShaCredentialsOperator scramOps = mock(ScramShaCredentialsOperator.class);

        KafkaUser newTlsUser = ResourceUtils.createKafkaUserTls();
        newTlsUser.getMetadata().setName("new-tls-user");
        KafkaUser newScramShaUser = ResourceUtils.createKafkaUserScramSha();
        newScramShaUser.getMetadata().setName("new-scram-sha-user");
        KafkaUser existingTlsUser = ResourceUtils.createKafkaUserTls();
        existingTlsUser.getMetadata().setName("existing-tls-user");
        Secret clientsCa = ResourceUtils.createClientsCa();
        Secret existingTlsUserSecret = ResourceUtils.createUserSecretTls();
        existingTlsUserSecret.getMetadata().setName("existing-tls-user");
        Secret existingScramShaUserSecret = ResourceUtils.createUserSecretScramSha();
        existingScramShaUserSecret.getMetadata().setName("existing-scram-sha-user");
        KafkaUser existingScramShaUser = ResourceUtils.createKafkaUserTls();
        existingScramShaUser.getMetadata().setName("existing-scram-sha-user");
        Secret deletedUserCert = ResourceUtils.createUserSecretTls();
        deletedUserCert.getMetadata().setName("deleted-user");

        when(mockCrdOps.list(eq(ResourceUtils.NAMESPACE), eq(Labels.userLabels(ResourceUtils.LABELS)))).thenReturn(Arrays.asList(newTlsUser, newScramShaUser, existingTlsUser, existingScramShaUser));
        when(mockSecretOps.list(eq(ResourceUtils.NAMESPACE), eq(Labels.userLabels(ResourceUtils.LABELS).withKind(KafkaUser.RESOURCE_KIND)))).thenReturn(Arrays.asList(existingTlsUserSecret, existingScramShaUserSecret, deletedUserCert));
        when(aclOps.getUsersWithAcls()).thenReturn(new HashSet<String>(Arrays.asList("existing-tls-user", "second-deleted-user")));
        when(scramOps.list()).thenReturn(asList("existing-tls-user", "deleted-scram-sha-user"));

        when(mockCrdOps.get(eq(newTlsUser.getMetadata().getNamespace()), eq(newTlsUser.getMetadata().getName()))).thenReturn(newTlsUser);
        when(mockCrdOps.get(eq(newScramShaUser.getMetadata().getNamespace()), eq(newScramShaUser.getMetadata().getName()))).thenReturn(newScramShaUser);
        when(mockCrdOps.get(eq(existingTlsUser.getMetadata().getNamespace()), eq(existingTlsUser.getMetadata().getName()))).thenReturn(existingTlsUser);
        when(mockCrdOps.get(eq(existingTlsUser.getMetadata().getNamespace()), eq(existingScramShaUser.getMetadata().getName()))).thenReturn(existingScramShaUser);
        when(mockCrdOps.get(eq(deletedUserCert.getMetadata().getNamespace()), eq(deletedUserCert.getMetadata().getName()))).thenReturn(null);
        when(mockSecretOps.get(eq(clientsCa.getMetadata().getNamespace()), eq(clientsCa.getMetadata().getName()))).thenReturn(clientsCa);
        when(mockSecretOps.get(eq(newTlsUser.getMetadata().getNamespace()), eq(newTlsUser.getMetadata().getName()))).thenReturn(null);
        when(mockSecretOps.get(eq(newScramShaUser.getMetadata().getNamespace()), eq(newScramShaUser.getMetadata().getName()))).thenReturn(null);
        when(mockSecretOps.get(eq(existingTlsUser.getMetadata().getNamespace()), eq(existingTlsUser.getMetadata().getName()))).thenReturn(existingTlsUserSecret);
        when(mockSecretOps.get(eq(existingScramShaUser.getMetadata().getNamespace()), eq(existingScramShaUser.getMetadata().getName()))).thenReturn(existingScramShaUserSecret);
        when(mockSecretOps.get(eq(deletedUserCert.getMetadata().getNamespace()), eq(deletedUserCert.getMetadata().getName()))).thenReturn(deletedUserCert);

        Set<String> createdOrUpdated = new CopyOnWriteArraySet<>();
        Set<String> deleted = new CopyOnWriteArraySet<>();

        Async async = context.async(7);
        KafkaUserOperator op = new KafkaUserOperator(vertx,
                mockCertManager,
                mockCrdOps,
                mockSecretOps, scramOps,
                aclOps, ResourceUtils.CA_NAME, ResourceUtils.NAMESPACE) {

            @Override
            public void createOrUpdate(Reconciliation reconciliation, KafkaUser user, Secret clientCa, Secret userSecret, Handler<AsyncResult<Void>> h) {
                createdOrUpdated.add(user.getMetadata().getName());
                async.countDown();
                h.handle(Future.succeededFuture());
            }
            @Override
            public void delete(Reconciliation reconciliation, Handler h) {
                deleted.add(reconciliation.name());
                async.countDown();
                h.handle(Future.succeededFuture());
            }
        };

        // Now try to reconcile all the Kafka Connect clusters
        op.reconcileAll("test", ResourceUtils.NAMESPACE, Labels.userLabels(ResourceUtils.LABELS));

        async.await();

        context.assertEquals(new HashSet(asList("new-tls-user", "existing-tls-user",
                "new-scram-sha-user", "existing-scram-sha-user")), createdOrUpdated);
        context.assertEquals(new HashSet(asList("deleted-user", "second-deleted-user", "deleted-scram-sha-user")), deleted);
    }

    // TODO create scram user
    // TODO update scram user
    // TODO delete scram user
}
