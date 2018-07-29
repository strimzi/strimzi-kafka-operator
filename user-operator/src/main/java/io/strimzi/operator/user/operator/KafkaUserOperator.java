/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.DoneableKafkaConnectAssembly;
import io.strimzi.api.kafka.DoneableKafkaUser;
import io.strimzi.api.kafka.KafkaConnectAssemblyList;
import io.strimzi.api.kafka.KafkaUserList;
import io.strimzi.api.kafka.model.ExternalLogging;
import io.strimzi.api.kafka.model.KafkaConnectAssembly;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.Reconciliation;
import io.strimzi.operator.cluster.model.AssemblyType;
import io.strimzi.operator.cluster.model.KafkaConnectCluster;
import io.strimzi.operator.cluster.model.Labels;
import io.strimzi.operator.cluster.operator.resource.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.CrdOperator;
import io.strimzi.operator.cluster.operator.resource.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.ServiceOperator;
import io.strimzi.operator.user.Reconciliation;
import io.strimzi.operator.user.model.KafkaUserModel;
import io.strimzi.operator.user.operator.resource.CrdOperator;
import io.strimzi.operator.user.operator.resource.SecretOperator;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * <p>Assembly operator for a "Kafka Connect" assembly, which manages:</p>
 * <ul>
 *     <li>A Kafka Connect Deployment and related Services</li>
 * </ul>
 */
public class KafkaUserOperator {

    private static final Logger log = LogManager.getLogger(KafkaUserOperator.class.getName());
    SecretOperator secretOperations;
    CertManager certManager;

    /**
     * @param vertx The Vertx instance
     * @param isOpenShift Whether we're running with OpenShift
     * @param configMapOperations For operating on ConfigMaps
     * @param deploymentOperations For operating on Deployments
     * @param serviceOperations For operating on Services
     * @param secretOperations For operating on Secrets
     */
    public KafkaUserOperator(Vertx vertx,
                             CertManager certManager,
                             CrdOperator<KubernetesClient, KafkaUser, KafkaUserList, DoneableKafkaUser> connectOperator,
                             SecretOperator secretOperations) {
        this.certManager = certManager;
        this.secretOperations = secretOperations;
    }

    @Override
    protected void createOrUpdate(Reconciliation reconciliation, KafkaUser kafkaUser, List<Secret> assemblySecrets, Handler<AsyncResult<Void>> handler) {

        String namespace = reconciliation.namespace();
        String userName = reconciliation.assemblyName();
        KafkaUserModel user;
        try {
            user = KafkaUserModel.fromCrd(kafkaUser);
        } catch (Exception e) {
            handler.handle(Future.failedFuture(e));
            return;
        }

        log.debug("{}: Updating KafkaUser", reconciliation, userName, namespace);
        Future<Void> chainFuture = Future.future();
        secretOperations.reconcile(namespace, user.getSecretName(), user.generateSecret())
                .compose(chainFuture::complete, chainFuture);
        chainFuture.setHandler(handler);
    }

    @Override
    protected void delete(Reconciliation reconciliation, Handler<AsyncResult<Void>> handler) {
        String namespace = reconciliation.namespace();
        String user = reconciliation.assemblyName();

        CompositeFuture.join(secretOperations.reconcile(namespace, KafkaUser.secretName(user), null))
            .map((Void) null).setHandler(handler);
    }

    @Override
    protected List<HasMetadata> getResources(String namespace, Labels selector) {
        List<HasMetadata> result = new ArrayList<>();
        result.addAll(secretOperations.list(namespace, selector));
        return result;
    }
}
