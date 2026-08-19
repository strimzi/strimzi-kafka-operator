/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.api.kafka.model.kafka.exporter.KafkaExporterResources;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.CertIssuer;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.CertSecretUtils;
import io.strimzi.operator.cluster.model.KafkaClusterSecurityContext;
import io.strimzi.operator.cluster.model.ModelUtils;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.RestartReason;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.model.WorkloadUtils;
import io.strimzi.operator.cluster.operator.resource.KafkaAgentClientProvider;
import io.strimzi.operator.cluster.operator.resource.KafkaRoller;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.events.KubernetesRestartEventPublisher;
import io.strimzi.operator.cluster.operator.resource.kubernetes.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.auth.Identity;
import io.strimzi.operator.common.auth.PemAuthIdentity;
import io.strimzi.operator.common.auth.PemTrustSet;
import io.strimzi.operator.common.ca.Ca;
import io.strimzi.operator.common.ca.CaConfig;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.resource.kubernetes.SecretOperator;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

/**
 * Class used for reconciliation of Cluster and Client CAs. This class contains both the steps of the CA reconciliation
 * pipeline and is also used to store the state between them.
 */
public class CaReconciler {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(CaReconciler.class.getName());

    /* test */ final Reconciliation reconciliation;
    private final long operationTimeoutMs;

    /* test */ final DeploymentOperator deploymentOperator;
    private final StrimziPodSetOperator strimziPodSetOperator;
    private final SecretOperator secretOperator;
    /* test */ final PodOperator podOperator;
    private final AdminClientProvider adminClientProvider;
    private final KafkaAgentClientProvider kafkaAgentClientProvider;
    private final CertIssuer certIssuer;
    private final PasswordGenerator passwordGenerator;
    private final KubernetesRestartEventPublisher eventPublisher;

    // Fields based on the Kafka CR config required for the reconciliation
    private final Kafka kafkaCr;
    private final OwnerReference ownerRef;
    private final CaConfig clusterCaConfig;
    private final CaConfig clientsCaConfig;
    private final KafkaClusterSecurityContext securityContext;

    // Fields used to store state during the reconciliation
    private Ca clusterCa;
    private Ca clientsCa;
    private Secret clusterCaCertSecret;
    private Secret coSecret;

    /* test */ boolean isClusterCaNeedFullTrust;
    /* test */ boolean isClusterCaFullyUsed;

    /**
     * Constructs the CA reconciler which reconciles the Cluster and Client CAs
     *
     * @param reconciliation    Reconciliation marker
     * @param kafkaCr           The Kafka custom resource
     * @param config            Cluster Operator Configuration
     * @param supplier          Supplier with Kubernetes Resource Operators
     * @param certIssuer        Certificate Issuer for issuing certificates
     * @param passwordGenerator Password generator for generating passwords
     * @param securityContext   Kafka cluster security context
     */
    public CaReconciler(
            Reconciliation reconciliation,
            Kafka kafkaCr,
            ClusterOperatorConfig config,
            ResourceOperatorSupplier supplier,
            CertIssuer certIssuer,
            PasswordGenerator passwordGenerator,
            KafkaClusterSecurityContext securityContext
    ) {
        this.reconciliation = reconciliation;
        this.operationTimeoutMs = config.getOperationTimeoutMs();

        this.deploymentOperator = supplier.deploymentOperations;
        this.strimziPodSetOperator = supplier.strimziPodSetOperator;
        this.secretOperator = supplier.secretOperations;
        this.podOperator = supplier.podOperations;

        this.adminClientProvider = supplier.adminClientProvider;
        this.kafkaAgentClientProvider = supplier.kafkaAgentClientProvider;
        this.certIssuer = certIssuer;
        this.passwordGenerator = passwordGenerator;

        this.eventPublisher = supplier.restartEventsPublisher;

        this.kafkaCr = kafkaCr;
        this.ownerRef = new OwnerReferenceBuilder()
                .withApiVersion(kafkaCr.getApiVersion())
                .withKind(kafkaCr.getKind())
                .withName(kafkaCr.getMetadata().getName())
                .withUid(kafkaCr.getMetadata().getUid())
                .withBlockOwnerDeletion(true)
                .withController(false)
                .build();
        this.clusterCaConfig = new CaConfig(kafkaCr.getSpec().getClusterCa(), config.isPkcs12KeystoreGeneration());
        this.clientsCaConfig = new CaConfig(kafkaCr.getSpec().getClientsCa(), config.isPkcs12KeystoreGeneration());
        this.securityContext = securityContext;
    }

    /**
     * The main reconciliation method which triggers the whole reconciliation pipeline. This is the method which is
     * expected to be called from the outside to trigger the reconciliation.
     *
     * @param clock     The clock for supplying the reconciler with the time instant of each reconciliation cycle.
     *                  That time is used for checking maintenance windows
     *
     * @return  Future with the CA reconciliation result containing the Cluster and Clients CAs
     */
    public CompletionStage<CaReconciliationResult> reconcile(Clock clock)    {
        return reconcileCas(clock)
                .thenCompose(i -> reconcileTrustBundleSecret())
                .thenCompose(i -> verifyClusterCaFullyTrustedAndUsed())
                .thenCompose(i -> reconcileClusterOperatorSecret(clock))
                .thenCompose(i -> maybeRollingUpdateForNewClusterCaKey())
                .thenCompose(i -> maybeRemoveOldClusterCaCertificates())
                .thenApply(i -> new CaReconciliationResult(clusterCa, clientsCa));
    }

    /**
     * Asynchronously reconciles the cluster and clients CA secrets.
     * The cluster CA secret has to have the name determined by {@link AbstractModel#clusterCaCertSecretName(String)}.
     * The clients CA secret has to have the name determined by {@link KafkaResources#clientsCaCertificateSecretName(String)}.
     * Within both the secrets the current certificate is stored under the key {@code ca.crt}
     * and the current key is stored under the key {@code ca.key}.
     *
     * @param clock     The clock for supplying the reconciler with the time instant of each reconciliation cycle.
     *                  That time is used for checking maintenance windows
     */
    CompletionStage<Void> reconcileCas(Clock clock) {
        String clusterCaCertName = AbstractModel.clusterCaCertSecretName(reconciliation.name());
        String clusterCaKeyName = AbstractModel.clusterCaKeySecretName(reconciliation.name());
        String clientsCaCertName = KafkaResources.clientsCaCertificateSecretName(reconciliation.name());
        String clientsCaKeyName = KafkaResources.clientsCaKeySecretName(reconciliation.name());
        String clusterOperatorName = KafkaResources.clusterOperatorCertsSecretName(reconciliation.name());

        return secretOperator.listAsync(reconciliation.namespace(), Labels.EMPTY.withStrimziKind(reconciliation.kind()).withStrimziCluster(reconciliation.name()))
                .thenCompose(clusterSecrets -> {
                    Secret existingClusterCaCertSecret = null;
                    Secret existingClusterCaKeySecret = null;
                    Secret existingClientsCaCertSecret = null;
                    Secret existingClientsCaKeySecret = null;

                    for (Secret secret : clusterSecrets) {
                        String secretName = secret.getMetadata().getName();
                        if (secretName.equals(clusterCaCertName)) {
                            existingClusterCaCertSecret = secret;
                        } else if (secretName.equals(clusterCaKeyName)) {
                            existingClusterCaKeySecret = secret;
                        } else if (secretName.equals(clientsCaCertName)) {
                            existingClientsCaCertSecret = secret;
                        } else if (secretName.equals(clientsCaKeyName)) {
                            existingClientsCaKeySecret = secret;
                        } else if (secretName.equals(clusterOperatorName)) {
                            coSecret = secret;
                        }
                    }

                    CompletionStage<Void> clusterCaFuture = createCaProvider(
                            Ca.CaRole.CLUSTER_CA,
                            clusterCaConfig,
                            existingClusterCaCertSecret,
                            existingClusterCaKeySecret,
                            clock
                    ).createAndReconcileCa().thenApply(result -> {
                        clusterCa = result.ca();
                        clusterCaCertSecret = result.certSecret();
                        return null;
                    });

                    CompletionStage<Void> clientsCaFuture = createCaProvider(
                            Ca.CaRole.CLIENTS_CA,
                            clientsCaConfig,
                            existingClientsCaCertSecret,
                            existingClientsCaKeySecret,
                            clock
                    ).createAndReconcileCa().thenApply(result -> {
                        clientsCa = result.ca();
                        return null;
                    });
                    return CompletableFuture.allOf(clusterCaFuture.toCompletableFuture(), clientsCaFuture.toCompletableFuture());
                });
    }

    /**
     * Creator method for CaProvider. Overriding this method can be used to get mocked provider.
     *
     * @return  CaProvider instance
     */
    /*test*/ CaProvider createCaProvider(Ca.CaRole caRole, CaConfig caConfig, Secret existingCaCertSecret, Secret existingCaKeySecret, Clock clock) {
        return CaProvider.create(reconciliation, caRole, caConfig, kafkaCr, secretOperator, certIssuer, passwordGenerator, clock, existingCaCertSecret, existingCaKeySecret);
    }

    /**
     * Creates or updates a Secret with the trust-bundles for the Cluster and Clients CAs.
     */
    CompletionStage<Void> reconcileTrustBundleSecret() {
        Secret trustBundleSecret = ModelUtils.createSecret(
                KafkaResources.trustBundleSecretName(reconciliation.name()),
                reconciliation.namespace(),
                Labels.generateDefaultLabels(kafkaCr, Labels.APPLICATION_NAME, "trust-bundle", AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME),
                ownerRef,
                Map.of("cluster-ca.crt", Util.encodeToBase64(clusterCa.trustedCaCerts()), "clients-ca.crt", Util.encodeToBase64(clientsCa.trustedCaCerts())),
                Map.of(clusterCa.caCertGenerationAnnotation(), String.valueOf(clusterCa.caCertGeneration()),
                        clientsCa.caCertGenerationAnnotation(), String.valueOf(clientsCa.caCertGeneration())),
                Map.of()
        );

        return secretOperator
                .reconcile(reconciliation, reconciliation.namespace(), KafkaResources.trustBundleSecretName(reconciliation.name()), trustBundleSecret)
                .thenApply(ignored -> null);
    }

    /**
     * Asynchronously reconciles the cluster operator Secret used to connect to Kafka.
     * This only updates the Secret if the latest Cluster CA is fully trusted across the cluster, otherwise if
     * something goes wrong during reconciliation when the next loop starts it won't be able to connect to Kafka.
     *
     * @param clock    The clock for supplying the reconciler with the time instant of each reconciliation cycle.
     *                 That time is used for checking maintenance windows
     */
    CompletionStage<Void> reconcileClusterOperatorSecret(Clock clock) {
        String componentName = "cluster-operator";

        if (coSecret != null && this.isClusterCaNeedFullTrust) {
            LOGGER.warnCr(reconciliation, "Cluster CA needs to be fully trusted across the cluster, keeping current CO secret and certs");
            return CompletableFuture.completedFuture(null);
        }

        CertAndKey oldCertAndKey = CertSecretUtils.keyStoreCertAndKey(coSecret, componentName, Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION);

        return clusterCa.maybeCopyOrGenerateClientCert(reconciliation, componentName, oldCertAndKey, Util.isMaintenanceTimeWindowsSatisfied(reconciliation, kafkaCr.getSpec().getMaintenanceTimeWindows(), clock.instant()))
                .thenCompose(updatedCert -> {
                    Map<String, String> secretData = CertSecretUtils.buildSecretData(componentName, updatedCert);
                    coSecret = ModelUtils.createSecret(
                            KafkaResources.clusterOperatorCertsSecretName(reconciliation.name()),
                            reconciliation.namespace(),
                            Labels.generateDefaultLabels(kafkaCr, Labels.APPLICATION_NAME, Labels.APPLICATION_NAME, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME),
                            ownerRef,
                            secretData,
                            Map.of(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(updatedCert.caCertGeneration())),
                            Map.of()
                    );
                    return secretOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.clusterOperatorCertsSecretName(reconciliation.name()), coSecret);
                })
                .thenApply(ignored -> null);
    }

    /**
     * Maybe perform a rolling update of the cluster to update the CA certificates in component truststores.
     * This is only necessary when the Cluster CA certificate has changed due to a new CA key.
     * It is not necessary when the CA certificate is renewed while retaining the existing key.
     * <p>
     * If Strimzi did not replace the CA key during the current reconciliation, {@code isClusterCaNeedFullTrust} is used to:
     *      * continue from a previous CA key replacement which didn't end successfully (i.e. CO stopped)
     *      * track key replacements when the user is managing the CA
     *
     * @return Future which completes when this step is done, either by rolling the cluster or by deciding
     *         that no rolling is needed.
     */
    CompletionStage<Void> maybeRollingUpdateForNewClusterCaKey() {
        if (clusterCa.keyReplaced() || isClusterCaNeedFullTrust) {
            RestartReason restartReason = RestartReason.CLUSTER_CA_CERT_KEY_REPLACED;
            Identity coIdentity = createCoIdentity();
            return patchClusterCaKeyGenerationAndReturnNodes()
                    .thenCompose(nodes -> rollKafkaBrokers(nodes, RestartReasons.of(restartReason), coIdentity))
                    .thenCompose(i -> rollDeploymentIfExists(KafkaResources.entityOperatorDeploymentName(reconciliation.name()), restartReason))
                    .thenCompose(i -> rollDeploymentIfExists(KafkaExporterResources.componentName(reconciliation.name()), restartReason))
                    .thenCompose(i -> rollDeploymentIfExists(CruiseControlResources.componentName(reconciliation.name()), restartReason));
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Gather the Kafka related components pods for checking Cluster CA key trust and Cluster CA certificate usage to sign servers certificate.
     * <p>
     * Verify that all the pods are already trusting the new CA certificate signed by a new CA key.
     * It checks each pod's CA key generation, compared with the new CA key generation.
     * When the trusting phase is not completed (i.e. because CO stopped), it needs to be recovered from where it was left.
     * <p>
     * Verify that all pods are already using the new CA certificate to sign server certificates.
     * It checks each pod's CA certificate generation, compared with the new CA certificate generation.
     * When the new CA certificate is used everywhere, the old CA certificate can be removed.
     */
    /* test */ CompletionStage<Void> verifyClusterCaFullyTrustedAndUsed() {
        isClusterCaNeedFullTrust = false;
        isClusterCaFullyUsed = true;

        // Building the selector for Kafka related components
        Labels labels =  Labels.forStrimziCluster(reconciliation.name()).withStrimziKind(Kafka.RESOURCE_KIND);

        return podOperator.listAsync(reconciliation.namespace(), labels)
                .thenApply(pods -> {

                    // still no Pods, a new Kafka cluster is under creation
                    if (pods.isEmpty()) {
                        isClusterCaFullyUsed = false;
                        return null;
                    }

                    int clusterCaCertGeneration = clusterCa.caCertGeneration();
                    int clusterCaKeyGeneration = clusterCa.caKeyGeneration();

                    LOGGER.debugCr(reconciliation, "Current cluster CA cert generation {}", clusterCaCertGeneration);
                    LOGGER.debugCr(reconciliation, "Current cluster CA key generation {}", clusterCaKeyGeneration);


                    for (Pod pod : pods) {
                        // with "RollingUpdate" strategy on Deployment(s) (i.e. the Cruise Control one),
                        // while the Deployment is reported to be ready, the old pod is still alive but terminating
                        // this condition is for skipping "Terminating" pods for checks on the CA key and old certificates
                        if (pod.getMetadata().getDeletionTimestamp() == null) {
                            int podClusterCaCertGeneration = Annotations.intAnnotation(pod, Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, clusterCaCertGeneration);
                            LOGGER.debugCr(reconciliation, "Pod {} has cluster CA cert generation {}", pod.getMetadata().getName(), podClusterCaCertGeneration);

                            int podClusterCaKeyGeneration = Annotations.intAnnotation(pod, Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, clusterCaKeyGeneration);
                            LOGGER.debugCr(reconciliation, "Pod {} has cluster CA key generation {} compared to the Secret CA key generation {}",
                                    pod.getMetadata().getName(), podClusterCaKeyGeneration, clusterCaKeyGeneration);

                            // only if all Kafka related components pods are updated to the new cluster CA cert generation,
                            // there is the possibility that we should remove the older cluster CA from the Secret and stores
                            if (clusterCaCertGeneration != podClusterCaCertGeneration) {
                                this.isClusterCaFullyUsed = false;
                            }

                            if (clusterCaKeyGeneration != podClusterCaKeyGeneration) {
                                this.isClusterCaNeedFullTrust = true;
                            }

                        } else {
                            LOGGER.debugCr(reconciliation, "Skipping CA key generation check on pod {}, it's terminating", pod.getMetadata().getName());
                        }

                        if (isClusterCaNeedFullTrust) {
                            LOGGER.debugCr(reconciliation, "The new Cluster CA is not yet trusted by all pods");
                        }
                        if (!isClusterCaFullyUsed) {
                            LOGGER.debugCr(reconciliation, "The old Cluster CA is still used by some server certificates and cannot be removed");
                        }
                    }
                    return null;
                });
    }

    /**
     * Patches the Kafka StrimziPodSets to update the Cluster CA key generation annotation and returns the nodes.
     *
     * @return Future containing the set of Kafka nodes which completes when the StrimziPodSets have been patched.
     */
    /* test */ CompletionStage<Set<NodeRef>> patchClusterCaKeyGenerationAndReturnNodes() {
        Labels selectorLabels = Labels.EMPTY
                .withStrimziKind(reconciliation.kind())
                .withStrimziCluster(reconciliation.name())
                .withStrimziName(KafkaResources.kafkaComponentName(reconciliation.name()));

        return strimziPodSetOperator.listAsync(reconciliation.namespace(), selectorLabels)
                .thenCompose(podSets -> {
                    if (podSets != null) {
                        List<StrimziPodSet> updatedPodSets = podSets
                                .stream()
                                .map(podSet -> WorkloadUtils.patchAnnotations(
                                        podSet,
                                        Map.of(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, String.valueOf(clusterCa.caKeyGeneration()))
                                )).toList();
                        return strimziPodSetOperator.batchReconcile(reconciliation, reconciliation.namespace(), updatedPodSets, selectorLabels)
                                .thenApply(i -> updatedPodSets.stream().flatMap(podSet -> ReconcilerUtils.nodesFromPodSet(podSet).stream())
                                .collect(Collectors.toSet()));
                    } else {
                        return CompletableFuture.completedFuture(Set.of());
                    }
                });
    }

    /* test */ CompletionStage<Void> rollKafkaBrokers(Set<NodeRef> nodes, RestartReasons podRollReasons, Identity coIdentity) {
        return createKafkaRoller(nodes, coIdentity).rollingRestart(pod -> {
            int clusterCaKeyGeneration = clusterCa.caKeyGeneration();
            int podClusterCaKeyGeneration = Annotations.intAnnotation(pod, Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, clusterCaKeyGeneration);
            if (clusterCaKeyGeneration == podClusterCaKeyGeneration) {
                LOGGER.debugCr(reconciliation, "Not rolling Pod {} since the Cluster CA cert key generation is correct.", pod.getMetadata().getName());
                return RestartReasons.empty();
            } else {
                LOGGER.debugCr(reconciliation, "Rolling Pod {} due to {}", pod.getMetadata().getName(), podRollReasons.getReasons());
                return podRollReasons;
            }
        });
    }

    /* test */ KafkaRoller createKafkaRoller(Set<NodeRef> nodes, Identity coIdentity) {
        return new KafkaRoller(reconciliation,
                podOperator,
                1_000,
                operationTimeoutMs,
                () -> new BackOff(250, 2, 10),
                nodes,
                coIdentity,
                adminClientProvider,
                kafkaAgentClientProvider,
                brokerId -> null,
                null,
                false,
                eventPublisher);
    }

    /**
     * Rolls deployments when they exist. This method is used by the CA renewal to roll deployments.
     *
     * @param deploymentName    Name of the deployment which should be rolled if it exists
     * @param reason            Reason for which it is being rolled
     *
     * @return  Succeeded future if it succeeded, failed otherwise.
     */
    /* test */ CompletionStage<Void> rollDeploymentIfExists(String deploymentName, RestartReason reason)  {
        return deploymentOperator.getAsync(reconciliation.namespace(), deploymentName)
                .thenCompose(dep -> {
                    if (dep != null) {
                        LOGGER.infoCr(reconciliation, "Rolling Deployment {} due to {}", deploymentName, reason.getDefaultNote());
                        return deploymentOperator.singlePodDeploymentRollingUpdate(reconciliation, reconciliation.namespace(), deploymentName, operationTimeoutMs);
                    } else {
                        return CompletableFuture.completedFuture(null);
                    }
                });
    }

    /**
     * Remove older cluster CA certificates if present in the corresponding Secret after a renewal by replacing the
     * corresponding CA private key.
     */
    /* test */ CompletionStage<Void> maybeRemoveOldClusterCaCertificates() {
        // if the new CA certificate is used to sign all server certificates
        if (isClusterCaFullyUsed) {
            LOGGER.debugCr(reconciliation, "Maybe there are old cluster CA certificates to remove");
            clusterCa.maybeDeleteOldCerts();

            if (clusterCa.certsRemoved()) {
                clusterCaCertSecret.setData(clusterCa.caCertData());
                return secretOperator.reconcile(reconciliation, reconciliation.namespace(), AbstractModel.clusterCaCertSecretName(reconciliation.name()), clusterCaCertSecret)
                        .thenApply(ignored -> null);
            } else {
                return CompletableFuture.completedFuture(null);
            }
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Creates the CO identity for connecting to the operands
     *
     * @return  Cluster operator identity
     */
    private Identity createCoIdentity() {
        return new Identity(
                securityContext.isStrimziTlsEncryption() ? new PemTrustSet(clusterCaCertSecret) : null,
                securityContext.isStrimziMtlsAuthentication() ? PemAuthIdentity.clusterOperator(coSecret) : null
        );
    }

    /**
     * Helper class to pass both Cluster and Clients CA as a result of the reconciliation
     *
     * @param clusterCa     The Cluster CA instance
     * @param clientsCa     The Clients CA instance
     */
    public record CaReconciliationResult(Ca clusterCa, Ca clientsCa) { }
}
