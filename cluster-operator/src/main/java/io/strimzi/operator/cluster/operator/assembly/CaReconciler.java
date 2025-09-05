/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.ResourceAnnotations;
import io.strimzi.api.kafka.model.common.CertificateAuthority;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.api.kafka.model.kafka.exporter.KafkaExporterResources;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.CertUtils;
import io.strimzi.operator.cluster.model.ClusterCa;
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
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.auth.PemAuthIdentity;
import io.strimzi.operator.common.auth.PemTrustSet;
import io.strimzi.operator.common.auth.TlsPemIdentity;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.ClientsCa;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.strimzi.operator.common.model.Ca.ANNO_STRIMZI_IO_CA_CERT_GENERATION;
import static io.strimzi.operator.common.model.Ca.ANNO_STRIMZI_IO_CA_KEY_GENERATION;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * Class used for reconciliation of Cluster and Client CAs. This class contains both the steps of the CA reconciliation
 * pipeline and is also used to store the state between them.
 */
public class CaReconciler {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(CaReconciler.class.getName());

    /* test */ final Reconciliation reconciliation;
    private final Vertx vertx;
    private final long operationTimeoutMs;

    /* test */ final DeploymentOperator deploymentOperator;
    private final StrimziPodSetOperator strimziPodSetOperator;
    private final SecretOperator secretOperator;
    /* test */ final PodOperator podOperator;
    private final AdminClientProvider adminClientProvider;
    private final KafkaAgentClientProvider kafkaAgentClientProvider;
    private final CertManager certManager;
    private final PasswordGenerator passwordGenerator;
    private final KubernetesRestartEventPublisher eventPublisher;

    // Fields based on the Kafka CR required for the reconciliation
    private final List<String> maintenanceWindows;
    private final OwnerReference ownerRef;
    private final CertificateAuthority clusterCaConfig;
    private final CertificateAuthority clientsCaConfig;
    private final Map<String, String> caLabels;
    private final Labels clusterOperatorSecretLabels;
    private final Map<String, String> clusterCaCertLabels;
    private final Map<String, String> clusterCaCertAnnotations;

    // Fields used to store state during the reconciliation
    private ClusterCa clusterCa;
    private ClientsCa clientsCa;
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
     * @param vertx             Vert.x instance
     * @param certManager       Certificate Manager for managing certificates
     * @param passwordGenerator Password generator for generating passwords
     */
    public CaReconciler(
            Reconciliation reconciliation,
            Kafka kafkaCr,
            ClusterOperatorConfig config,
            ResourceOperatorSupplier supplier,
            Vertx vertx,
            CertManager certManager,
            PasswordGenerator passwordGenerator
    ) {
        this.reconciliation = reconciliation;
        this.vertx = vertx;
        this.operationTimeoutMs = config.getOperationTimeoutMs();

        this.deploymentOperator = supplier.deploymentOperations;
        this.strimziPodSetOperator = supplier.strimziPodSetOperator;
        this.secretOperator = supplier.secretOperations;
        this.podOperator = supplier.podOperations;

        this.adminClientProvider = supplier.adminClientProvider;
        this.kafkaAgentClientProvider = supplier.kafkaAgentClientProvider;
        this.certManager = certManager;
        this.passwordGenerator = passwordGenerator;

        this.eventPublisher = supplier.restartEventsPublisher;

        // Extract required information from the Kafka CR
        this.maintenanceWindows = kafkaCr.getSpec().getMaintenanceTimeWindows();
        this.ownerRef = new OwnerReferenceBuilder()
                .withApiVersion(kafkaCr.getApiVersion())
                .withKind(kafkaCr.getKind())
                .withName(kafkaCr.getMetadata().getName())
                .withUid(kafkaCr.getMetadata().getUid())
                .withBlockOwnerDeletion(false)
                .withController(false)
                .build();
        this.clusterCaConfig = kafkaCr.getSpec().getClusterCa();
        this.clientsCaConfig = kafkaCr.getSpec().getClientsCa();
        this.caLabels = Labels.generateDefaultLabels(kafkaCr, Labels.APPLICATION_NAME, "certificate-authority", AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME).toMap();
        this.clusterOperatorSecretLabels = Labels.generateDefaultLabels(kafkaCr, Labels.APPLICATION_NAME, Labels.APPLICATION_NAME, AbstractModel.STRIMZI_CLUSTER_OPERATOR_NAME);
        this.clusterCaCertLabels = clusterCaCertLabels(kafkaCr);
        this.clusterCaCertAnnotations = clusterCaCertAnnotations(kafkaCr);
    }

    /**
     * Utility method to extract the template labels from the Kafka CR.
     *
     * @param kafkaCr   Kafka CR
     *
     * @return  Map with the labels from the Kafka CR or empty map if the template is not set
     */
    private static Map<String, String> clusterCaCertLabels(Kafka kafkaCr)    {
        if (kafkaCr.getSpec().getKafka() != null
                && kafkaCr.getSpec().getKafka().getTemplate() != null
                && kafkaCr.getSpec().getKafka().getTemplate().getClusterCaCert() != null
                && kafkaCr.getSpec().getKafka().getTemplate().getClusterCaCert().getMetadata() != null
                && kafkaCr.getSpec().getKafka().getTemplate().getClusterCaCert().getMetadata().getLabels() != null) {
            return kafkaCr.getSpec().getKafka().getTemplate().getClusterCaCert().getMetadata().getLabels();
        } else {
            return Map.of();
        }
    }

    /**
     * Utility method to extract the template annotations from the Kafka CR.
     *
     * @param kafkaCr   Kafka CR
     *
     * @return  Map with the annotation from the Kafka CR or empty map if the template is not set
     */
    private static Map<String, String> clusterCaCertAnnotations(Kafka kafkaCr)    {
        if (kafkaCr.getSpec().getKafka() != null
                && kafkaCr.getSpec().getKafka().getTemplate() != null
                && kafkaCr.getSpec().getKafka().getTemplate().getClusterCaCert() != null
                && kafkaCr.getSpec().getKafka().getTemplate().getClusterCaCert().getMetadata() != null
                && kafkaCr.getSpec().getKafka().getTemplate().getClusterCaCert().getMetadata().getAnnotations() != null) {
            return kafkaCr.getSpec().getKafka().getTemplate().getClusterCaCert().getMetadata().getAnnotations();
        } else {
            return Map.of();
        }
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
    public Future<CaReconciliationResult> reconcile(Clock clock)    {
        return reconcileCas(clock)
                .compose(i -> verifyClusterCaFullyTrustedAndUsed())
                .compose(i -> reconcileClusterOperatorSecret(clock))
                .compose(i -> maybeRollingUpdateForNewClusterCaKey())
                .compose(i -> maybeRemoveOldClusterCaCertificates())
                .map(i -> new CaReconciliationResult(clusterCa, clientsCa));
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
    @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    Future<Void> reconcileCas(Clock clock) {
        String clusterCaCertName = AbstractModel.clusterCaCertSecretName(reconciliation.name());
        String clusterCaKeyName = AbstractModel.clusterCaKeySecretName(reconciliation.name());
        String clientsCaCertName = KafkaResources.clientsCaCertificateSecretName(reconciliation.name());
        String clientsCaKeyName = KafkaResources.clientsCaKeySecretName(reconciliation.name());

        return secretOperator.listAsync(reconciliation.namespace(), Labels.EMPTY.withStrimziKind(reconciliation.kind()).withStrimziCluster(reconciliation.name()))
                .compose(clusterSecrets -> {
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
                        }
                    }

                    boolean generateClusterCa = clusterCaConfig == null || clusterCaConfig.isGenerateCertificateAuthority();
                    boolean generateClientsCa = clientsCaConfig == null || clientsCaConfig.isGenerateCertificateAuthority();

                    clusterCa = new ClusterCa(reconciliation, certManager, passwordGenerator,
                            reconciliation.name(),
                            existingClusterCaCertSecret,
                            existingClusterCaKeySecret,
                            ModelUtils.getCertificateValidity(clusterCaConfig),
                            ModelUtils.getRenewalDays(clusterCaConfig),
                            generateClusterCa,
                            clusterCaConfig != null ? clusterCaConfig.getCertificateExpirationPolicy() : null);


                    clientsCa = new ClientsCa(reconciliation, certManager, passwordGenerator,
                            clientsCaCertName, existingClientsCaCertSecret,
                            clientsCaKeyName, existingClientsCaKeySecret,
                            ModelUtils.getCertificateValidity(clientsCaConfig),
                            ModelUtils.getRenewalDays(clientsCaConfig),
                            generateClientsCa,
                            clientsCaConfig != null ? clientsCaConfig.getCertificateExpirationPolicy() : null);

                    List<Future<ReconcileResult<Secret>>> secretReconciliations = new ArrayList<>(4);

                    if (generateClusterCa)   {
                        clusterCa.createRenewOrReplace(Util.isMaintenanceTimeWindowsSatisfied(reconciliation, maintenanceWindows, clock.instant()),
                                isForceReplace(existingClusterCaKeySecret),
                                isForceRenew(existingClusterCaCertSecret));

                        OwnerReference ownerReference = clusterCaConfig != null && !clusterCaConfig.isGenerateSecretOwnerReference() ? null : ownerRef;

                        clusterCaCertSecret = createCaCertSecret(clusterCaCertName, clusterCaCertLabels, clusterCaCertAnnotations, ownerReference, clusterCa, existingClusterCaCertSecret);
                        secretReconciliations.add(secretOperator.reconcile(reconciliation, reconciliation.namespace(), clusterCaCertName, clusterCaCertSecret));

                        Secret clusterCaKeySecret = createCaKeySecret(clusterCaKeyName, ownerReference, clusterCa, existingClusterCaKeySecret);
                        secretReconciliations.add(secretOperator.reconcile(reconciliation, reconciliation.namespace(), clusterCaKeyName, clusterCaKeySecret));
                    } else {
                        clusterCaCertSecret = existingClusterCaCertSecret;
                    }

                    if (generateClientsCa)   {
                        clientsCa.createRenewOrReplace(Util.isMaintenanceTimeWindowsSatisfied(reconciliation, maintenanceWindows, clock.instant()),
                                isForceReplace(existingClientsCaKeySecret),
                                isForceRenew(existingClientsCaCertSecret));

                        OwnerReference ownerReference = clientsCaConfig != null && !clientsCaConfig.isGenerateSecretOwnerReference() ? null : ownerRef;

                        Secret clientsCaCertSecret = createCaCertSecret(clientsCaCertName, Map.of(), Map.of(), ownerReference, clientsCa, existingClientsCaCertSecret);
                        secretReconciliations.add(secretOperator.reconcile(reconciliation, reconciliation.namespace(), clientsCaCertName, clientsCaCertSecret));

                        Secret clientsCaKeySecret = createCaKeySecret(clientsCaKeyName, ownerReference, clientsCa, existingClientsCaKeySecret);
                        secretReconciliations.add(secretOperator.reconcile(reconciliation, reconciliation.namespace(), clientsCaKeyName, clientsCaKeySecret));
                    }

                    Promise<Void> caUpdatePromise = Promise.promise();

                    Future.join(secretReconciliations).onComplete(res -> {
                        if (res.succeeded())    {
                            caUpdatePromise.complete();
                        } else {
                            caUpdatePromise.fail(res.cause());
                        }
                    });

                    return caUpdatePromise.future();
                });
    }

    /**
     * Asynchronously reconciles the cluster operator Secret used to connect to Kafka.
     * This only updates the Secret if the latest Cluster CA is fully trusted across the cluster, otherwise if
     * something goes wrong during reconciliation when the next loop starts it won't be able to connect to Kafka.
     *
     * @param clock    The clock for supplying the reconciler with the time instant of each reconciliation cycle.
     *                 That time is used for checking maintenance windows
     */
    Future<Void> reconcileClusterOperatorSecret(Clock clock) {
        return secretOperator.getAsync(reconciliation.namespace(), KafkaResources.clusterOperatorCertsSecretName(reconciliation.name()))
                .compose(oldSecret -> {
                    coSecret = oldSecret;
                    if (oldSecret != null && this.isClusterCaNeedFullTrust) {
                        LOGGER.warnCr(reconciliation, "Cluster CA needs to be fully trusted across the cluster, keeping current CO secret and certs");
                        return Future.succeededFuture();
                    }

                    coSecret = CertUtils.buildTrustedCertificateSecret(
                            reconciliation,
                            clusterCa,
                            coSecret,
                            reconciliation.namespace(),
                            KafkaResources.clusterOperatorCertsSecretName(reconciliation.name()),
                            "cluster-operator",
                            "cluster-operator",
                            clusterOperatorSecretLabels,
                            ownerRef,
                            Util.isMaintenanceTimeWindowsSatisfied(reconciliation, maintenanceWindows, clock.instant())
                    );

                    return secretOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.clusterOperatorCertsSecretName(reconciliation.name()), coSecret)
                            .mapEmpty();
                });
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
    Future<Void> maybeRollingUpdateForNewClusterCaKey() {
        if (clusterCa.keyReplaced() || isClusterCaNeedFullTrust) {
            RestartReason restartReason = RestartReason.CLUSTER_CA_CERT_KEY_REPLACED;
            TlsPemIdentity coTlsPemIdentity = new TlsPemIdentity(new PemTrustSet(clusterCaCertSecret), PemAuthIdentity.clusterOperator(coSecret));
            return patchClusterCaKeyGenerationAndReturnNodes()
                    .compose(nodes -> rollKafkaBrokers(nodes, RestartReasons.of(restartReason), coTlsPemIdentity))
                    .compose(i -> rollDeploymentIfExists(KafkaResources.entityOperatorDeploymentName(reconciliation.name()), restartReason))
                    .compose(i -> rollDeploymentIfExists(KafkaExporterResources.componentName(reconciliation.name()), restartReason))
                    .compose(i -> rollDeploymentIfExists(CruiseControlResources.componentName(reconciliation.name()), restartReason));
        } else {
            return Future.succeededFuture();
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
    /* test */ Future<Void> verifyClusterCaFullyTrustedAndUsed() {
        isClusterCaNeedFullTrust = false;
        isClusterCaFullyUsed = true;

        // Building the selector for Kafka related components
        Labels labels =  Labels.forStrimziCluster(reconciliation.name()).withStrimziKind(Kafka.RESOURCE_KIND);

        return podOperator.listAsync(reconciliation.namespace(), labels)
                .compose(pods -> {

                    // still no Pods, a new Kafka cluster is under creation
                    if (pods.isEmpty()) {
                        isClusterCaFullyUsed = false;
                        return Future.succeededFuture();
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
                    return Future.succeededFuture();
                });
    }

    /**
     * Patches the Kafka StrimziPodSets to update the Cluster CA key generation annotation and returns the nodes.
     *
     * @return Future containing the set of Kafka nodes which completes when the StrimziPodSets have been patched.
     */
    /* test */ Future<Set<NodeRef>> patchClusterCaKeyGenerationAndReturnNodes() {
        Labels selectorLabels = Labels.EMPTY
                .withStrimziKind(reconciliation.kind())
                .withStrimziCluster(reconciliation.name())
                .withStrimziName(KafkaResources.kafkaComponentName(reconciliation.name()));

        return strimziPodSetOperator.listAsync(reconciliation.namespace(), selectorLabels)
                .compose(podSets -> {
                    if (podSets != null) {
                        List<StrimziPodSet> updatedPodSets = podSets
                                .stream()
                                .map(podSet -> WorkloadUtils.patchAnnotations(
                                        podSet,
                                        Map.of(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, String.valueOf(clusterCa.caKeyGeneration()))
                                )).toList();
                        return strimziPodSetOperator.batchReconcile(reconciliation, reconciliation.namespace(), updatedPodSets, selectorLabels)
                                .map(i -> updatedPodSets.stream().flatMap(podSet -> ReconcilerUtils.nodesFromPodSet(podSet).stream())
                                .collect(Collectors.toSet()));
                    } else {
                        return Future.succeededFuture(Set.of());
                    }
                });
    }

    /* test */ Future<Void> rollKafkaBrokers(Set<NodeRef> nodes, RestartReasons podRollReasons, TlsPemIdentity coTlsPemIdentity) {
        return createKafkaRoller(nodes, coTlsPemIdentity).rollingRestart(pod -> {
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

    /* test */ KafkaRoller createKafkaRoller(Set<NodeRef> nodes, TlsPemIdentity coTlsPemIdentity) {
        return new KafkaRoller(reconciliation,
                vertx,
                podOperator,
                1_000,
                operationTimeoutMs,
                () -> new BackOff(250, 2, 10),
                nodes,
                coTlsPemIdentity,
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
    /* test */ Future<Void> rollDeploymentIfExists(String deploymentName, RestartReason reason)  {
        return deploymentOperator.getAsync(reconciliation.namespace(), deploymentName)
                .compose(dep -> {
                    if (dep != null) {
                        LOGGER.infoCr(reconciliation, "Rolling Deployment {} due to {}", deploymentName, reason.getDefaultNote());
                        return deploymentOperator.singlePodDeploymentRollingUpdate(reconciliation, reconciliation.namespace(), deploymentName, operationTimeoutMs);
                    } else {
                        return Future.succeededFuture();
                    }
                });
    }

    /**
     * Remove older cluster CA certificates if present in the corresponding Secret after a renewal by replacing the
     * corresponding CA private key.
     */
    /* test */ Future<Void> maybeRemoveOldClusterCaCertificates() {
        // if the new CA certificate is used to sign all server certificates
        if (isClusterCaFullyUsed) {
            LOGGER.debugCr(reconciliation, "Maybe there are old cluster CA certificates to remove");
            clusterCa.maybeDeleteOldCerts();

            if (clusterCa.certsRemoved()) {
                clusterCaCertSecret.setData(clusterCa.caCertData());
                return secretOperator.reconcile(reconciliation, reconciliation.namespace(), AbstractModel.clusterCaCertSecretName(reconciliation.name()), clusterCaCertSecret)
                        .mapEmpty();
            } else {
                return Future.succeededFuture();
            }
        } else {
            return Future.succeededFuture();
        }
    }

    private boolean isForceReplace(Secret caSecret) {
        if (caSecret != null && caSecret.getMetadata() != null &&
                Annotations.hasAnnotation(caSecret, ResourceAnnotations.ANNO_STRIMZI_IO_FORCE_REPLACE)) {
            return Annotations.booleanAnnotation(caSecret, ResourceAnnotations.ANNO_STRIMZI_IO_FORCE_REPLACE, false);
        } else {
            return false;
        }
    }

    private boolean isForceRenew(Secret caSecret) {
        if (caSecret != null && caSecret.getMetadata() != null &&
                Annotations.hasAnnotation(caSecret, ResourceAnnotations.ANNO_STRIMZI_IO_FORCE_RENEW)) {
            return Annotations.booleanAnnotation(caSecret, ResourceAnnotations.ANNO_STRIMZI_IO_FORCE_RENEW, false);
        } else {
            return false;
        }
    }

    private Secret createCaCertSecret(String secretName, Map<String, String> additionalLabels, Map<String, String> additionalAnnotations,
                                      OwnerReference ownerReference, Ca ca, Secret existingCaCertSecret) {
        Map<String, String> certAnnotations = new HashMap<>(2);
        certAnnotations.put(ANNO_STRIMZI_IO_CA_CERT_GENERATION, String.valueOf(ca.caCertGeneration()));

        if (ca.postponed()
                && existingCaCertSecret != null
                && Annotations.hasAnnotation(existingCaCertSecret, Annotations.ANNO_STRIMZI_IO_FORCE_RENEW))   {
            certAnnotations.put(Annotations.ANNO_STRIMZI_IO_FORCE_RENEW, Annotations.stringAnnotation(existingCaCertSecret, Annotations.ANNO_STRIMZI_IO_FORCE_RENEW, "false"));
        }
        return createCaSecret(secretName, ca.caCertData(), Util.mergeLabelsOrAnnotations(caLabels, additionalLabels),
                Util.mergeLabelsOrAnnotations(certAnnotations, additionalAnnotations), ownerReference);

    }

    private Secret createCaKeySecret(String secretName, OwnerReference ownerReference, Ca ca, Secret existingCaKeySecret) {
        Map<String, String> keyAnnotations = new HashMap<>(2);
        keyAnnotations.put(ANNO_STRIMZI_IO_CA_KEY_GENERATION, String.valueOf(ca.caKeyGeneration()));

        if (ca.postponed()
                && existingCaKeySecret != null
                && Annotations.hasAnnotation(existingCaKeySecret, Annotations.ANNO_STRIMZI_IO_FORCE_REPLACE))   {
            keyAnnotations.put(Annotations.ANNO_STRIMZI_IO_FORCE_REPLACE, Annotations.stringAnnotation(existingCaKeySecret, Annotations.ANNO_STRIMZI_IO_FORCE_REPLACE, "false"));
        }
        return createCaSecret(secretName, ca.caKeyData(), caLabels, keyAnnotations, ownerReference);

    }

    private Secret createCaSecret(String name, Map<String, String> data,
                                         Map<String, String> labels, Map<String, String> annotations, OwnerReference ownerReference) {
        List<OwnerReference> or = ownerReference != null ? singletonList(ownerReference) : emptyList();
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withNamespace(reconciliation.namespace())
                    .withLabels(labels)
                    .withAnnotations(annotations)
                    .withOwnerReferences(or)
                .endMetadata()
                .withType("Opaque")
                .withData(data)
                .build();
    }

    /**
     * Helper class to pass both Cluster and Clients CA as a result of the reconciliation
     *
     * @param clusterCa     The Cluster CA instance
     * @param clientsCa     The Clients CA instance
     */
    public record CaReconciliationResult(ClusterCa clusterCa, ClientsCa clientsCa) { }
}
