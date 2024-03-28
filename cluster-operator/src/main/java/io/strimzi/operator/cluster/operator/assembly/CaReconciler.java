/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
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
import io.strimzi.operator.cluster.operator.resource.KafkaAgentClientProvider;
import io.strimzi.operator.cluster.operator.resource.KafkaRoller;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.ZooKeeperRoller;
import io.strimzi.operator.cluster.operator.resource.ZookeeperLeaderFinder;
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
import io.strimzi.operator.common.model.InvalidResourceException;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

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
    private final PodOperator podOperator;
    private final AdminClientProvider adminClientProvider;
    private final KafkaAgentClientProvider kafkaAgentClientProvider;
    private final ZookeeperLeaderFinder zookeeperLeaderFinder;
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
        this.zookeeperLeaderFinder = supplier.zookeeperLeaderFinder;
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
                .compose(i -> rollingUpdateForNewCaKey())
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
                .compose(clusterSecrets -> vertx.executeBlocking(() -> {
                    Secret clusterCaCertSecret = null;
                    Secret clusterCaKeySecret = null;
                    Secret clientsCaCertSecret = null;
                    Secret clientsCaKeySecret = null;
                    Secret brokersSecret = null;

                    for (Secret secret : clusterSecrets) {
                        String secretName = secret.getMetadata().getName();
                        if (secretName.equals(clusterCaCertName)) {
                            clusterCaCertSecret = secret;
                        } else if (secretName.equals(clusterCaKeyName)) {
                            clusterCaKeySecret = secret;
                        } else if (secretName.equals(clientsCaCertName)) {
                            clientsCaCertSecret = secret;
                        } else if (secretName.equals(clientsCaKeyName)) {
                            clientsCaKeySecret = secret;
                        } else if (secretName.equals(KafkaResources.kafkaSecretName(reconciliation.name()))) {
                            brokersSecret = secret;
                        }
                    }

                    // When we are not supposed to generate the CA, but it does not exist, we should just throw an error
                    checkCustomCaSecret(clusterCaConfig, clusterCaCertSecret, clusterCaKeySecret, "Cluster CA");

                    clusterCa = new ClusterCa(reconciliation, certManager, passwordGenerator, reconciliation.name(), clusterCaCertSecret,
                            clusterCaKeySecret,
                            ModelUtils.getCertificateValidity(clusterCaConfig),
                            ModelUtils.getRenewalDays(clusterCaConfig),
                            clusterCaConfig == null || clusterCaConfig.isGenerateCertificateAuthority(), clusterCaConfig != null ? clusterCaConfig.getCertificateExpirationPolicy() : null);
                    clusterCa.initCaSecrets(clusterSecrets);
                    clusterCa.createRenewOrReplace(
                            reconciliation.namespace(), reconciliation.name(), caLabels,
                            clusterCaCertLabels, clusterCaCertAnnotations,
                            clusterCaConfig != null && !clusterCaConfig.isGenerateSecretOwnerReference() ? null : ownerRef,
                            Util.isMaintenanceTimeWindowsSatisfied(reconciliation, maintenanceWindows, clock.instant()));

                    // When we are not supposed to generate the CA, but it does not exist, we should just throw an error
                    checkCustomCaSecret(clientsCaConfig, clientsCaCertSecret, clientsCaKeySecret, "Clients CA");

                    clientsCa = new ClientsCa(reconciliation, certManager,
                            passwordGenerator, clientsCaCertName,
                            clientsCaCertSecret, clientsCaKeyName,
                            clientsCaKeySecret,
                            ModelUtils.getCertificateValidity(clientsCaConfig),
                            ModelUtils.getRenewalDays(clientsCaConfig),
                            clientsCaConfig == null || clientsCaConfig.isGenerateCertificateAuthority(),
                            clientsCaConfig != null ? clientsCaConfig.getCertificateExpirationPolicy() : null);
                    clientsCa.initBrokerSecret(brokersSecret);
                    clientsCa.createRenewOrReplace(reconciliation.namespace(), reconciliation.name(),
                            caLabels, Map.of(), Map.of(),
                            clientsCaConfig != null && !clientsCaConfig.isGenerateSecretOwnerReference() ? null : ownerRef,
                            Util.isMaintenanceTimeWindowsSatisfied(reconciliation, maintenanceWindows, clock.instant()));

                    return null;
                }))
                .compose(i -> {
                    Promise<Void> caUpdatePromise = Promise.promise();

                    List<Future<ReconcileResult<Secret>>> secretReconciliations = new ArrayList<>(2);

                    if (clusterCaConfig == null || clusterCaConfig.isGenerateCertificateAuthority())   {
                        Future<ReconcileResult<Secret>> clusterSecretReconciliation = secretOperator.reconcile(reconciliation, reconciliation.namespace(), clusterCaCertName, clusterCa.caCertSecret())
                                .compose(ignored -> secretOperator.reconcile(reconciliation, reconciliation.namespace(), clusterCaKeyName, clusterCa.caKeySecret()));
                        secretReconciliations.add(clusterSecretReconciliation);
                    }

                    if (clientsCaConfig == null || clientsCaConfig.isGenerateCertificateAuthority())   {
                        Future<ReconcileResult<Secret>> clientsSecretReconciliation = secretOperator.reconcile(reconciliation, reconciliation.namespace(), clientsCaCertName, clientsCa.caCertSecret())
                                .compose(ignored -> secretOperator.reconcile(reconciliation, reconciliation.namespace(), clientsCaKeyName, clientsCa.caKeySecret()));
                        secretReconciliations.add(clientsSecretReconciliation);
                    }

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
     * Utility method for checking the Secret existence when custom CA is used. The custom CA is configured but the
     * secrets do not exist, it will throw InvalidConfigurationException.
     *
     * @param ca            The CA Configuration from the Custom Resource
     * @param certSecret    Secret with the certificate public key
     * @param keySecret     Secret with the certificate private key
     * @param caDescription The name of the CA for which this check is executed ("Cluster CA" or "Clients CA" - used
     *                      in the exception message)
     */
    private void checkCustomCaSecret(CertificateAuthority ca, Secret certSecret, Secret keySecret, String caDescription)   {
        if (ca != null && !ca.isGenerateCertificateAuthority() && (certSecret == null || keySecret == null))   {
            throw new InvalidResourceException(caDescription + " should not be generated, but the secrets were not found.");
        }
    }

    /**
     * Asynchronously reconciles the cluster operator Secret used to connect to Kafka and ZooKeeper.
     * This only updates the Secret if the latest Cluster CA is fully trusted across the cluster, otherwise if
     * something goes wrong during reconciliation when the next loop starts it won't be able to connect to
     * Kafka and ZooKeeper anymore.
     *
     * @param clock    The clock for supplying the reconciler with the time instant of each reconciliation cycle.
                       That time is used for checking maintenance windows
     */
    Future<Void> reconcileClusterOperatorSecret(Clock clock) {
        coSecret = clusterCa.clusterOperatorSecret();
        if (coSecret != null && this.isClusterCaNeedFullTrust) {
            LOGGER.warnCr(reconciliation, "Cluster CA needs to be fully trusted across the cluster, keeping current CO secret and certs");
            return Future.succeededFuture();
        }
        coSecret = CertUtils.buildTrustedCertificateSecret(
                reconciliation,
                clusterCa,
                clusterCa.clusterOperatorSecret(),
                reconciliation.namespace(),
                KafkaResources.secretName(reconciliation.name()),
                "cluster-operator",
                "cluster-operator",
                clusterOperatorSecretLabels,
                ownerRef,
                Util.isMaintenanceTimeWindowsSatisfied(reconciliation, maintenanceWindows, clock.instant())
        );

        return secretOperator.reconcile(reconciliation, reconciliation.namespace(), KafkaResources.secretName(reconciliation.name()), coSecret)
                .map((Void) null);
    }

    /**
     * Perform a rolling update of the cluster so that CA certificates get added to their truststores, or expired CA
     * certificates get removed from their truststores. Note this is only necessary when the CA certificate has changed
     * due to a new CA key. It is not necessary when the CA certificate is replace while retaining the existing key.
     */
    Future<Void> rollingUpdateForNewCaKey() {
        RestartReasons podRollReasons = RestartReasons.empty();

        // cluster CA needs to be fully trusted
        // it is coming from a cluster CA key replacement which didn't end successfully (i.e. CO stopped) and we need to continue from there
        if (clusterCa.keyReplaced() || isClusterCaNeedFullTrust) {
            podRollReasons.add(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED);
        }

        if (clientsCa.keyReplaced()) {
            podRollReasons.add(RestartReason.CLIENT_CA_CERT_KEY_REPLACED);
        }

        if (podRollReasons.shouldRestart()) {
            TlsPemIdentity coTlsPemIdentity = new TlsPemIdentity(new PemTrustSet(clusterCa.caCertSecret()), PemAuthIdentity.clusterOperator(coSecret));
            return getZooKeeperReplicas()
                    .compose(replicas -> maybeRollZookeeper(replicas, podRollReasons, coTlsPemIdentity))
                    .compose(i -> getKafkaReplicas())
                    .compose(nodes -> rollKafkaBrokers(nodes, podRollReasons, coTlsPemIdentity))
                    .compose(i -> maybeRollDeploymentIfExists(KafkaResources.entityOperatorDeploymentName(reconciliation.name()), podRollReasons))
                    .compose(i -> maybeRollDeploymentIfExists(KafkaExporterResources.componentName(reconciliation.name()), podRollReasons))
                    .compose(i -> maybeRollDeploymentIfExists(CruiseControlResources.componentName(reconciliation.name()), podRollReasons));
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Gather the Kafka related components pods for checking CA key trust and CA certificate usage to sign servers certificate.
     *
     * Verify that all the pods are already trusting the new CA certificate signed by a new CA key.
     * It checks each pod's CA key generation, compared with the new CA key generation.
     * When the trusting phase is not completed (i.e. because CO stopped), it needs to be recovered from where it was left.
     *
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

                    int clusterCaCertGeneration = clusterCa.certGeneration();
                    int clusterCaKeyGeneration = clusterCa.keyGeneration();

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
     * If we need to roll the ZooKeeper cluster to roll out the trust to a new CA certificate when a CA private key is
     * being replaced, we need to know what the current number of ZooKeeper nodes is. Getting it from the Kafka custom
     * resource might not be good enough if a scale-up /scale-down is happening at the same time. So we get the
     * StrimziPodSet and find out the correct number of ZooKeeper nodes from it.
     *
     * @return  Current number of ZooKeeper replicas
     */
    /* test */ Future<Integer> getZooKeeperReplicas() {
        return strimziPodSetOperator.getAsync(reconciliation.namespace(), KafkaResources.zookeeperComponentName(reconciliation.name()))
                .compose(podSet -> {
                    if (podSet != null
                            && podSet.getSpec() != null
                            && podSet.getSpec().getPods() != null) {
                        return Future.succeededFuture(podSet.getSpec().getPods().size());
                    } else {
                        return Future.succeededFuture(0);
                    }
                });
    }

    /**
     * Checks whether the ZooKeeper cluster needs ot be rolled to trust the new CA private key. ZooKeeper uses only the
     * Cluster CA and not the Clients CA. So the rolling happens only when Cluster CA private key changed.
     *
     * @param replicas              Current number of ZooKeeper replicas
     * @param podRestartReasons     List of reasons to restart the pods
     * @param coTlsPemIdentity      Trust set and identity for TLS client authentication for connecting to ZooKeeper
     *
     * @return  Future which completes when this step is done either by rolling the ZooKeeper cluster or by deciding
     *          that no rolling is needed.
     */
    /* test */ Future<Void> maybeRollZookeeper(int replicas, RestartReasons podRestartReasons, TlsPemIdentity coTlsPemIdentity) {
        if (podRestartReasons.contains(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED)) {
            Labels zkSelectorLabels = Labels.EMPTY
                    .withStrimziKind(reconciliation.kind())
                    .withStrimziCluster(reconciliation.name())
                    .withStrimziName(KafkaResources.zookeeperComponentName(reconciliation.name()));

            Function<Pod, List<String>> rollZkPodAndLogReason = pod -> {
                List<String> reason = List.of(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED.getDefaultNote());
                LOGGER.debugCr(reconciliation, "Rolling Pod {} to {}", pod.getMetadata().getName(), reason);
                return reason;
            };
            return new ZooKeeperRoller(podOperator, zookeeperLeaderFinder, operationTimeoutMs)
                    .maybeRollingUpdate(reconciliation, replicas, zkSelectorLabels, rollZkPodAndLogReason, coTlsPemIdentity);
        } else {
            return Future.succeededFuture();
        }
    }

    /* test */ Future<Set<NodeRef>> getKafkaReplicas() {
        Labels selectorLabels = Labels.EMPTY
                .withStrimziKind(reconciliation.kind())
                .withStrimziCluster(reconciliation.name())
                .withStrimziName(KafkaResources.kafkaComponentName(reconciliation.name()));

        return strimziPodSetOperator.listAsync(reconciliation.namespace(), selectorLabels)
                .compose(podSets -> {
                    Set<NodeRef> nodes = new LinkedHashSet<>();

                    if (podSets != null) {
                        for (StrimziPodSet podSet : podSets) {
                            nodes.addAll(ReconcilerUtils.nodesFromPodSet(podSet));
                        }
                    }

                    return Future.succeededFuture(nodes);
                });
    }

    /* test */ Future<Void> rollKafkaBrokers(Set<NodeRef> nodes, RestartReasons podRollReasons, TlsPemIdentity coTlsPemIdentity) {
        return new KafkaRoller(
                reconciliation,
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
                null,
                false,
                eventPublisher
        ).rollingRestart(pod -> {
            LOGGER.debugCr(reconciliation, "Rolling Pod {} due to {}", pod.getMetadata().getName(), podRollReasons.getReasons());
            return podRollReasons;
        });
    }

    // Entity Operator, Kafka Exporter, and Cruise Control are only rolled when the cluster CA cert key is replaced
    Future<Void> maybeRollDeploymentIfExists(String deploymentName, RestartReasons podRollReasons) {
        if (podRollReasons.contains(RestartReason.CLUSTER_CA_CERT_KEY_REPLACED)) {
            return rollDeploymentIfExists(deploymentName, RestartReason.CLUSTER_CA_CERT_KEY_REPLACED.getDefaultNote());
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Rolls deployments when they exist. This method is used by the CA renewal to roll deployments.
     *
     * @param deploymentName    Name of the deployment which should be rolled if it exists
     * @param reason           Reason for which it is being rolled
     *
     * @return  Succeeded future if it succeeded, failed otherwise.
     */
    Future<Void> rollDeploymentIfExists(String deploymentName, String reason)  {
        return deploymentOperator.getAsync(reconciliation.namespace(), deploymentName)
                .compose(dep -> {
                    if (dep != null) {
                        LOGGER.infoCr(reconciliation, "Rolling Deployment {} due to {}", deploymentName, reason);
                        return deploymentOperator.rollingUpdate(reconciliation, reconciliation.namespace(), deploymentName, operationTimeoutMs);
                    } else {
                        return Future.succeededFuture();
                    }
                });
    }

    /**
     * Remove older cluster CA certificates if present in the corresponding Secret after a renewal by replacing the
     * corresponding CA private key.
     */
    Future<Void> maybeRemoveOldClusterCaCertificates() {
        // if the new CA certificate is used to sign all server certificates
        if (isClusterCaFullyUsed) {
            LOGGER.debugCr(reconciliation, "Maybe there are old cluster CA certificates to remove");
            clusterCa.maybeDeleteOldCerts();

            if (clusterCa.certsRemoved()) {
                return secretOperator.reconcile(reconciliation, reconciliation.namespace(), AbstractModel.clusterCaCertSecretName(reconciliation.name()), clusterCa.caCertSecret())
                        .map((Void) null);
            } else {
                return Future.succeededFuture();
            }
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Helper class to pass both Cluster and Clients CA as a result of the reconciliation
     */
    public record CaReconciliationResult(ClusterCa clusterCa, ClientsCa clientsCa) { }
}
