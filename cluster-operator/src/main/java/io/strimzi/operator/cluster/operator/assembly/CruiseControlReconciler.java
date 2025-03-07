/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.assembly;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.kafka.cruisecontrol.CruiseControlResources;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.CertUtils;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.CruiseControl;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.DeploymentOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.NetworkPolicyOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceAccountOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceOperator;
import io.strimzi.operator.common.Annotations;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.model.Ca;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.vertx.core.Future;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class used for reconciliation of Cruise Control. This class contains both the steps of the Cruise Control
 * reconciliation pipeline and is also used to store the state between them.
 */
public class CruiseControlReconciler {
    private final Reconciliation reconciliation;
    private final CruiseControl cruiseControl;
    private final ClusterCa clusterCa;
    private final List<String> maintenanceWindows;
    private final long operationTimeoutMs;
    private final String operatorNamespace;
    private final Labels operatorNamespaceLabels;
    private final boolean isNetworkPolicyGeneration;
    private final boolean isTopicOperatorEnabled;

    private final DeploymentOperator deploymentOperator;
    private final SecretOperator secretOperator;
    private final ServiceAccountOperator serviceAccountOperator;
    private final ServiceOperator serviceOperator;
    private final NetworkPolicyOperator networkPolicyOperator;
    private final ConfigMapOperator configMapOperator;
    private final PasswordGenerator passwordGenerator;

    private String certificateHash = "";
    private String serverConfigurationHash = "";
    private String capacityConfigurationHash = "";
    private String apiSecretHash = "";
    
    /**
     * Constructs the Cruise Control reconciler
     *
     * @param reconciliation            Reconciliation marker
     * @param config                    Cluster Operator Configuration
     * @param supplier                  Supplier with Kubernetes Resource Operators
     * @param passwordGenerator         The password generator for API users
     * @param kafkaAssembly             The Kafka custom resource
     * @param versions                  The supported Kafka versions
     * @param kafkaBrokerNodes          List of the broker nodes which are part of the Kafka cluster
     * @param kafkaBrokerStorage        A map with storage configuration used by the Kafka cluster and its broker pools
     * @param kafkaBrokerResources      A map with resource configuration used by the Kafka cluster and its broker pools
     * @param clusterCa                 The Cluster CA instance
     */
    @SuppressWarnings({"checkstyle:ParameterNumber"})
    public CruiseControlReconciler(
            Reconciliation reconciliation,
            ClusterOperatorConfig config,
            ResourceOperatorSupplier supplier,
            PasswordGenerator passwordGenerator,
            Kafka kafkaAssembly,
            KafkaVersion.Lookup versions,
            Set<NodeRef> kafkaBrokerNodes,
            Map<String, Storage> kafkaBrokerStorage,
            Map<String, ResourceRequirements> kafkaBrokerResources,
            ClusterCa clusterCa
    ) {
        this.reconciliation = reconciliation;
        this.cruiseControl = CruiseControl.fromCrd(reconciliation, kafkaAssembly, versions, kafkaBrokerNodes, kafkaBrokerStorage, 
            kafkaBrokerResources, supplier.sharedEnvironmentProvider);
        this.clusterCa = clusterCa;
        this.maintenanceWindows = kafkaAssembly.getSpec().getMaintenanceTimeWindows();
        this.operationTimeoutMs = config.getOperationTimeoutMs();
        this.operatorNamespace = config.getOperatorNamespace();
        this.operatorNamespaceLabels = config.getOperatorNamespaceLabels();
        this.isNetworkPolicyGeneration = config.isNetworkPolicyGeneration();
        this.passwordGenerator = passwordGenerator;
        this.isTopicOperatorEnabled = kafkaAssembly.getSpec().getEntityOperator() != null 
            && kafkaAssembly.getSpec().getEntityOperator().getTopicOperator() != null;
        this.deploymentOperator = supplier.deploymentOperations;
        this.secretOperator = supplier.secretOperations;
        this.serviceAccountOperator = supplier.serviceAccountOperations;
        this.serviceOperator = supplier.serviceOperations;
        this.networkPolicyOperator = supplier.networkPolicyOperator;
        this.configMapOperator = supplier.configMapOperations;
    }

    /**
     * The main reconciliation method which triggers the whole reconciliation pipeline. This is the method which is
     * expected to be called from the outside to trigger the reconciliation.
     *
     * @param isOpenShift       Flag indicating whether we are on OpenShift or not
     * @param imagePullPolicy   Image pull policy
     * @param imagePullSecrets  List of Image pull secrets
     * @param clock             The clock for supplying the reconciler with the time instant of each reconciliation cycle.
     *                          That time is used for checking maintenance windows
     *
     * @return                  Future which completes when the reconciliation completes
     */
    public Future<Void> reconcile(boolean isOpenShift, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets, Clock clock)    {
        return networkPolicy()
                .compose(i -> serviceAccount())
                .compose(i -> configMap())
                .compose(i -> certificatesSecret(clock))
                .compose(i -> apiSecret())
                .compose(i -> service())
                .compose(i -> deployment(isOpenShift, imagePullPolicy, imagePullSecrets))
                .compose(i -> waitForDeploymentReadiness());
    }

    /**
     * Manages the Cruise Control Network Policies.
     *
     * @return  Future which completes when the reconciliation is done
     */
    protected Future<Void> networkPolicy() {
        if (isNetworkPolicyGeneration) {
            return networkPolicyOperator
                    .reconcile(
                            reconciliation,
                            reconciliation.namespace(),
                            CruiseControlResources.networkPolicyName(reconciliation.name()),
                            cruiseControl != null ? cruiseControl.generateNetworkPolicy(
                                operatorNamespace, operatorNamespaceLabels, isTopicOperatorEnabled) : null
                    ).mapEmpty();
        } else {
            return Future.succeededFuture();
        }
    }

    /**
     * Manages the Cruise Control Service Account.
     *
     * @return  Future which completes when the reconciliation is done
     */
    protected Future<Void> serviceAccount() {
        return serviceAccountOperator
                .reconcile(
                        reconciliation,
                        reconciliation.namespace(),
                        CruiseControlResources.serviceAccountName(reconciliation.name()),
                        cruiseControl != null ? cruiseControl.generateServiceAccount() : null
                ).mapEmpty();
    }

    /**
     * Manages the Cruise Control ConfigMap which contains the following:
     * (1) Cruise Control server configuration
     * (2) Cruise Control broker capacity configuration
     * (3) Cruise Control server logging and metrics configuration
     *
     * @return Future which completes when the reconciliation is done
     */
    protected Future<Void> configMap() {
        if (cruiseControl != null) {
            return MetricsAndLoggingUtils.metricsAndLogging(reconciliation, configMapOperator, cruiseControl.logging(), cruiseControl.metrics())
                    .compose(metricsAndLogging -> {
                        ConfigMap configMap = cruiseControl.generateConfigMap(metricsAndLogging);

                        this.serverConfigurationHash = Util.hashStub(configMap.getData().get(CruiseControl.SERVER_CONFIG_FILENAME));
                        this.capacityConfigurationHash = Util.hashStub(configMap.getData().get(CruiseControl.CAPACITY_CONFIG_FILENAME));

                        return configMapOperator
                                .reconcile(
                                        reconciliation,
                                        reconciliation.namespace(),
                                        CruiseControlResources.configMapName(reconciliation.name()),
                                        configMap
                                ).mapEmpty();
                    });
        } else {
            return configMapOperator.reconcile(reconciliation, reconciliation.namespace(), CruiseControlResources.configMapName(reconciliation.name()), null)
                    .mapEmpty();
        }
    }

    /**
     * Manages the Cruise Control certificates Secret.
     *
     * @param clock The clock for supplying the reconciler with the time instant of each reconciliation cycle.
     *              That time is used for checking maintenance windows
     *
     * @return      Future which completes when the reconciliation is done
     */
    protected Future<Void> certificatesSecret(Clock clock) {
        if (cruiseControl != null) {
            return secretOperator.getAsync(reconciliation.namespace(), CruiseControlResources.secretName(reconciliation.name()))
                    .compose(oldSecret -> {
                        Secret newSecret = cruiseControl.generateCertificatesSecret(reconciliation.namespace(), reconciliation.name(), clusterCa, oldSecret, Util.isMaintenanceTimeWindowsSatisfied(reconciliation, maintenanceWindows, clock.instant()));

                        return secretOperator
                                .reconcile(reconciliation, reconciliation.namespace(), CruiseControlResources.secretName(reconciliation.name()), newSecret)
                                .compose(i -> {
                                    certificateHash = CertUtils.getCertificateShortThumbprint(newSecret, Ca.SecretEntry.CRT.asKey(CruiseControl.COMPONENT_TYPE));

                                    return Future.succeededFuture();
                                });
                    });
        } else {
            return secretOperator.reconcile(reconciliation, reconciliation.namespace(), CruiseControlResources.secretName(reconciliation.name()), null)
                    .mapEmpty();
        }
    }

    /**
     * Generates a new centralized Cruise Control API secret containing an authentication file comprising of all
     * REST API credentials, it is generated using the following API secrets:
     *
     *  (1) Previous Cruise Control API secret
     *      The centralized API secret contains passwords for the healthcheck and rebalance operator users as well
     *      the authentication file comprising all REST API credentials.
     *
     *  (2) User-managed API secret
     *      The user-managed API secret contains a custom authentication file and can specify any number of API users.
     *
     *  (3) Topic Operator-managed API secret
     *      The Topic Operator-managed API secret contains a username and password for the Topic Operator user, which
     *      has ADMIN role by default.
     *
     * If there are any changes to the API user content in any of the secrets, the centralized Cruise Control secret is
     * updated with the new changes. Otherwise the content in the centralized Cruise Control secret remains the same.
     *
     * Strimzi maintains a hash of the centralized Cruise Control API secret content to detect changes that require a pod restart.
     *
     * @return Future which completes when the reconciliation is done.
     */
    protected Future<Void> apiSecret() {
        if (cruiseControl != null) {
            Future<Secret> ccApiSecretFuture = secretOperator.getAsync(reconciliation.namespace(), CruiseControlResources.apiSecretName(reconciliation.name()));
            Future<Secret> userManagedApiSecretFuture = cruiseControl.apiCredentials().getUserManagedApiSecretName() != null
                    ? secretOperator.getAsync(reconciliation.namespace(), cruiseControl.apiCredentials().getUserManagedApiSecretName())
                    : Future.succeededFuture(null);
            Future<Secret> topicOperatorManagedApiSecretFuture = isTopicOperatorEnabled
                    ? secretOperator.getAsync(reconciliation.namespace(), KafkaResources.entityTopicOperatorCcApiSecretName(reconciliation.name()))
                    : Future.succeededFuture(null);
            return Future.join(ccApiSecretFuture, userManagedApiSecretFuture, topicOperatorManagedApiSecretFuture)
                .compose(
                    compositeFuture -> {
                        Secret oldCcApiSecret = compositeFuture.resultAt(0);
                        Secret userManagedApiSecret = compositeFuture.resultAt(1);
                        Secret topicOperatorManagedApiSecret = compositeFuture.resultAt(2);

                        Secret newCcApiUsersSecret = cruiseControl.apiCredentials().generateApiSecret(passwordGenerator,
                                oldCcApiSecret, userManagedApiSecret, topicOperatorManagedApiSecret);

                        this.apiSecretHash = ReconcilerUtils.hashSecretContent(newCcApiUsersSecret);
                        return secretOperator.reconcile(reconciliation, reconciliation.namespace(), CruiseControlResources.apiSecretName(reconciliation.name()), newCcApiUsersSecret)
                            .mapEmpty();
                    }
                );
        } else {
            return secretOperator.reconcile(reconciliation, reconciliation.namespace(), CruiseControlResources.apiSecretName(reconciliation.name()), null)
                    .mapEmpty();
        }
    }

    /**
     * Manages the Cruise Control Service.
     *
     * @return  Future which completes when the reconciliation is done
     */
    protected Future<Void> service() {
        return serviceOperator
                .reconcile(
                        reconciliation,
                        reconciliation.namespace(),
                        CruiseControlResources.serviceName(reconciliation.name()),
                        cruiseControl != null ? cruiseControl.generateService() : null
                ).mapEmpty();
    }

    /**
     * Manages the Cruise Control Deployment.
     *
     * @return  Future which completes when the reconciliation is done
     */
    protected Future<Void> deployment(boolean isOpenShift, ImagePullPolicy imagePullPolicy, List<LocalObjectReference> imagePullSecrets) {
        if (cruiseControl != null) {
            Map<String, String> podAnnotations = new LinkedHashMap<>();
            podAnnotations.put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_CERT_GENERATION, String.valueOf(clusterCa.caCertGeneration()));
            podAnnotations.put(Ca.ANNO_STRIMZI_IO_CLUSTER_CA_KEY_GENERATION, String.valueOf(clusterCa.caKeyGeneration()));
            podAnnotations.put(Annotations.ANNO_STRIMZI_IO_CONFIGURATION_HASH, serverConfigurationHash);
            podAnnotations.put(CruiseControl.ANNO_STRIMZI_CAPACITY_CONFIGURATION_HASH, capacityConfigurationHash);
            podAnnotations.put(Annotations.ANNO_STRIMZI_AUTH_HASH, apiSecretHash);
            podAnnotations.put(Annotations.ANNO_STRIMZI_SERVER_CERT_HASH, certificateHash);
            
            Deployment deployment = cruiseControl.generateDeployment(podAnnotations, isOpenShift, imagePullPolicy, imagePullSecrets);

            return deploymentOperator
                    .reconcile(reconciliation, reconciliation.namespace(), CruiseControlResources.componentName(reconciliation.name()), deployment)
                    .mapEmpty();
        } else {
            return deploymentOperator.reconcile(reconciliation, reconciliation.namespace(), CruiseControlResources.componentName(reconciliation.name()), null)
                    .mapEmpty();
        }
    }

    /**
     * Waits for the Cruise Control deployment to finish any rolling and get ready.
     *
     * @return  Future which completes when the reconciliation is done
     */
    protected Future<Void> waitForDeploymentReadiness() {
        if (cruiseControl != null) {
            return deploymentOperator.waitForObserved(reconciliation, reconciliation.namespace(), CruiseControlResources.componentName(reconciliation.name()), 1_000, operationTimeoutMs)
                    .compose(i -> deploymentOperator.readiness(reconciliation, reconciliation.namespace(), CruiseControlResources.componentName(reconciliation.name()), 1_000, operationTimeoutMs));
        } else {
            return Future.succeededFuture();
        }
    }
}
