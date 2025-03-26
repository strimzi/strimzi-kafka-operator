/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.operator;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.api.model.coordination.v1.Lease;
import io.fabric8.kubernetes.api.model.coordination.v1.LeaseBuilder;
import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBuilder;
import io.skodjob.testframe.installation.InstallationMethod;
import io.skodjob.testframe.resources.KubeResourceManager;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.enums.DeploymentTypes;
import io.strimzi.systemtest.resources.kubernetes.DeploymentResource;
import io.strimzi.systemtest.templates.kubernetes.ClusterRoleBindingTemplates;
import io.strimzi.systemtest.templates.kubernetes.RoleBindingTemplates;
import io.strimzi.systemtest.templates.kubernetes.RoleTemplates;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.systemtest.utils.kubeUtils.objects.LeaseUtils;
import io.strimzi.systemtest.utils.kubeUtils.rbac.RbacUtils;
import io.strimzi.test.ReadWriteUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;

/**
 * Class for handling the Yaml manifest installation.
 * Everything is done by resources and ResourceManager.
 */
public class YamlInstallation implements InstallationMethod {

    private static final Logger LOGGER = LogManager.getLogger(YamlInstallation.class);

    public static final String CO_INSTALL_DIR = TestUtils.USER_PATH + "/../packaging/install/cluster-operator/";
    public static final String PATH_TO_CO_CONFIG = CO_INSTALL_DIR + "060-Deployment-strimzi-cluster-operator.yaml";

    private ClusterOperatorConfiguration clusterOperatorConfiguration;

    /**
     * Constructor with specific {@link ClusterOperatorConfiguration}.
     *
     * @param clusterOperatorConfiguration  default or customized {@link ClusterOperatorConfiguration} which should be used
     *                                      for installation of the ClusterOperator via application of YAMLs
     */
    public YamlInstallation(ClusterOperatorConfiguration clusterOperatorConfiguration) {
        this.clusterOperatorConfiguration = clusterOperatorConfiguration;
    }

    /**
     * Returns the configured {@link ClusterOperatorConfiguration}.
     * This is then used in {@link SetupClusterOperator#getClusterConfiguration()} (based on the installation method).
     *
     * @return  {@link ClusterOperatorConfiguration} configured and used during the installation
     */
    public ClusterOperatorConfiguration getClusterOperatorConfiguration() {
        return clusterOperatorConfiguration;
    }

    /**
     * Installation method for YAML install type.
     * It:
     *   - applies installation files - except RoleBindings and Deployment - as those are handled in different methods
     *      - that means it creates modified - Lease, (Cluster)Roles, ConfigMap, ServiceAccount, CRDs
     *   - applies default Roles, ClusterRoles, and (Cluster)Role Bindings - based on the RBAC scope (in {@link ClusterOperatorConfiguration#getClusterOperatorRBACType()}
     *   - deploys the ClusterOperator's Deployment based on configuration from {@link #clusterOperatorConfiguration}
     */
    @Override
    public void install() {
        // Firstly, apply all installation files - except RoleBindings and Deployment
        applyInstallationFiles();
        // Based on the scope, apply default (Cluster)Roles and RoleBindings
        applyDefaultRolesClusterRolesAndRoleBindings();
        // Finally, deploy the Cluster Operator
        deployClusterOperator();
    }

    /**
     * No implementation of deletion.
     * Everything is deployed using {@link KubeResourceManager}, meaning it is properly deleted at the end of the tests.
     * In case that this method is used, it throws the {@link UnsupportedOperationException} - as resource manager should be used.
     */
    @Override
    public void delete() {
        throw new UnsupportedOperationException("delete() should not be called directly; use KubeResourceManager instead.");
    }

    /**
     * Applies files from the {@link #CO_INSTALL_DIR} path.
     * It filters out all the ClusterRoleBindings, RoleBindings, and Deployment files, as they are handled in {@link #applyDefaultRolesClusterRolesAndRoleBindings()}
     * and {@link #deployClusterOperator()}.
     * In case of ClusterRoles it checks if the files should be switched to Roles (because of Namespace-scoped installation).
     * For the ClusterRole it also checks if the Lease name should be changed for corresponding ClusterOperator.
     * These files are then applied (created) using the {@link KubeResourceManager}.
     */
    public void applyInstallationFiles() {
        List<File> operatorFiles = Arrays.stream(new File(CO_INSTALL_DIR).listFiles()).sorted()
            .filter(File::isFile)
            .filter(file ->
                // We are skipping these files because we are handling them in different methods
                !file.getName().matches(".*(Binding|Deployment)-.*"))
            .toList();

        for (File operatorFile : operatorFiles) {
            // In case of Namespace-scoped we need to switch from ClusterRole to Role
            // Doing it here so we skip issues with wrong types (ClusterRole containing Role kind in the object etc.)
            if (operatorFile.getName().contains(TestConstants.CLUSTER_ROLE + "-")) {
                operatorFile =  RbacUtils.switchClusterRolesToRolesIfNeeded(operatorFile, clusterOperatorConfiguration.isNamespaceScopedInstallation());
            }

            final String resourceType = operatorFile.getName().split("-")[1];

            switch (resourceType) {
                case TestConstants.ROLE:
                    Role role = ReadWriteUtils.readObjectFromYamlFilepath(operatorFile, Role.class);
                    KubeResourceManager.get().createOrUpdateResourceWithWait(new RoleBuilder(role)
                        .editMetadata()
                            .withNamespace(clusterOperatorConfiguration.getNamespaceName())
                        .endMetadata()
                        .build());
                    break;
                case TestConstants.CLUSTER_ROLE:
                    ClusterRole clusterRole = ReadWriteUtils.readObjectFromYamlFilepath(
                        LeaseUtils.changeLeaseNameInResourceIfNeeded(operatorFile.getAbsolutePath(), clusterOperatorConfiguration.getExtraEnvVars()), ClusterRole.class);
                    KubeResourceManager.get().createOrUpdateResourceWithWait(clusterRole);
                    break;
                case TestConstants.SERVICE_ACCOUNT:
                    ServiceAccount serviceAccount = ReadWriteUtils.readObjectFromYamlFilepath(operatorFile, ServiceAccount.class);
                    KubeResourceManager.get().createOrUpdateResourceWithWait(new ServiceAccountBuilder(serviceAccount)
                        .editMetadata()
                            .withNamespace(clusterOperatorConfiguration.getNamespaceName())
                        .endMetadata()
                        .build());
                    break;
                case TestConstants.CONFIG_MAP:
                    ConfigMap configMap = ReadWriteUtils.readObjectFromYamlFilepath(operatorFile, ConfigMap.class);
                    KubeResourceManager.get().createOrUpdateResourceWithWait(new ConfigMapBuilder(configMap)
                        .editMetadata()
                            .withNamespace(clusterOperatorConfiguration.getNamespaceName())
                            .withName(clusterOperatorConfiguration.getOperatorDeploymentName())
                        .endMetadata()
                        .build());
                    break;
                case TestConstants.LEASE:
                    // Loads the resource through Fabric8 Kubernetes Client => that way we do not need to add a direct
                    // dependency on Jackson Datatype JSR310 to decode the Lease resource
                    Lease lease = kubeClient().getClient().leases().load(operatorFile).item();
                    KubeResourceManager.get().createOrUpdateResourceWithWait(new LeaseBuilder(lease)
                        .editMetadata()
                            .withNamespace(clusterOperatorConfiguration.getNamespaceName())
                            .withName(clusterOperatorConfiguration.getOperatorDeploymentName())
                        .endMetadata()
                        .build());
                    break;
                case TestConstants.CUSTOM_RESOURCE_DEFINITION_SHORT:
                    CustomResourceDefinition customResourceDefinition = ReadWriteUtils.readObjectFromYamlFilepath(operatorFile, CustomResourceDefinition.class);
                    KubeResourceManager.get().createOrUpdateResourceWithWait(customResourceDefinition);
                    break;
                default:
                    LOGGER.error("Unknown installation resource type: {}", resourceType);
                    throw new RuntimeException("Unknown installation resource type:" + resourceType);
            }
        }
    }

    /**
     * This method applies the all needed ClusterRoles, Roles, and RoleBindings everywhere where they are needed.
     * The full procedure and what is applied where is described in the comments directly in the code of the method.
     */
    public void applyDefaultRolesClusterRolesAndRoleBindings() {
        // ------------------
        // watch multiple Namespaces
        // ------------------
        // 1. replace the .spec.subjects[0].namespace: field in all RoleBindings to the one where we will install the CO
        // 2. create the RoleBinding in each Namespace the operator will watch -> specified in .metadata.namespace
        // ------------------
        // watch all Namespaces
        // ------------------
        // 1. replace the .spec.subjects[0].namespace: field in all RoleBindings to the one where we will install the CO
        // 2. .metadata.namespace is the Namespace where we will install CO
        // 3. create ClusterRoleBindings for -namespaced, -watched, -entity-operator-delegation pointing at these RoleBindings in the CO install namespace

        // In case that we are watching all the Namespaces - cluster-wide installation - using `*`, we need to apply extra ClusterRoleBindings
        // for CO and other operators, so they can watch and apply changes in all Namespaces in the whole cluster
        if (clusterOperatorConfiguration.isWatchingAllNamespaces()) {
            List<ClusterRoleBinding> clusterRoleBindings = ClusterRoleBindingTemplates.clusterRoleBindingsForAllNamespaces(clusterOperatorConfiguration.getNamespaceName());
            KubeResourceManager.get().createOrUpdateResourceWithWait(clusterRoleBindings.toArray(new ClusterRoleBinding[0]));

            applyRoleBindings(clusterOperatorConfiguration.getNamespaceName());
        } else {
            for (String namespaceToWatch : clusterOperatorConfiguration.getListOfNamespacesToWatch()) {
                // In case that we are running Namespace-scoped installation, we need to create Roles in each of the Namespace we will watch
                // including the co-namespace
                if (clusterOperatorConfiguration.isNamespaceScopedInstallation()) {
                    applyRoles(namespaceToWatch);
                }

                // Apply RoleBindings to each of the Namespace, pointing to co-namespace
                applyRoleBindings(namespaceToWatch);
            }
        }

        // In case that we run cluster-wide installation, we need to apply ClusterRoleBindings
        if (!clusterOperatorConfiguration.isNamespaceScopedInstallation()) {
            applyClusterRoleBindings();
        }
    }

    /**
     * Applies all RoleBindings listed in the method.
     * It also changes the ClusterRole to Role in the RoleBindings in case of Namespace-scoped installation.
     * Finally, for the `022-RoleBinding-strimzi-cluster-operator.yaml` it checks if the change of the Lease name is needed or not
     * (and changes it in case of need).
     * These RoleBindings are then applied in each Namespace that should be watched by the ClusterOperator.
     *
     * @param namespaceToWatch  Namespace which ClusterOperator should watch and where these RoleBindings should be created
     */
    public void applyRoleBindings(String namespaceToWatch) {
        List<String> roleBindings = List.of(
            // 020-RoleBinding => Cluster Operator rights for managing operands
            "020-RoleBinding-strimzi-cluster-operator.yaml",
            // 022-RoleBinding => Leader election RoleBinding (is only in the operator namespace)
            "022-RoleBinding-strimzi-cluster-operator.yaml",
            // 023-RoleBinding => Leader election RoleBinding (is only in the operator namespace)
            "023-RoleBinding-strimzi-cluster-operator.yaml",
            // 031-RoleBinding => Entity Operator delegation
            "031-RoleBinding-strimzi-cluster-operator-entity-operator-delegation.yaml"
        );

        roleBindings.forEach(fileName -> {
            File roleBindingFile = new File(CO_INSTALL_DIR, fileName);
            roleBindingFile = RbacUtils.switchClusterRolesToRolesIfNeeded(roleBindingFile, clusterOperatorConfiguration.isNamespaceScopedInstallation());

            String path = roleBindingFile.getAbsolutePath();

            if (fileName.startsWith("022-")) {
                path = LeaseUtils.changeLeaseNameInResourceIfNeeded(path, clusterOperatorConfiguration.getExtraEnvVars());
            }

            KubeResourceManager.get().createOrUpdateResourceWithWait(
                RoleBindingTemplates.roleBindingFromFile(clusterOperatorConfiguration.getNamespaceName(), namespaceToWatch, path)
            );
        });
    }

    /**
     * Applies all Roles listed in the method.
     * It also changes the ClusterRole to Role in case of Namespace-scoped installation.
     * Finally, for the `022-ClusterRole-strimzi-cluster-operator-role.yaml"` it checks if the change of the Lease name is needed or not
     * (and changes it in case of need).
     * These Roles are then applied in each Namespace that should be watched by the ClusterOperator.
     *
     * @param namespaceToWatch  Namespace which ClusterOperator should watch and where these Roles should be created
     */
    public void applyRoles(String namespaceToWatch) {
        List<String> roleFiles = List.of(
            "020-ClusterRole-strimzi-cluster-operator-role.yaml",
            "021-ClusterRole-strimzi-cluster-operator-role.yaml",
            "022-ClusterRole-strimzi-cluster-operator-role.yaml",
            "023-ClusterRole-strimzi-cluster-operator-role.yaml",
            "030-ClusterRole-strimzi-kafka-broker.yaml",
            "031-ClusterRole-strimzi-entity-operator.yaml",
            "033-ClusterRole-strimzi-kafka-client.yaml"
        );

        roleFiles.forEach(fileName -> {
            File roleFile = new File(CO_INSTALL_DIR, fileName);
            roleFile = RbacUtils.switchClusterRolesToRolesIfNeeded(roleFile, clusterOperatorConfiguration.isNamespaceScopedInstallation());

            String path = roleFile.getAbsolutePath();

            if (fileName.startsWith("022-")) {
                path = LeaseUtils.changeLeaseNameInResourceIfNeeded(path, clusterOperatorConfiguration.getExtraEnvVars());
            }

            KubeResourceManager.get().createOrUpdateResourceWithWait(
                RoleTemplates.roleFromFile(namespaceToWatch, path)
            );
        });
    }

    /**
     * In case of cluster-wide installation, it applies all ClusterRoleBindings listed in the method.
     */
    private void applyClusterRoleBindings() {
        List<String> clusterRoleBindings = List.of(
            "021-ClusterRoleBinding-strimzi-cluster-operator.yaml",
            "030-ClusterRoleBinding-strimzi-cluster-operator-kafka-broker-delegation.yaml",
            "033-ClusterRoleBinding-strimzi-cluster-operator-kafka-client-delegation.yaml"
        );

        for (String fileName : clusterRoleBindings) {
            KubeResourceManager.get().createOrUpdateResourceWithWait(
                ClusterRoleBindingTemplates.clusterRoleBindingFromFile(clusterOperatorConfiguration.getNamespaceName(),
                    new File(CO_INSTALL_DIR, fileName).getAbsolutePath())
            );
        }
    }

    /**
     * Method that deploys the ClusterOperator based on the {@link #clusterOperatorConfiguration} configured at
     * initialization of the {@link YamlInstallation} method.
     */
    public void deployClusterOperator() {
        deployClusterOperator(clusterOperatorConfiguration);
    }

    /**
     * Based on {@link ClusterOperatorConfiguration} specified in the {@param clusterOperatorConfiguration} it configures
     * the Deployment (with all the env variables, labels, replicas, etc.) and deploys it using the {@link KubeResourceManager}.
     *
     * @param clusterOperatorConfiguration  desired configuration of the ClusterOperator
     */
    public static void deployClusterOperator(ClusterOperatorConfiguration clusterOperatorConfiguration) {
        Deployment clusterOperator = DeploymentResource.getDeploymentFromYaml(PATH_TO_CO_CONFIG);

        // Get env from config file
        List<EnvVar> envVars = clusterOperator.getSpec().getTemplate().getSpec().getContainers().get(0).getEnv();
        // Get default CO image
        String coImage = clusterOperator.getSpec().getTemplate().getSpec().getContainers().get(0).getImage();

        // Update images
        for (EnvVar envVar : envVars) {
            switch (envVar.getName()) {
                case "STRIMZI_NAMESPACE":
                    envVar.setValue(clusterOperatorConfiguration.getNamespacesToWatch());
                    envVar.setValueFrom(null);
                    break;
                case "STRIMZI_FULL_RECONCILIATION_INTERVAL_MS":
                    envVar.setValue(Long.toString(clusterOperatorConfiguration.getReconciliationInterval()));
                    break;
                case "STRIMZI_OPERATION_TIMEOUT_MS":
                    envVar.setValue(Long.toString(clusterOperatorConfiguration.getOperationTimeout()));
                    break;
                case "STRIMZI_FEATURE_GATES":
                    if (!envVar.getValue().equals(Environment.STRIMZI_FEATURE_GATES)) {
                        envVar.setValue(clusterOperatorConfiguration.getFeatureGates());
                    }
                    break;
                default:
                    if (envVar.getName().contains("KAFKA_BRIDGE_IMAGE")) {
                        envVar.setValue(Environment.useLatestReleasedBridge() ? envVar.getValue() : Environment.BRIDGE_IMAGE);
                    } else if (envVar.getName().contains("STRIMZI_DEFAULT")) {
                        envVar.setValue(StUtils.changeOrgAndTag(envVar.getValue()));
                    } else if (envVar.getName().contains("IMAGES")) {
                        envVar.setValue(StUtils.changeOrgAndTagInImageMap(envVar.getValue()));
                    }
            }
        }

        envVars.add(new EnvVar("STRIMZI_IMAGE_PULL_POLICY", Environment.COMPONENTS_IMAGE_PULL_POLICY, null));
        envVars.add(new EnvVar("STRIMZI_LOG_LEVEL", Environment.STRIMZI_LOG_LEVEL, null));

        // Go through all the extra env vars configured in the clusterOperatorConfiguration and set the values
        // in those which name is matching
        if (clusterOperatorConfiguration.getExtraEnvVars() != null) {
            envVars.forEach(envVar ->
                clusterOperatorConfiguration.getExtraEnvVars().stream()
                    .filter(extraVar -> envVar.getName().equals(extraVar.getName()))
                    .findFirst()
                    .ifPresent(xVar -> envVar.setValue(xVar.getValue()))
            );
        }

        // Configure image pull secret for Deployment file of CO and for components - Kafka, KafkaBridge ...
        if (Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET != null && !Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET.isEmpty()) {
            // For Cluster Operator
            List<LocalObjectReference> imagePullSecrets = Collections.singletonList(new LocalObjectReference(Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET));
            clusterOperator.getSpec().getTemplate().getSpec().setImagePullSecrets(imagePullSecrets);

            // For components
            envVars.add(new EnvVar("STRIMZI_IMAGE_PULL_SECRETS", Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET, null));
        }

        // Map the current envVars list to Map for easier manipulation
        Map<String, EnvVar> envVarMap = envVars.stream()
            .collect(Collectors.toMap(EnvVar::getName, Function.identity()));

        // Adding custom evn vars specified by user in installation
        if (clusterOperatorConfiguration.getExtraEnvVars() != null) {
            clusterOperatorConfiguration.getExtraEnvVars().forEach(extraEnvVar -> {
                // If we are configuring EnvVar in extraEnvVars that was already configured before, set the value of extraEnvVar
                envVarMap.computeIfPresent(extraEnvVar.getName(), (key, existingEnvVar) -> {
                    existingEnvVar.setValue(extraEnvVar.getValue());
                    return existingEnvVar;
                });

                // Otherwise put the extra EnvVar to the Map - as it's not in the already configured EnvVars
                envVarMap.putIfAbsent(extraEnvVar.getName(), extraEnvVar);
            });
        }

        // Apply updated env variables
        clusterOperator.getSpec().getTemplate().getSpec().getContainers().get(0).setEnv(envVarMap.values().stream().toList());

        // FIPS mode seems to consume more resources, so in case that we are running on FIPS cluster, we adjust the resources a bit
        if (KubeClusterResource.getInstance().fipsEnabled()) {
            ResourceRequirements resourceRequirements = new ResourceRequirementsBuilder()
                .withRequests(Map.of("memory", new Quantity(TestConstants.CO_REQUESTS_MEMORY), "cpu", new Quantity(
                    TestConstants.CO_REQUESTS_CPU)))
                .withLimits(Map.of("memory", new Quantity(TestConstants.CO_LIMITS_MEMORY), "cpu", new Quantity(
                    TestConstants.CO_LIMITS_CPU)))
                .build();

            clusterOperator.getSpec().getTemplate().getSpec().getContainers().get(0).setResources(resourceRequirements);
        }

        KubeResourceManager.get().createResourceWithWait(new DeploymentBuilder(clusterOperator)
            .editMetadata()
                .withName(clusterOperatorConfiguration.getOperatorDeploymentName())
                .withNamespace(clusterOperatorConfiguration.getNamespaceName())
                .addToLabels(TestConstants.DEPLOYMENT_TYPE, DeploymentTypes.BundleClusterOperator.name())
            .endMetadata()
            .editSpec()
                .withReplicas(clusterOperatorConfiguration.getReplicas())
                .editSelector()
                    .addToMatchLabels(clusterOperatorConfiguration.getExtraLabels())
                .endSelector()
                .editTemplate()
                    .editMetadata()
                        .addToLabels(clusterOperatorConfiguration.getExtraLabels())
                    .endMetadata()
                    .editSpec()
                        .editFirstContainer()
                            .withImage(StUtils.changeOrgAndTag(coImage))
                            .withImagePullPolicy(Environment.OPERATOR_IMAGE_PULL_POLICY)
                        .endContainer()
                        .editFirstVolume()
                            .editEmptyDir()
                            // in case we execute more than 10 test cases in parallel we at least 2Mi storage
                            .withNewSizeLimit("2Mi")
                            .endEmptyDir()
                        .endVolume()
                        .editLastVolume()
                            .editConfigMap()
                                .withName(clusterOperatorConfiguration.getOperatorDeploymentName())
                            .endConfigMap()
                        .endVolume()
                    .endSpec()
                .endTemplate()
            .endSpec()
            .build()
        );
    }
}