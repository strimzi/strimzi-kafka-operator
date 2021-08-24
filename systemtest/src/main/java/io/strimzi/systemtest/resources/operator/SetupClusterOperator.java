/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.operator;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.strimzi.systemtest.BeforeAllOnce;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.enums.ClusterOperatorRBACType;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.kubernetes.ClusterRoleBindingResource;
import io.strimzi.systemtest.resources.kubernetes.NetworkPolicyResource;
import io.strimzi.systemtest.resources.kubernetes.RoleBindingResource;
import io.strimzi.systemtest.resources.kubernetes.RoleResource;
import io.strimzi.systemtest.resources.operator.specific.HelmResource;
import io.strimzi.systemtest.resources.operator.specific.OlmResource;
import io.strimzi.systemtest.templates.kubernetes.ClusterRoleBindingTemplates;
import io.strimzi.systemtest.utils.StUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.executor.Exec;
import io.strimzi.test.logs.CollectorElement;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.cluster.OpenShift;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Stack;
import java.util.stream.Collectors;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class SetupClusterOperator {

    private static final Logger LOGGER = LogManager.getLogger(SetupClusterOperator.class);
    public static final String CO_INSTALL_DIR = TestUtils.USER_PATH + "/../packaging/install/cluster-operator";
    private static final String INFRA_NAMESPACE = "infra-namespace";

    private KubeClusterResource cluster = KubeClusterResource.getInstance();
    private Stack<String> clusterOperatorConfigs = new Stack<>();
    private HelmResource helmResource;
    private OlmResource olmResource;

    private ExtensionContext extensionContext;
    private String clusterOperatorName;
    private String namespaceInstallTo;
    private String namespaceToWatch;
    private List<String> bindingsNamespaces;
    private long operationTimeout;
    private long reconciliationInterval;
    private List<EnvVar> extraEnvVars;
    private Map<String, String> extraLabels;
    private ClusterOperatorRBACType clusterOperatorRBACType;

    public SetupClusterOperator() {}
    public SetupClusterOperator(SetupClusterOperatorBuilder builder) {
        this.extensionContext = builder.extensionContext;
        this.clusterOperatorName = builder.clusterOperatorName;
        this.namespaceInstallTo = builder.namespaceInstallTo;
        this.namespaceToWatch = builder.namespaceToWatch;
        this.bindingsNamespaces = builder.bindingsNamespaces;
        this.operationTimeout = builder.operationTimeout;
        this.reconciliationInterval = builder.reconciliationInterval;
        this.extraEnvVars = builder.extraEnvVars;
        this.extraLabels = builder.extraLabels;
        this.clusterOperatorRBACType = builder.clusterOperatorRBACType;

        // assign defaults is something is not specified
        if (this.clusterOperatorName == null || this.clusterOperatorName.isEmpty()) this.clusterOperatorName = Constants.STRIMZI_DEPLOYMENT_NAME;
        // if namespace is not set we install operator to 'infra-namespace'
        if (this.namespaceInstallTo == null || this.namespaceInstallTo.isEmpty()) this.namespaceInstallTo = INFRA_NAMESPACE;
        if (this.namespaceToWatch == null) this.namespaceToWatch = this.namespaceInstallTo;
        if (this.bindingsNamespaces == null) this.bindingsNamespaces = Collections.singletonList(this.namespaceInstallTo);
        if (this.operationTimeout == 0) this.operationTimeout = Constants.CO_OPERATION_TIMEOUT_DEFAULT;
        if (this.reconciliationInterval == 0) this.reconciliationInterval = Constants.RECONCILIATION_INTERVAL;
        if (this.extraEnvVars == null) this.extraEnvVars = new ArrayList<>();
        if (this.extraLabels == null) this.extraLabels = new HashMap<>();
        if (this.clusterOperatorRBACType == null) this.clusterOperatorRBACType = ClusterOperatorRBACType.CLUSTER;
    }

    public static SetupClusterOperator buildDefaultInstallation() {
        return new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(BeforeAllOnce.getSharedExtensionContext())
            .withNamespace(Constants.INFRA_NAMESPACE)
            .withWatchingNamespaces(Constants.WATCH_ALL_NAMESPACES)
            .createInstallation();
    }

    /**
     * This method install Strimzi Cluster Operator based on environment variable configuration.
     * It can install operator by classic way (apply bundle yamls) or use OLM. For OLM you need to set all other OLM env variables.
     * Don't use this method in tests, where specific configuration of CO is needed.
     */
    public SetupClusterOperator runInstallation() {
        String testClassName = extensionContext.getRequiredTestClass().getName();
        String testMethodName = testClassName.contains(extensionContext.getDisplayName()) ? "" : extensionContext.getDisplayName();

        if (Environment.isOlmInstall()) {
            LOGGER.info("Going to install ClusterOperator via OLM");
            // cluster-wide olm co-operator
            if (namespaceToWatch.equals(Constants.WATCH_ALL_NAMESPACES)) {
                // if RBAC is enable we don't run tests in parallel mode and with that said we don't create another namespaces
                if (!Environment.isNamespaceRbacScope()) {
                    if (extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.PREPARE_OPERATOR_ENV_KEY + namespaceInstallTo) == null) {
                        cluster.setNamespace(namespaceInstallTo);

                        cluster.createNamespaces(CollectorElement.createCollectorElement(testClassName, testMethodName), namespaceInstallTo, bindingsNamespaces);
                        extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.PREPARE_OPERATOR_ENV_KEY + namespaceInstallTo, false);
                    }
                    createClusterRoleBindings();
                    olmResource = new OlmResource(cluster.getDefaultOlmNamespace());
                    olmResource.create(extensionContext, operationTimeout, reconciliationInterval);
                }
                // single-namespace olm co-operator
            } else {
                if (extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.PREPARE_OPERATOR_ENV_KEY + namespaceInstallTo) == null) {
                    cluster.setNamespace(namespaceInstallTo);
                    cluster.createNamespaces(CollectorElement.createCollectorElement(testClassName, testMethodName), namespaceInstallTo, bindingsNamespaces);
                    extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.PREPARE_OPERATOR_ENV_KEY + namespaceInstallTo, false);
                }
                olmResource = new OlmResource(namespaceInstallTo);
                olmResource.create(extensionContext, operationTimeout, reconciliationInterval);
            }
        } else if (Environment.isHelmInstall()) {
            LOGGER.info("Going to install ClusterOperator via Helm");
            helmResource = new HelmResource(namespaceInstallTo, namespaceToWatch);
            if (extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.PREPARE_OPERATOR_ENV_KEY + namespaceInstallTo) == null) {
                cluster.setNamespace(namespaceInstallTo);
                cluster.createNamespaces(CollectorElement.createCollectorElement(testClassName, testMethodName), namespaceInstallTo, bindingsNamespaces);
                extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.PREPARE_OPERATOR_ENV_KEY + namespaceInstallTo, false);
            }
            helmResource.create(extensionContext, operationTimeout, reconciliationInterval);
        } else {
            bundleInstallation();
        }
        return this;
    }

    public SetupClusterOperator runBundleInstallation() {
        bundleInstallation();
        return this;
    }

    private void bundleInstallation() {
        LOGGER.info("Going to install ClusterOperator via Yaml bundle");
        String testClassName = extensionContext.getRequiredTestClass().getName();
        String testMethodName = testClassName.contains(extensionContext.getDisplayName()) ? "" : extensionContext.getDisplayName();

        // check if namespace is already created
        if (extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(Constants.PREPARE_OPERATOR_ENV_KEY + namespaceInstallTo) == null) {
            cluster.createNamespaces(CollectorElement.createCollectorElement(testClassName, testMethodName), namespaceInstallTo, bindingsNamespaces);
            extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.PREPARE_OPERATOR_ENV_KEY + namespaceInstallTo, false);
        } else {
            LOGGER.info("Environment for ClusterOperator was already prepared! Going to install it now.");
        }
        prepareEnvForOperator(extensionContext, namespaceInstallTo, bindingsNamespaces);
        applyBindings();

        // copy image-pull secret
        if (Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET != null && !Environment.SYSTEM_TEST_STRIMZI_IMAGE_PULL_SECRET.isEmpty()) {
            StUtils.copyImagePullSecret(namespaceInstallTo);
        }
        // 060-Deployment
        ResourceManager.setCoDeploymentName(clusterOperatorName);
        ResourceManager.getInstance().createResource(extensionContext,
            new BundleResource.BundleResourceBuilder()
                .withName(clusterOperatorName)
                .withNamespace(namespaceInstallTo)
                .withWatchingNamespaces(namespaceToWatch)
                .withOperationTimeout(operationTimeout)
                .withReconciliationInterval(reconciliationInterval)
                .withExtraEnvVars(extraEnvVars)
                .withExtraLabels(extraLabels)
                .buildBundleInstance()
                .buildBundleDeployment()
                .build());
    }

    private void createClusterRoleBindings() {
        // Create ClusterRoleBindings that grant cluster-wide access to all OpenShift projects
        List<ClusterRoleBinding> clusterRoleBindingList = ClusterRoleBindingTemplates.clusterRoleBindingsForAllNamespaces(namespaceInstallTo);
        clusterRoleBindingList.forEach(clusterRoleBinding ->
            ResourceManager.getInstance().createResource(extensionContext, clusterRoleBinding));
    }

    public static class SetupClusterOperatorBuilder {

        private ExtensionContext extensionContext;
        private String clusterOperatorName;
        private String namespaceInstallTo;
        private String namespaceToWatch;
        private List<String> bindingsNamespaces;
        private long operationTimeout;
        private long reconciliationInterval;
        private List<EnvVar> extraEnvVars;
        private Map<String, String> extraLabels;
        private ClusterOperatorRBACType clusterOperatorRBACType;

        public SetupClusterOperatorBuilder withExtensionContext(ExtensionContext extensionContext) {
            this.extensionContext = extensionContext;
            return self();
        }
        public SetupClusterOperatorBuilder withClusterOperatorName(String clusterOperatorName) {
            this.clusterOperatorName = clusterOperatorName;
            return self();
        }
        public SetupClusterOperatorBuilder withNamespace(String namespaceInstallTo) {
            this.namespaceInstallTo = namespaceInstallTo;
            return self();
        }
        public SetupClusterOperatorBuilder withWatchingNamespaces(String namespaceToWatch) {
            this.namespaceToWatch = namespaceToWatch;
            return self();
        }
        public SetupClusterOperatorBuilder withBindingsNamespaces(List<String> bindingsNamespaces) {
            this.bindingsNamespaces = bindingsNamespaces;
            return self();
        }
        public SetupClusterOperatorBuilder withOperationTimeout(long operationTimeout) {
            this.operationTimeout = operationTimeout;
            return self();
        }
        public SetupClusterOperatorBuilder withReconciliationInterval(long reconciliationInterval) {
            this.reconciliationInterval = reconciliationInterval;
            return self();
        }

        // currently supported only for Bundle installation
        public SetupClusterOperatorBuilder withExtraEnvVars(List<EnvVar> envVars) {
            this.extraEnvVars = envVars;
            return self();
        }

        // currently supported only for Bundle installation
        public SetupClusterOperatorBuilder withExtraLabels(Map<String, String> extraLabels) {
            this.extraLabels = extraLabels;
            return self();
        }

        // currently supported only for Bundle installation
        public SetupClusterOperatorBuilder withClusterOperatorRBACType(ClusterOperatorRBACType clusterOperatorRBACType) {
            this.clusterOperatorRBACType = clusterOperatorRBACType;
            return self();
        }

        private SetupClusterOperatorBuilder self() {
            return this;
        }

        public SetupClusterOperator createInstallation() {
            return new SetupClusterOperator(this);
        }
    }

    /**
     * Prepare environment for cluster operator which includes creation of namespaces, custom resources and operator
     * specific config files such as ServiceAccount, Roles and CRDs.
     * @param clientNamespace namespace which will be created and used as default by kube client
     * @param namespaces list of namespaces which will be created
     * @param resources list of path to yaml files with resources specifications
     */
    public void prepareEnvForOperator(ExtensionContext extensionContext, String clientNamespace, List<String> namespaces, String... resources) {
        assumeTrue(!Environment.isHelmInstall() && !Environment.isOlmInstall());
        applyClusterOperatorInstallFiles(clientNamespace);
        NetworkPolicyResource.applyDefaultNetworkPolicySettings(extensionContext, namespaces);

        if (cluster.cluster() instanceof OpenShift) {
            // This is needed in case you are using internal kubernetes registry and you want to pull images from there
            if (kubeClient().getNamespace(Environment.STRIMZI_ORG) != null) {
                for (String namespace : namespaces) {
                    LOGGER.debug("Setting group policy for Openshift registry in namespace: " + namespace);
                    Exec.exec(null, Arrays.asList("oc", "policy", "add-role-to-group", "system:image-puller", "system:serviceaccounts:" + namespace, "-n", Environment.STRIMZI_ORG), 0, false, false);
                }
            }
        }
    }

    /**
     * Prepare environment for cluster operator which includes creation of namespaces, custom resources and operator
     * specific config files such as ServiceAccount, Roles and CRDs.
     * @param clientNamespace namespace which will be created and used as default by kube client
     */
    public void prepareEnvForOperator(ExtensionContext extensionContext, String clientNamespace) {
        prepareEnvForOperator(extensionContext, clientNamespace, Collections.singletonList(clientNamespace));
    }

    /**
     * Perform application of ServiceAccount, Roles and CRDs needed for proper cluster operator deployment.
     * Configuration files are loaded from packaging/install/cluster-operator directory.
     */
    public void applyClusterOperatorInstallFiles(String namespace) {
        List<File> operatorFiles = Arrays.stream(new File(CO_INSTALL_DIR).listFiles()).sorted()
            .filter(File::isFile)
            .filter(file ->
                !file.getName().matches(".*(Binding|Deployment)-.*"))
            .collect(Collectors.toList());

        for (File operatorFile : operatorFiles) {
            File createFile = operatorFile;
            if (operatorFile.getName().contains("ClusterRole-")) {
                createFile = switchClusterRolesToRolesIfNeeded(createFile);
            }

            LOGGER.info("Creating configuration file: {}", createFile.getAbsolutePath());
            cmdKubeClient().namespace(namespace).createOrReplace(createFile);
            clusterOperatorConfigs.push(createFile.getPath());
        }
    }

    /**
     * Replace all references to ClusterRole to Role.
     * This includes ClusterRoles themselves as well as RoleBindings that reference them.
     */
    public File switchClusterRolesToRolesIfNeeded(File oldFile) {
        boolean isRbacScope = this.extraEnvVars.stream().anyMatch(it -> it.getName().equals(Environment.STRIMZI_RBAC_SCOPE_ENV) && it.getValue().equals(Environment.STRIMZI_RBAC_SCOPE_NAMESPACE));

        if (Environment.isNamespaceRbacScope() || isRbacScope || this.clusterOperatorRBACType == ClusterOperatorRBACType.NAMESPACE) {
            try {
                File tmpFile = File.createTempFile("rbac-" + oldFile.getName().replace(".yaml", ""), ".yaml");
                TestUtils.writeFile(tmpFile.getAbsolutePath(), TestUtils.readFile(oldFile).replace("ClusterRole", "Role"));
                LOGGER.info("Replaced ClusterRole for Role in {}", oldFile.getAbsolutePath());

                return tmpFile;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            return oldFile;
        }
    }

    public void applyRoleBindings(ExtensionContext extensionContext, String namespace, String bindingsNamespace) {
        // 020-RoleBinding
        File roleFile = new File(Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/020-RoleBinding-strimzi-cluster-operator.yaml");
        RoleBindingResource.roleBinding(extensionContext, switchClusterRolesToRolesIfNeeded(roleFile).getAbsolutePath(), namespace, bindingsNamespace);
        // 031-RoleBinding
        roleFile = new File(Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/031-RoleBinding-strimzi-cluster-operator-entity-operator-delegation.yaml");
        RoleBindingResource.roleBinding(extensionContext, switchClusterRolesToRolesIfNeeded(roleFile).getAbsolutePath(), namespace, bindingsNamespace);
    }

    public void applyRoles(String namespace) {
        File roleFile = new File(Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/020-ClusterRole-strimzi-cluster-operator-role.yaml");
        RoleResource.role(extensionContext, switchClusterRolesToRolesIfNeeded(roleFile).getAbsolutePath(), namespace);

        roleFile = new File(Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/021-ClusterRole-strimzi-cluster-operator-role.yaml");
        RoleResource.role(extensionContext, switchClusterRolesToRolesIfNeeded(roleFile).getAbsolutePath(), namespace);

        roleFile = new File(Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/030-ClusterRole-strimzi-kafka-broker.yaml");
        RoleResource.role(extensionContext, switchClusterRolesToRolesIfNeeded(roleFile).getAbsolutePath(), namespace);

        roleFile = new File(Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/031-ClusterRole-strimzi-entity-operator.yaml");
        RoleResource.role(extensionContext, switchClusterRolesToRolesIfNeeded(roleFile).getAbsolutePath(), namespace);

        roleFile = new File(Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/033-ClusterRole-strimzi-kafka-client.yaml");
        RoleResource.role(extensionContext, switchClusterRolesToRolesIfNeeded(roleFile).getAbsolutePath(), namespace);
    }

    /**
     * Method to apply Strimzi cluster operator specific RoleBindings and ClusterRoleBindings for specific namespaces.
     */
    public void applyBindings() {
        if (Environment.isNamespaceRbacScope() || this.clusterOperatorRBACType.equals(ClusterOperatorRBACType.NAMESPACE)) {
            // if roles only, only deploy the rolebindings
            for (String bindingsNamespace : bindingsNamespaces) {
                applyRoles(bindingsNamespace);
                applyRoleBindings(extensionContext, namespaceInstallTo, bindingsNamespace);
            }
            applyRoleBindings(extensionContext, namespaceInstallTo, namespaceInstallTo);
        } else {
            for (String bindingsNamespace : bindingsNamespaces) {
                applyClusterRoleBindings(extensionContext, this.namespaceInstallTo);
                applyRoleBindings(extensionContext, this.namespaceInstallTo, bindingsNamespace);
            }
        }
        // cluster-wide installation
        if (namespaceToWatch.equals(Constants.WATCH_ALL_NAMESPACES)) {
            if (Environment.isNamespaceRbacScope()) {
                // we override namespaceToWatch to where cluster operator is installed because RBAC is
                // enabled and we have use only single namespace
                namespaceToWatch = namespaceInstallTo;
            } else {
                // if RBAC is enable we don't run tests in parallel mode and with that said we don't create another namespaces
                createClusterRoleBindings();
            }
        }
    }

    private static void applyClusterRoleBindings(ExtensionContext extensionContext, String namespace) {
        // 021-ClusterRoleBinding
        ClusterRoleBindingResource.clusterRoleBinding(extensionContext, Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/021-ClusterRoleBinding-strimzi-cluster-operator.yaml", namespace);
        // 030-ClusterRoleBinding
        ClusterRoleBindingResource.clusterRoleBinding(extensionContext, Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/030-ClusterRoleBinding-strimzi-cluster-operator-kafka-broker-delegation.yaml", namespace);
        // 033-ClusterRoleBinding
        ClusterRoleBindingResource.clusterRoleBinding(extensionContext, Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/033-ClusterRoleBinding-strimzi-cluster-operator-kafka-client-delegation.yaml", namespace);
    }

    /**
     * Delete ServiceAccount, Roles and CRDs from kubernetes cluster.
     */
    public void deleteClusterOperatorInstallFiles() {
        while (!clusterOperatorConfigs.empty()) {
            String clusterOperatorConfig = clusterOperatorConfigs.pop();
            LOGGER.info("Deleting configuration file: {}", clusterOperatorConfig);
            cmdKubeClient().delete(clusterOperatorConfig);
        }
    }

    public void unInstall() {
        LOGGER.info("Un-installing cluster operator from {} namespace", Constants.INFRA_NAMESPACE);
        BeforeAllOnce.getSharedExtensionContext().getStore(ExtensionContext.Namespace.GLOBAL).put(Constants.PREPARE_OPERATOR_ENV_KEY + namespaceInstallTo, null);

        // trigger that we will again create namespace

        if (Environment.isHelmInstall()) {
            helmResource.delete();
        } else if (Environment.isOlmInstall()) {
            olmResource.delete();
        } else {
            // Clear cluster from all created namespaces and configurations files for cluster operator.
            deleteClusterOperatorInstallFiles();
            KubeClusterResource.getInstance().deleteCustomResources(BeforeAllOnce.getSharedExtensionContext());
            KubeClusterResource.getInstance().deleteNamespace(BeforeAllOnce.getSharedExtensionContext(), Constants.INFRA_NAMESPACE);
        }
    }

    public SetupClusterOperator rollbackToDefaultConfiguration() {
        // un-install old cluster operator
        unInstall();

        // install new one with default configuration
        return buildDefaultInstallation().runInstallation();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SetupClusterOperator that = (SetupClusterOperator) o;
        return operationTimeout == that.operationTimeout &&
            reconciliationInterval == that.reconciliationInterval &&
            Objects.equals(cluster, that.cluster) &&
            Objects.equals(clusterOperatorConfigs, that.clusterOperatorConfigs) &&
            Objects.equals(helmResource, that.helmResource) &&
            Objects.equals(olmResource, that.olmResource) &&
            Objects.equals(clusterOperatorName, that.clusterOperatorName) &&
            Objects.equals(namespaceInstallTo, that.namespaceInstallTo) &&
            Objects.equals(namespaceToWatch, that.namespaceToWatch) &&
            Objects.equals(bindingsNamespaces, that.bindingsNamespaces) &&
            Objects.equals(extraEnvVars, that.extraEnvVars) &&
            Objects.equals(extraLabels, that.extraLabels) &&
            clusterOperatorRBACType == that.clusterOperatorRBACType;
    }
    @Override
    public int hashCode() {
        return Objects.hash(cluster, clusterOperatorConfigs, helmResource, olmResource, extensionContext,
            clusterOperatorName, namespaceInstallTo, namespaceToWatch, bindingsNamespaces, operationTimeout,
            extraEnvVars, extraLabels, clusterOperatorRBACType);
    }
}
