/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.kubernetes.ClusterRoleBindingResource;
import io.strimzi.systemtest.resources.kubernetes.NetworkPolicyResource;
import io.strimzi.systemtest.resources.kubernetes.RoleBindingResource;
import io.strimzi.systemtest.resources.operator.BundleResource;
import io.strimzi.systemtest.resources.specific.HelmResource;
import io.strimzi.systemtest.resources.specific.OlmResource;
import io.strimzi.systemtest.templates.kubernetes.ClusterRoleBindingTemplates;
import io.strimzi.test.TestUtils;
import io.strimzi.test.executor.Exec;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.cluster.Minishift;
import io.strimzi.test.k8s.cluster.OpenShift;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;

import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class Install {

    private static final Logger LOGGER = LogManager.getLogger(Install.class);
    public static final String CO_INSTALL_DIR = TestUtils.USER_PATH + "/../packaging/install/cluster-operator";

    private KubeClusterResource cluster = KubeClusterResource.getInstance();
    private Stack<String> clusterOperatorConfigs = new Stack<>();
    private HelmResource helmResource = new HelmResource();
    private OlmResource olmResource;

    private ExtensionContext extensionContext;
    private String clusterOperatorName;
    private String namespaceName;
    private String namespaceEnv;
    private List<String> bindingsNamespaces;
    private long operationTimeout;
    private long reconciliationInterval;

    Install() {}
    Install(InstallBuilder builder) {
        this.extensionContext = builder.extensionContext;
        this.clusterOperatorName = builder.clusterOperatorName;
        this.namespaceName = builder.namespaceName;
        this.namespaceEnv = builder.namespaceEnv;
        this.bindingsNamespaces = builder.bindingsNamespaces;
        this.operationTimeout = builder.operationTimeout;
        this.reconciliationInterval = builder.reconciliationInterval;

        this.olmResource = new OlmResource(namespaceName);
    }

    /**
     * This method install Strimzi Cluster Operator based on environment variable configuration.
     * It can install operator by classic way (apply bundle yamls) or use OLM. For OLM you need to set all other OLM env variables.
     * Don't use this method in tests, where specific configuration of CO is needed.
     * @param namespace namespace where CO should be installed into
     */
    public Install runInstallation() {
        if (Environment.isOlmInstall()) {
            LOGGER.info("Going to install ClusterOperator via OLM");
            cluster.setNamespace(namespaceName);
            cluster.createNamespace(namespaceName);
            // TODO: cluster-wide here just change some envs in olm installation...
//            if (namespaceEnv.equals("*")) {
//            }
            olmResource.create(extensionContext, namespaceName, operationTimeout, reconciliationInterval);
        } else if (Environment.isHelmInstall()) {
            LOGGER.info("Going to install ClusterOperator via Helm");
            cluster.setNamespace(namespaceName);
            cluster.createNamespace(namespaceName);
            // TODO: cluster-wide
//            if (namespaceEnv.equals("*")) {
//            }
            helmResource.create(extensionContext, operationTimeout, reconciliationInterval);
        } else {
            LOGGER.info("Going to install ClusterOperator via Yaml bundle");
            prepareEnvForOperator(extensionContext, namespaceName, bindingsNamespaces);
            if (Environment.isNamespaceRbacScope()) {
                // if roles only, only deploy the rolebindings
                applyRoleBindings(extensionContext, namespaceName, namespaceName);
            } else {
                applyBindings(extensionContext, namespaceName, bindingsNamespaces);
            }
            // cluster-wide installation
            if (namespaceEnv.equals("*")) {
                // Create ClusterRoleBindings that grant cluster-wide access to all OpenShift projects
                List<ClusterRoleBinding> clusterRoleBindingList = ClusterRoleBindingTemplates.clusterRoleBindingsForAllNamespaces(namespaceName);
                clusterRoleBindingList.forEach(clusterRoleBinding ->
                    ClusterRoleBindingResource.clusterRoleBinding(extensionContext, clusterRoleBinding));
            }
            // 060-Deployment
            ResourceManager.setCoDeploymentName(clusterOperatorName);
            ResourceManager.getInstance().createResource(extensionContext,
                new BundleResource.BundleResourceBuilder()
                    .withName(Constants.STRIMZI_DEPLOYMENT_NAME)
                    .withNamespaceName(namespaceName)
                    .withNamespaceEnv(namespaceEnv)
                    .withOperationTimeout(operationTimeout)
                    .withReconciliationInterval(reconciliationInterval)
                    .buildBundleInstance()
                    .buildBundleDeployment()
                    .build());
        }
        return this;
    }

    public static class InstallBuilder {

        private ExtensionContext extensionContext;
        private String clusterOperatorName;
        private String namespaceName;
        private String namespaceEnv;
        private List<String> bindingsNamespaces;
        private long operationTimeout;
        private long reconciliationInterval;

        public InstallBuilder withExtensionContext(ExtensionContext extensionContext) {
            this.extensionContext = extensionContext;
            return self();
        }
        public InstallBuilder withClusterOperatorName(String clusterOperatorName) {
            this.clusterOperatorName = clusterOperatorName;
            return self();
        }
        public InstallBuilder withNamespaceName(String namespaceName) {
            this.namespaceName = namespaceName;
            return self();
        }
        public InstallBuilder withNamespaceEnv(String namespaceEnv) {
            this.namespaceEnv = namespaceEnv;
            return self();
        }
        public InstallBuilder withBindingsNamespaces(List<String> bindingsNamespaces) {
            this.bindingsNamespaces = bindingsNamespaces;
            return self();
        }
        public InstallBuilder withOperationTimeout(long operationTimeout) {
            this.operationTimeout = operationTimeout;
            return self();
        }
        public InstallBuilder withReconciliationInterval(long reconciliationInterval) {
            this.reconciliationInterval = reconciliationInterval;
            return self();
        }

        private InstallBuilder self() {
            return this;
        }

        public Install createInstallation() {
            return new Install(this);
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
        cluster.createNamespaces(clientNamespace, namespaces);
        cluster.createCustomResources(resources);
        applyClusterOperatorInstallFiles(clientNamespace);
        NetworkPolicyResource.applyDefaultNetworkPolicySettings(extensionContext, namespaces);

        if (cluster.cluster() instanceof Minishift || cluster.cluster() instanceof OpenShift) {
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
     * @param resources list of path to yaml files with resources specifications
     */
    public void prepareEnvForOperator(ExtensionContext extensionContext, String clientNamespace, String... resources) {
        prepareEnvForOperator(extensionContext, clientNamespace, Collections.singletonList(clientNamespace), resources);
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
        clusterOperatorConfigs.clear();
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
    public static File switchClusterRolesToRolesIfNeeded(File oldFile) {
        if (Environment.isNamespaceRbacScope()) {
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

    protected static void applyRoleBindings(ExtensionContext extensionContext, String namespace, String bindingsNamespace) {
        // 020-RoleBinding
        RoleBindingResource.roleBinding(extensionContext, Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/020-RoleBinding-strimzi-cluster-operator.yaml", namespace, bindingsNamespace);
        // 031-RoleBinding
        RoleBindingResource.roleBinding(extensionContext, Constants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/031-RoleBinding-strimzi-cluster-operator-entity-operator-delegation.yaml", namespace, bindingsNamespace);
    }

    /**
     * Method to apply Strimzi cluster operator specific RoleBindings and ClusterRoleBindings for specific namespaces.
     * @param namespace namespace where CO will be deployed to
     * @param bindingsNamespaces list of namespaces where Bindings should be deployed to
     */
    public static void applyBindings(ExtensionContext extensionContext, String namespace, List<String> bindingsNamespaces) {
        for (String bindingsNamespace : bindingsNamespaces) {
            applyClusterRoleBindings(extensionContext, namespace);
            applyRoleBindings(extensionContext, namespace, bindingsNamespace);
        }
    }

    /**
     * Method for apply Strimzi cluster operator specific Role and ClusterRole bindings for specific namespaces.
     * @param namespace namespace where CO will be deployed to
     */
    public static void applyBindings(ExtensionContext extensionContext, String namespace) {
        applyBindings(extensionContext, namespace, Collections.singletonList(namespace));
    }

    /**
     * Method for apply Strimzi cluster operator specific Role and ClusterRole bindings for specific namespaces.
     * @param namespace namespace where CO will be deployed to
     * @param bindingsNamespaces array of namespaces where Bindings should be deployed to
     */
    public static void applyBindings(ExtensionContext extensionContext, String namespace, String... bindingsNamespaces) {
        applyBindings(extensionContext, namespace, Arrays.asList(bindingsNamespaces));
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
}
