/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.operator;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinition;
import io.fabric8.kubernetes.api.model.coordination.v1.Lease;
import io.fabric8.kubernetes.api.model.coordination.v1.LeaseBuilder;
import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBuilder;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleBuilder;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.enums.ClusterOperatorRBACType;
import io.strimzi.systemtest.enums.OlmInstallationStrategy;
import io.strimzi.systemtest.resources.NamespaceManager;
import io.strimzi.systemtest.resources.ResourceItem;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.kubernetes.ClusterRoleBindingResource;
import io.strimzi.systemtest.resources.kubernetes.RoleBindingResource;
import io.strimzi.systemtest.resources.kubernetes.RoleResource;
import io.strimzi.systemtest.resources.operator.configuration.OlmConfiguration;
import io.strimzi.systemtest.resources.operator.configuration.OlmConfigurationBuilder;
import io.strimzi.systemtest.resources.operator.specific.HelmResource;
import io.strimzi.systemtest.resources.operator.specific.OlmResource;
import io.strimzi.systemtest.templates.kubernetes.ClusterRoleBindingTemplates;
import io.strimzi.systemtest.utils.kubeUtils.controllers.DeploymentUtils;
import io.strimzi.systemtest.utils.specific.OlmUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.executor.Exec;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.cluster.OpenShift;
import io.strimzi.test.logs.CollectorElement;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.PreconditionViolationException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Stack;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.strimzi.test.k8s.KubeClusterResource.kubeClient;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * SetupClusterOperator encapsulates the whole installation process of Cluster Operator (i.e., RoleBinding, ClusterRoleBinding,
 * ConfigMap, Deployment, CustomResourceDefinition, preparation of the Namespace). Based on the @code{Environment}
 * values, this class decides how Cluster Operator should be installed (i.e., Olm, Helm, Bundle). Moreover, it provides
 * @code{rollbackToDefaultConfiguration()} method, which basically re-install Cluster Operator to the default values. In
 * case user wants to edit specific installation, one can use @code{defaultInstallation()}, which returns SetupClusterOperatorBuilder.
 */
@SuppressFBWarnings("SSD_DO_NOT_USE_INSTANCE_LOCK_ON_SHARED_STATIC_DATA")
public class SetupClusterOperator {

    private static final Logger LOGGER = LogManager.getLogger(SetupClusterOperator.class);
    public static final String CO_INSTALL_DIR = TestUtils.USER_PATH + "/../packaging/install/cluster-operator";

    private static SetupClusterOperator instanceHolder;

    private static KubeClusterResource cluster = KubeClusterResource.getInstance();
    private static HelmResource helmResource;
    private static OlmResource olmResource;

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
    private int replicas = 1;

    private String testClassName;
    // by default, we expect at least empty method name in order to collect logs correctly
    private String testMethodName = "";
    private List<RoleBinding> roleBindings;
    private List<Role> roles;
    private List<ClusterRole> clusterRoles;
    private List<ClusterRoleBinding> clusterRoleBindings;

    private static final Predicate<String> IS_OLM_CLUSTER_WIDE = namespaceToWatch -> namespaceToWatch.equals(TestConstants.WATCH_ALL_NAMESPACES) || namespaceToWatch.split(",").length > 1;
    private static final Predicate<SetupClusterOperator> IS_EMPTY = co -> co.helmResource == null && co.olmResource == null &&
        co.extensionContext == null && co.clusterOperatorName == null && co.namespaceInstallTo == null &&
        co.namespaceToWatch == null && co.bindingsNamespaces == null && co.operationTimeout == 0 && co.reconciliationInterval == 0 &&
        co.extraEnvVars == null && co.clusterOperatorRBACType == null && co.testClassName == null && co.testMethodName == null;

    public synchronized static SetupClusterOperator getInstance() {
        if (instanceHolder == null) {
            // empty cluster operator
            instanceHolder = new SetupClusterOperator();
        }
        return instanceHolder;
    }

    public SetupClusterOperator() {}
    @SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
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
        this.replicas = builder.replicas;
        this.roleBindings = builder.roleBindings;
        this.roles = builder.roles;
        this.clusterRoles = builder.clusterRoles;
        this.clusterRoleBindings = builder.clusterRoleBindings;

        // assign defaults is something is not specified
        if (this.clusterOperatorName == null || this.clusterOperatorName.isEmpty()) {
            this.clusterOperatorName = TestConstants.STRIMZI_DEPLOYMENT_NAME;
        }
        // if namespace is not set we install operator to 'co-namespace'
        if (this.namespaceInstallTo == null || this.namespaceInstallTo.isEmpty()) {
            this.namespaceInstallTo = TestConstants.CO_NAMESPACE;
        }
        if (this.namespaceToWatch == null) {
            this.namespaceToWatch = this.namespaceInstallTo;
        }
        if (this.bindingsNamespaces == null) {
            this.bindingsNamespaces = new ArrayList<>();
            this.bindingsNamespaces.add(this.namespaceInstallTo);
        }
        if (this.operationTimeout == 0) {
            this.operationTimeout = TestConstants.CO_OPERATION_TIMEOUT_DEFAULT;
        }
        if (this.reconciliationInterval == 0) {
            this.reconciliationInterval = TestConstants.RECONCILIATION_INTERVAL;
        }
        if (this.extraEnvVars == null) {
            this.extraEnvVars = new ArrayList<>();
        }
        if (this.extraLabels == null) {
            this.extraLabels = new HashMap<>();
        }
        if (this.clusterOperatorRBACType == null) {
            this.clusterOperatorRBACType = ClusterOperatorRBACType.CLUSTER;
        }
        instanceHolder = this;
    }

    /**
     * Auxiliary method, which check if Cluster Operator namespace is created.
     * @return true if Cluster Operator namespace not created, otherwise false (i.e., namespace already created)
     */
    private boolean isClusterOperatorNamespaceNotCreated() {
        return extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).get(TestConstants.PREPARE_OPERATOR_ENV_KEY + namespaceInstallTo) == null;
    }

    /**
     * Auxiliary method, which provides default Cluster Operator instance. By default we mean using in all cases
     * @code{BeforeAllOnce.getSharedExtensionContext())} and other attributes are dependent base on the installation type
     * (i.e., Olm, Helm, Bundle) and RBAC setup (i.e., Cluster, Namespace).
     *
     *
     * @return default Cluster Operator builder
     */
    public SetupClusterOperatorBuilder defaultInstallation() {
        // default initialization
        SetupClusterOperatorBuilder clusterOperatorBuilder = new SetupClusterOperator.SetupClusterOperatorBuilder()
            .withExtensionContext(ResourceManager.getTestContext());

        // RBAC set to `NAMESPACE`
        if (Environment.isNamespaceRbacScope() && !Environment.isHelmInstall()) {
            clusterOperatorBuilder = clusterOperatorBuilder.withNamespace(TestConstants.CO_NAMESPACE);
            return clusterOperatorBuilder;
        }
        // otherwise
        return clusterOperatorBuilder
            .withNamespace(TestConstants.CO_NAMESPACE)
            .withWatchingNamespaces(TestConstants.WATCH_ALL_NAMESPACES);
    }

    /**
     * This method install Strimzi Cluster Operator based on environment variable configuration.
     * It can install operator by classic way (apply bundle yamls) or use OLM. For OLM you need to set all other OLM env variables.
     * Don't use this method in tests, where specific configuration of CO is needed.
     */
    @SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
    public SetupClusterOperator runInstallation() {
        LOGGER.info("Cluster Operator installation configuration:\n{}", this::prettyPrint);
        LOGGER.debug("Cluster Operator installation configuration:\n{}", this::toString);

        this.testClassName = this.extensionContext.getRequiredTestClass() != null ? this.extensionContext.getRequiredTestClass().getName() : "";

        try {
            if (this.extensionContext.getRequiredTestMethod() != null) {
                this.testMethodName = this.extensionContext.getRequiredTestMethod().getName();
            }
        } catch (PreconditionViolationException e) {
            LOGGER.debug("Test method is not present: {}\n{}", e.getMessage(), e.getCause());
            // getRequiredTestMethod() is not present, in @BeforeAll scope so we're avoiding PreconditionViolationException exception
            this.testMethodName = "";
        }

        if (Environment.isOlmInstall()) {
            runOlmInstallation();
        } else if (Environment.isHelmInstall()) {
            helmInstallation();
        } else {
            bundleInstallation();
        }
        return this;
    }

    public SetupClusterOperator runBundleInstallation() {
        LOGGER.info("Cluster Operator installation configuration:\n{}", this::toString);
        bundleInstallation();
        return this;
    }

    public SetupClusterOperator runOlmInstallation() {
        LOGGER.info("Cluster Operator installation configuration:\n{}", this::toString);
        olmInstallation();
        return this;
    }

    public SetupClusterOperator runHelmInstallation() {
        LOGGER.info("Cluster Operator installation configuration:\n{}", this::toString);
        helmInstallation();
        return this;
    }

    @SuppressFBWarnings("ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD")
    public SetupClusterOperator runManualOlmInstallation(final String fromOlmChannelName, final String fromVersion) {
        createClusterOperatorNamespaceIfPossible();

        OlmConfiguration olmConfiguration = new OlmConfigurationBuilder()
            .withNamespaceName(this.namespaceInstallTo)
            .withExtensionContext(extensionContext)
            .withOlmInstallationStrategy(OlmInstallationStrategy.Manual)
            .withChannelName(fromOlmChannelName)
            .withOperatorVersion(fromVersion)
            .withEnvVars(extraEnvVars)
            .withOperationTimeout(operationTimeout)
            .withReconciliationInterval(reconciliationInterval)
            .withNamespaceToWatch(this.namespaceInstallTo)
            .build();

        olmResource = new OlmResource(olmConfiguration);
        olmResource.create();

        return this;
    }

    private void helmInstallation() {
        LOGGER.info("Install Cluster Operator via Helm");
        helmResource = new HelmResource(namespaceInstallTo, namespaceToWatch);
        createClusterOperatorNamespaceIfPossible();
        helmResource.create(extensionContext, operationTimeout, reconciliationInterval, extraEnvVars, replicas);
    }

    private void bundleInstallation() {
        LOGGER.info("Install Cluster Operator via Yaml bundle");
        // check if namespace is already created
        createClusterOperatorNamespaceIfPossible();
        prepareEnvForOperator(namespaceInstallTo, bindingsNamespaces);
        // if we manage directly in the individual test one of the Role, ClusterRole, RoleBindings and ClusterRoleBinding we must do it
        // everything by ourselves in scope of RBAC permissions otherwise we apply the default one
        if (this.isRolesAndBindingsManagedByAnUser()) {
            final List<HasMetadata> listOfRolesAndBindings = Stream.of(
                    this.roles, this.roleBindings, this.clusterRoles, this.clusterRoleBindings)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
            for (final HasMetadata itemRoleOrBinding : listOfRolesAndBindings) {
                ResourceManager.getInstance().createResourceWithWait(itemRoleOrBinding);
            }
        } else {
            LOGGER.info("Install default bindings");
            this.applyDefaultBindings();
        }

        // 060-Deployment
        ResourceManager.setCoDeploymentName(clusterOperatorName);
        ResourceManager.getInstance().createResourceWithWait(
            new BundleResource.BundleResourceBuilder()
                .withReplicas(replicas)
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

    private void olmInstallation() {
        LOGGER.info("Install Cluster Operator via OLM");

        OlmConfigurationBuilder olmConfiguration = new OlmConfigurationBuilder()
            .withExtensionContext(extensionContext)
            .withOperationTimeout(operationTimeout)
            .withReconciliationInterval(reconciliationInterval)
            .withEnvVars(extraEnvVars);

        // cluster-wide olm co-operator or multi-namespaces in once we also deploy cluster-wide
        if (IS_OLM_CLUSTER_WIDE.test(namespaceToWatch)) {
            // if RBAC is enable we don't run tests in parallel mode and with that said we don't create another namespaces
            if (!Environment.isNamespaceRbacScope()) {
                bindingsNamespaces = bindingsNamespaces.contains(TestConstants.CO_NAMESPACE) ? Collections.singletonList(
                    TestConstants.CO_NAMESPACE) : bindingsNamespaces;

                createClusterOperatorNamespaceIfPossible();
                createClusterRoleBindings();

                // watch all namespaces
                olmConfiguration.withNamespaceToWatch(TestConstants.WATCH_ALL_NAMESPACES);

                olmResource = new OlmResource(olmConfiguration.withNamespaceName(namespaceInstallTo).build());
                olmResource.create();
            }
        } else {
            // single-namespace olm co-operator
            createClusterOperatorNamespaceIfPossible();

            // watch same namespace, where operator is installed
            olmConfiguration.withNamespaceToWatch(namespaceInstallTo);

            olmResource = new OlmResource(olmConfiguration.withNamespaceName(namespaceInstallTo).build());
            olmResource.create();
        }
    }

    /**
     * Upgrade cluster operator by updating subscription and obtaining new install plan,
     * which has not been used yet and also approves the installation
     */
    public void upgradeClusterOperator(OlmConfiguration olmConfiguration) {
        if (kubeClient().listPodsByPrefixInName(ResourceManager.getCoDeploymentName()).isEmpty()) {
            throw new RuntimeException("We can not perform upgrade! Cluster Operator Pod is not present.");
        }

        updateSubscription(olmConfiguration);
        OlmUtils.waitUntilNonUsedInstallPlanWithSpecificCsvIsPresentAndApprove(namespaceInstallTo, olmConfiguration.getCsvName());
        DeploymentUtils.waitForDeploymentAndPodsReady(namespaceInstallTo, olmConfiguration.getOlmOperatorDeploymentName(), 1);
    }

    public void updateSubscription(OlmConfiguration olmConfiguration) {
        // add CSV resource to the end of the stack -> to be deleted after the subscription and operator group
        ResourceManager.STORED_RESOURCES.get(olmConfiguration.getExtensionContext().getDisplayName())
            .add(ResourceManager.STORED_RESOURCES.get(olmConfiguration.getExtensionContext().getDisplayName()).size(), new ResourceItem(olmResource::deleteCSV));
        olmResource.updateSubscription(olmConfiguration);
    }

    private void createClusterOperatorNamespaceIfPossible() {
        if (this.isClusterOperatorNamespaceNotCreated()) {
            cluster.setNamespace(namespaceInstallTo);

            if (this.extensionContext != null) {
                ResourceManager.STORED_RESOURCES.computeIfAbsent(this.extensionContext.getDisplayName(), k -> new Stack<>());
                ResourceManager.STORED_RESOURCES.get(this.extensionContext.getDisplayName()).push(
                    new ResourceItem<>(this::deleteClusterOperatorNamespace));


                // when RBAC=NAMESPACE, we need to add a binding namespace Environment.TEST_SUITE_NAMESPACE, because now,
                // we always has two namespaces (i.e., co-namespace and test-suite-namespace)
                if (Environment.isNamespaceRbacScope() || this.clusterOperatorRBACType.equals(ClusterOperatorRBACType.NAMESPACE)) {
                    if (!this.bindingsNamespaces.contains(Environment.TEST_SUITE_NAMESPACE)) {
                        this.bindingsNamespaces.add(Environment.TEST_SUITE_NAMESPACE);
                    }
                    if (!this.namespaceToWatch.contains(Environment.TEST_SUITE_NAMESPACE)) {
                        this.namespaceToWatch += "," + Environment.TEST_SUITE_NAMESPACE;
                    }
                }

                final CollectorElement collectorElement = this.testMethodName == null || this.testMethodName.isEmpty() ?
                    CollectorElement.createCollectorElement(this.testClassName) :
                    CollectorElement.createCollectorElement(this.testClassName, this.testMethodName);

                NamespaceManager.getInstance().createNamespaces(namespaceInstallTo, collectorElement, bindingsNamespaces);

                this.extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.PREPARE_OPERATOR_ENV_KEY + namespaceInstallTo, true);
            }
        }
    }

    private void deleteClusterOperatorNamespace() {
        LOGGER.info("Deleting Namespace {}", this.namespaceInstallTo);
        NamespaceManager.getInstance().deleteNamespaceWithWaitAndRemoveFromSet(this.namespaceInstallTo, CollectorElement.createCollectorElement(testClassName, testMethodName));
    }

    private void createClusterRoleBindings() {
        // Create ClusterRoleBindings that grant cluster-wide access to all OpenShift projects
        List<ClusterRoleBinding> clusterRoleBindingList = ClusterRoleBindingTemplates.clusterRoleBindingsForAllNamespaces(namespaceInstallTo);
        clusterRoleBindingList.forEach(clusterRoleBinding ->
            ResourceManager.getInstance().createResourceWithWait(clusterRoleBinding));
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
        private int replicas = 1;
        private List<RoleBinding> roleBindings;
        private List<Role> roles;
        private List<ClusterRole> clusterRoles;
        private List<ClusterRoleBinding> clusterRoleBindings;

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

        public SetupClusterOperatorBuilder addToTheWatchingNamespaces(String namespaceToWatch) {
            if (this.namespaceToWatch != null) {
                if (!this.namespaceToWatch.equals("*")) {
                    this.namespaceToWatch += "," + namespaceToWatch;
                }
            } else {
                this.namespaceToWatch = namespaceToWatch;
            }
            return self();
        }

        public SetupClusterOperatorBuilder withBindingsNamespaces(List<String> bindingsNamespaces) {
            this.bindingsNamespaces = bindingsNamespaces;
            return self();
        }

        public SetupClusterOperatorBuilder addToTheBindingsNamespaces(String bindingsNamespace) {
            if (this.bindingsNamespaces != null) {
                this.bindingsNamespaces = new ArrayList<>(this.bindingsNamespaces);
            } else {
                this.bindingsNamespaces = new ArrayList<>(Collections.singletonList(bindingsNamespace));
            }
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

        public SetupClusterOperatorBuilder withReplicas(int replicas) {
            this.replicas = replicas;
            return self();
        }

        public SetupClusterOperatorBuilder withRoleBindings(final List<RoleBinding> roleBindings) {
            this.roleBindings = roleBindings;
            return self();
        }

        public SetupClusterOperatorBuilder withRoles(final List<Role> roles) {
            this.roles = roles;
            return self();
        }

        public SetupClusterOperatorBuilder withClusterRoles(final List<ClusterRole> clusterRoles) {
            this.clusterRoles = clusterRoles;
            return self();
        }

        public SetupClusterOperatorBuilder withClusterRoleBindings(final List<ClusterRoleBinding> clusterRoleBindings) {
            this.clusterRoleBindings = clusterRoleBindings;
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
     */
    public void prepareEnvForOperator(String clientNamespace, List<String> namespaces) {
        assumeTrue(!Environment.isHelmInstall() && !Environment.isOlmInstall());
        applyClusterOperatorInstallFiles(clientNamespace);

        if (cluster.cluster() instanceof OpenShift) {
            // This is needed in case you are using internal kubernetes registry and you want to pull images from there
            if (kubeClient().getNamespace(Environment.STRIMZI_ORG) != null) {
                for (String namespace : namespaces) {
                    LOGGER.debug("Setting group policy for Openshift registry in Namespace: " + namespace);
                    Exec.exec(null, Arrays.asList("oc", "policy", "add-role-to-group", "system:image-puller", "system:serviceaccounts:" + namespace, "-n", Environment.STRIMZI_ORG), 0, Level.DEBUG, false);
                }
            }
        }
    }

    public String changeLeaseNameInResourceIfNeeded(String yamlPath) {
        final EnvVar leaseEnvVar = extraEnvVars.stream().filter(envVar -> envVar.getName().equals("STRIMZI_LEADER_ELECTION_LEASE_NAME")).findFirst().orElse(null);
        Map.Entry<String, String> resourceEntry = TestConstants.LEASE_FILES_AND_RESOURCES.entrySet().stream().filter(entry -> yamlPath.equals(entry.getValue())).findFirst().orElse(null);

        if (leaseEnvVar != null && resourceEntry != null) {
            try {
                String[] path = yamlPath.split("/");
                String fileName = path[path.length - 1].replace(TestConstants.STRIMZI_DEPLOYMENT_NAME, leaseEnvVar.getValue()).replace(".yaml", "");
                File tmpFile = Files.createTempFile(fileName, "yaml").toFile();

                String tmpFileContent;
                final String resourceName = leaseEnvVar.getValue() + "-leader-election";

                switch (resourceEntry.getKey()) {
                    case TestConstants.ROLE:
                        RoleBuilder roleBuilder = new RoleBuilder(TestUtils.configFromYaml(yamlPath, Role.class))
                            .editMetadata()
                                .withName(resourceName)
                            .endMetadata()
                            .editMatchingRule(rule -> rule.getFirstResourceName().equals(TestConstants.STRIMZI_DEPLOYMENT_NAME))
                                .withResourceNames(leaseEnvVar.getValue())
                            .endRule();

                        tmpFileContent = TestUtils.toYamlString(roleBuilder.build());
                        break;
                    case TestConstants.CLUSTER_ROLE:
                        ClusterRoleBuilder clusterRoleBuilder = new ClusterRoleBuilder(TestUtils.configFromYaml(yamlPath, ClusterRole.class))
                            .editMetadata()
                                .withName(resourceName)
                            .endMetadata()
                            .editMatchingRule(rule -> rule.getResourceNames().stream().findAny().orElse("").equals(
                                TestConstants.STRIMZI_DEPLOYMENT_NAME))
                                .withResourceNames(leaseEnvVar.getValue())
                            .endRule();

                        tmpFileContent = TestUtils.toYamlString(clusterRoleBuilder.build());
                        break;
                    case TestConstants.ROLE_BINDING:
                        RoleBindingBuilder roleBindingBuilder = new RoleBindingBuilder(TestUtils.configFromYaml(yamlPath, RoleBinding.class))
                            .editMetadata()
                                .withName(resourceName)
                            .endMetadata()
                            .editRoleRef()
                                .withName(resourceName)
                            .endRoleRef();

                        tmpFileContent = TestUtils.toYamlString(roleBindingBuilder.build());
                        break;
                    default:
                        return yamlPath;
                }

                TestUtils.writeFile(tmpFile.getAbsolutePath(), tmpFileContent);
                return tmpFile.getAbsolutePath();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            return yamlPath;
        }
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
                .toList();

        for (File operatorFile : operatorFiles) {
            File createFile = operatorFile;

            if (createFile.getName().contains(TestConstants.CLUSTER_ROLE + "-")) {
                createFile = switchClusterRolesToRolesIfNeeded(createFile);
            }

            final String resourceType = createFile.getName().split("-")[1];

            switch (resourceType) {
                case TestConstants.ROLE:
                    if (!this.isRolesAndBindingsManagedByAnUser()) {
                        Role role = TestUtils.configFromYaml(createFile, Role.class);
                        ResourceManager.getInstance().createResourceWithWait(new RoleBuilder(role)
                            .editMetadata()
                                .withNamespace(namespace)
                            .endMetadata()
                            .build());
                    }
                    break;
                case TestConstants.CLUSTER_ROLE:
                    if (!this.isRolesAndBindingsManagedByAnUser()) {
                        ClusterRole clusterRole = TestUtils.configFromYaml(changeLeaseNameInResourceIfNeeded(createFile.getAbsolutePath()), ClusterRole.class);
                        ResourceManager.getInstance().createResourceWithWait(clusterRole);
                    }
                    break;
                case TestConstants.SERVICE_ACCOUNT:
                    ServiceAccount serviceAccount = TestUtils.configFromYaml(createFile, ServiceAccount.class);
                    ResourceManager.getInstance().createResourceWithWait(new ServiceAccountBuilder(serviceAccount)
                        .editMetadata()
                            .withNamespace(namespace)
                        .endMetadata()
                        .build());
                    break;
                case TestConstants.CONFIG_MAP:
                    ConfigMap configMap = TestUtils.configFromYaml(createFile, ConfigMap.class);
                    ResourceManager.getInstance().createResourceWithWait(new ConfigMapBuilder(configMap)
                        .editMetadata()
                            .withNamespace(namespace)
                            .withName(clusterOperatorName)
                        .endMetadata()
                        .build());
                    break;
                case TestConstants.LEASE:
                    // Loads the resource through Fabric8 Kubernetes Client => that way we do not need to add a direct
                    // dependency on Jackson Datatype JSR310 to decode the Lease resource
                    Lease lease = kubeClient().getClient().leases().load(createFile).item();
                    ResourceManager.getInstance().createResourceWithWait(new LeaseBuilder(lease)
                            .editMetadata()
                                .withNamespace(namespace)
                                .withName(clusterOperatorName)
                            .endMetadata()
                            .build());
                    break;
                case TestConstants.CUSTOM_RESOURCE_DEFINITION_SHORT:
                    CustomResourceDefinition customResourceDefinition = TestUtils.configFromYaml(createFile, CustomResourceDefinition.class);
                    ResourceManager.getInstance().createResourceWithWait(customResourceDefinition);
                    break;
                default:
                    LOGGER.error("Unknown installation resource type: {}", resourceType);
                    throw new RuntimeException("Unknown installation resource type:" + resourceType);
            }
        }
    }

    /**
     * Replace all references to ClusterRole to Role.
     * This includes ClusterRoles themselves as well as RoleBindings that reference them.
     */
    public File switchClusterRolesToRolesIfNeeded(File oldFile, boolean explicitConversionEnabled) {
        if (Environment.isNamespaceRbacScope() || this.clusterOperatorRBACType == ClusterOperatorRBACType.NAMESPACE || explicitConversionEnabled) {
            try {
                final String[] fileNameArr = oldFile.getName().split("-");
                // change ClusterRole to Role
                fileNameArr[1] = "Role";
                final String changeFileName = Arrays.stream(fileNameArr).map(item -> "-" + item).collect(Collectors.joining()).substring(1);
                File tmpFile = Files.createTempFile(changeFileName.replace(".yaml", ""), ".yaml").toFile();
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

    public File switchClusterRolesToRolesIfNeeded(File oldFile) {
        return this.switchClusterRolesToRolesIfNeeded(oldFile, false);
    }

    /**
     * Applies the RoleBindings for the Cluster Operator
     *
     * @param namespace         Namespace in which the operator is deployed
     * @param bindingsNamespace Namespace watched by the operator
     */
    public void applyRoleBindings(String namespace, String bindingsNamespace) {
        // 020-RoleBinding => Cluster Operator rights for managing operands
        File roleFile = new File(TestConstants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/020-RoleBinding-strimzi-cluster-operator.yaml");
        RoleBindingResource.roleBinding(switchClusterRolesToRolesIfNeeded(roleFile).getAbsolutePath(), namespace, bindingsNamespace);

        // 022-RoleBinding => Leader election RoleBinding (is only in the operator namespace)
        roleFile = new File(TestConstants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/022-RoleBinding-strimzi-cluster-operator.yaml");
        roleFile = switchClusterRolesToRolesIfNeeded(roleFile);
        RoleBindingResource.roleBinding(changeLeaseNameInResourceIfNeeded(roleFile.getAbsolutePath()), namespace, namespace);

        // 023-RoleBinding => Leader election RoleBinding (is only in the operator namespace)
        roleFile = new File(TestConstants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/023-RoleBinding-strimzi-cluster-operator.yaml");
        RoleBindingResource.roleBinding(switchClusterRolesToRolesIfNeeded(roleFile).getAbsolutePath(), namespace, bindingsNamespace);

        // 031-RoleBinding => Entity Operator delegation
        roleFile = new File(TestConstants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/031-RoleBinding-strimzi-cluster-operator-entity-operator-delegation.yaml");
        RoleBindingResource.roleBinding(switchClusterRolesToRolesIfNeeded(roleFile).getAbsolutePath(), namespace, bindingsNamespace);
    }

    public void applyRoles(String namespace) {
        File roleFile = new File(TestConstants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/020-ClusterRole-strimzi-cluster-operator-role.yaml");
        RoleResource.role(switchClusterRolesToRolesIfNeeded(roleFile).getAbsolutePath(), namespace);

        roleFile = new File(TestConstants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/021-ClusterRole-strimzi-cluster-operator-role.yaml");
        RoleResource.role(switchClusterRolesToRolesIfNeeded(roleFile).getAbsolutePath(), namespace);

        roleFile = new File(TestConstants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/022-ClusterRole-strimzi-cluster-operator-role.yaml");
        roleFile = switchClusterRolesToRolesIfNeeded(roleFile);
        RoleResource.role(changeLeaseNameInResourceIfNeeded(roleFile.getAbsolutePath()), namespace);

        roleFile = new File(TestConstants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/023-ClusterRole-strimzi-cluster-operator-role.yaml");
        RoleResource.role(switchClusterRolesToRolesIfNeeded(roleFile).getAbsolutePath(), namespace);

        roleFile = new File(TestConstants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/030-ClusterRole-strimzi-kafka-broker.yaml");
        RoleResource.role(switchClusterRolesToRolesIfNeeded(roleFile).getAbsolutePath(), namespace);

        roleFile = new File(TestConstants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/031-ClusterRole-strimzi-entity-operator.yaml");
        RoleResource.role(switchClusterRolesToRolesIfNeeded(roleFile).getAbsolutePath(), namespace);

        roleFile = new File(TestConstants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/033-ClusterRole-strimzi-kafka-client.yaml");
        RoleResource.role(switchClusterRolesToRolesIfNeeded(roleFile).getAbsolutePath(), namespace);
    }

    /**
     * Method to apply Strimzi cluster operator specific RoleBindings and ClusterRoleBindings for specific namespaces.
     */
    public void applyDefaultBindings() {
        if (Environment.isNamespaceRbacScope() || this.clusterOperatorRBACType.equals(ClusterOperatorRBACType.NAMESPACE)) {
            // if roles only, only deploy the rolebindings
            for (String bindingsNamespace : bindingsNamespaces) {
                applyRoles(bindingsNamespace);
                applyRoleBindings(namespaceInstallTo, bindingsNamespace);
            }
            // RoleBindings also deployed in CO namespace
            applyRoleBindings(namespaceInstallTo, namespaceInstallTo);
        } else {
            for (String bindingsNamespace : bindingsNamespaces) {
                applyClusterRoleBindings(extensionContext, this.namespaceInstallTo);
                applyRoleBindings(this.namespaceInstallTo, bindingsNamespace);
            }
        }
        // cluster-wide installation
        if (namespaceToWatch.equals(TestConstants.WATCH_ALL_NAMESPACES)) {
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
        ClusterRoleBindingResource.clusterRoleBinding(TestConstants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/021-ClusterRoleBinding-strimzi-cluster-operator.yaml", namespace);
        // 030-ClusterRoleBinding
        ClusterRoleBindingResource.clusterRoleBinding(TestConstants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/030-ClusterRoleBinding-strimzi-cluster-operator-kafka-broker-delegation.yaml", namespace);
        // 033-ClusterRoleBinding
        ClusterRoleBindingResource.clusterRoleBinding(TestConstants.PATH_TO_PACKAGING_INSTALL_FILES + "/cluster-operator/033-ClusterRoleBinding-strimzi-cluster-operator-kafka-client-delegation.yaml", namespace);
    }

    public synchronized void unInstall() {
        if (IS_EMPTY.test(this)) {
            LOGGER.info(String.join("", Collections.nCopies(76, "=")));
            LOGGER.info("Skip un-installation of the Cluster Operator");
            LOGGER.info(String.join("", Collections.nCopies(76, "=")));
        } else {
            LOGGER.info(String.join("", Collections.nCopies(76, "=")));
            LOGGER.info("Un-installing Cluster Operator from Namespace: {}", namespaceInstallTo);
            LOGGER.info(String.join("", Collections.nCopies(76, "=")));

            if (this.extensionContext != null) {
                this.extensionContext.getStore(ExtensionContext.Namespace.GLOBAL).put(TestConstants.PREPARE_OPERATOR_ENV_KEY + namespaceInstallTo, null);
            }

            // trigger that we will again create namespace
            if (Environment.isHelmInstall()) {
                helmResource.delete();
            } else {
                // clear all resources related to the extension context
                try {
                    if (!Environment.SKIP_TEARDOWN) {
                        ResourceManager.getInstance().deleteResources();
                    }
                } catch (Exception e) {
                    Thread.currentThread().interrupt();
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        SetupClusterOperator otherInstallation = (SetupClusterOperator) other;

        return operationTimeout == otherInstallation.operationTimeout &&
            reconciliationInterval == otherInstallation.reconciliationInterval &&
            Objects.equals(cluster, otherInstallation.cluster) &&
            Objects.equals(clusterOperatorName, otherInstallation.clusterOperatorName) &&
            Objects.equals(namespaceInstallTo, otherInstallation.namespaceInstallTo) &&
            Objects.equals(namespaceToWatch, otherInstallation.namespaceToWatch) &&
            Objects.equals(bindingsNamespaces, otherInstallation.bindingsNamespaces) &&
            Objects.equals(extraEnvVars, otherInstallation.extraEnvVars) &&
            Objects.equals(extraLabels, otherInstallation.extraLabels) &&
            clusterOperatorRBACType == otherInstallation.clusterOperatorRBACType;
    }
    @Override
    public int hashCode() {
        return Objects.hash(cluster, helmResource, olmResource, extensionContext,
            clusterOperatorName, namespaceInstallTo, namespaceToWatch, bindingsNamespaces, operationTimeout,
            extraEnvVars, extraLabels, clusterOperatorRBACType);
    }

    /**
     * Auxiliary method, which compares @{this} CO configuration with @code{otherClusterOperator} and return string of
     * differences.
     * @param otherClusterOperator cluster operator configuration, which is compared
     * @return string of attributes which differs from @code{otherClusterOperator} configuration each of separate row
     */
    public String diff(final SetupClusterOperator otherClusterOperator) {
        final StringBuilder diffString = new StringBuilder();

        if (this.operationTimeout != otherClusterOperator.operationTimeout) {
            diffString.append("operationTimeout=").append(this.operationTimeout).append(", ");
        }
        if (this.reconciliationInterval != otherClusterOperator.reconciliationInterval) {
            diffString.append("reconciliationInterval=").append(this.reconciliationInterval).append(", ");
        }
        if (!this.clusterOperatorName.equals(otherClusterOperator.clusterOperatorName)) {
            diffString.append("clusterOperatorName=").append(this.clusterOperatorName).append(", ");
        }
        if (!this.namespaceInstallTo.equals(otherClusterOperator.namespaceInstallTo)) {
            diffString.append("namespaceInstallTo=").append(this.namespaceInstallTo).append(", ");
        }
        if (!this.namespaceToWatch.equals(otherClusterOperator.namespaceToWatch)) {
            diffString.append("namespaceToWatch=").append(this.namespaceToWatch).append(", ");
        }
        if (this.bindingsNamespaces != otherClusterOperator.bindingsNamespaces) {
            diffString.append("bindingsNamespaces=").append(this.bindingsNamespaces).append(", ");
        }
        if (!this.extraEnvVars.equals(otherClusterOperator.extraEnvVars)) {
            diffString.append("extraEnvVars=").append(this.extraEnvVars).append(", ");
        }
        if (!this.extraLabels.equals(otherClusterOperator.extraLabels)) {
            diffString.append("extraLabels=").append(this.extraLabels).append(", ");
        }
        if (this.clusterOperatorRBACType != otherClusterOperator.clusterOperatorRBACType) {
            diffString.append("clusterOperatorRBACType=").append(this.clusterOperatorRBACType).append(", ");
        }

        return diffString.toString();
    }

    @Override
    public String toString() {
        return "SetupClusterOperator{" +
            "cluster=" + cluster +
            ", extensionContext=" + extensionContext +
            ", clusterOperatorName='" + clusterOperatorName + '\'' +
            ", namespaceInstallTo='" + namespaceInstallTo + '\'' +
            ", namespaceToWatch='" + namespaceToWatch + '\'' +
            ", bindingsNamespaces=" + bindingsNamespaces +
            ", operationTimeout=" + operationTimeout +
            ", reconciliationInterval=" + reconciliationInterval +
            ", extraEnvVars=" + extraEnvVars +
            ", extraLabels=" + extraLabels +
            ", clusterOperatorRBACType=" + clusterOperatorRBACType +
            ", testClassName='" + testClassName + '\'' +
            ", testMethodName='" + testMethodName + '\'' +
            '}';
    }

    public String prettyPrint() {
        return String.format("extensionContext=%s%n" +
                "clusterOperatorName=%s%n" +
                "namespaceInstallTo=%s%n" +
                "namespaceToWatch=%s%n" +
                "bindingsNamespaces=%s%n" +
                "operationTimeout=%s%n" +
                "reconciliationInterval=%s%n" +
                "clusterOperatorRBACType=%s%n" +
                "%s%s%s%s", extensionContext,
                clusterOperatorName,
                namespaceInstallTo,
                namespaceToWatch,
                bindingsNamespaces,
                operationTimeout,
                reconciliationInterval,
                clusterOperatorRBACType,
                extraEnvVars.isEmpty() ? "" : ", extraEnvVars=" + extraEnvVars + "%n",
                extraLabels.isEmpty() ? "" : "extraLabels=" + extraLabels + "%n",
                testClassName == null ? "" : "testClassName='" + testClassName + '\'' + "%n",
                testMethodName == null ? "" : "testMethodName='" + testMethodName + '\'' + "%n").trim();
    }

    /**
     * Auxiliary method for retrieve namespace where Cluster Operator installed.
     *
     * @return return value may vary on following:
     *    1. Olm installation           :   return KubeClusterResource.getInstance().getDefaultOlmNamespace() (based on platform)
     *    2. Helm &amp; installation    :   return @code{namespaceInstallTo}
     */
    public String getDeploymentNamespace() {
        return namespaceInstallTo == null ? TestConstants.CO_NAMESPACE : namespaceInstallTo;
    }

    public OlmResource getOlmResource() {
        return olmResource;
    }

    public String getClusterOperatorName() {
        if (Environment.isOlmInstall()) {
            return olmResource.getOlmConfiguration().getOlmOperatorDeploymentName();
        }
        return clusterOperatorName;
    }

    /**
     * Helper method, which returns information whether a user is managing RBAC roles by himself or not.
     *
     * @return  True if any of {@code this.role}, {@code this.clusterRoles}, {@code this.roleBindings}, {@code this.clusterRoleBindings}
     *          is set, otherwise false.
     */
    public boolean isRolesAndBindingsManagedByAnUser() {
        return this.roles != null || this.clusterRoles != null || this.roleBindings != null || this.clusterRoleBindings != null;
    }
}
