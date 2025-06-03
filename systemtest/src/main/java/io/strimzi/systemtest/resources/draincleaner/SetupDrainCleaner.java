/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.draincleaner;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.admissionregistration.v1.ValidatingWebhookConfiguration;
import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleBuilder;
import io.skodjob.testframe.security.CertAndKey;
import io.skodjob.testframe.security.CertAndKeyFiles;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.security.SystemTestCertGenerator;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.test.ReadWriteUtils;
import io.strimzi.test.TestUtils;
import io.strimzi.test.k8s.KubeClusterResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.x509.GeneralName;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SetupDrainCleaner {

    private static final String OPENSHIFT_DIRECTORY = "openshift";
    private static final String KUBERNETES_DIRECTORY = "kubernetes";
    public static final String DRAIN_CLEANER_DIRECTORY = KubeClusterResource.getInstance().isOpenShiftLikeCluster() ? OPENSHIFT_DIRECTORY : KUBERNETES_DIRECTORY;
    public static final String PATH_TO_DC_CONFIG = TestUtils.USER_PATH + "/../packaging/install/drain-cleaner/" + DRAIN_CLEANER_DIRECTORY;

    private static final Logger LOGGER = LogManager.getLogger(SetupDrainCleaner.class);

    public void applyInstallFiles() {
        LOGGER.info("Applying files from path: {}", PATH_TO_DC_CONFIG);

        List<File> drainCleanerFiles = Arrays.stream(new File(PATH_TO_DC_CONFIG).listFiles()).sorted()
            .filter(File::isFile)
            .toList();

        SecretBuilder customDrainCleanerSecretBuilder = null;

        if (DRAIN_CLEANER_DIRECTORY.equals(KUBERNETES_DIRECTORY)) {
            // we need to create our own certificates before applying install-files
            final CertAndKey drainCleanerKeyPair = SystemTestCertGenerator
                .generateRootCaCertAndKey("C=CZ, L=Prague, O=Strimzi Drain Cleaner, CN=StrimziDrainCleanerCA",
                    // add hostnames (i.e., SANs) to the certificate
                    new ASN1Encodable[]{
                        new GeneralName(GeneralName.dNSName, TestConstants.DRAIN_CLEANER_DEPLOYMENT_NAME),
                        new GeneralName(GeneralName.dNSName, TestConstants.DRAIN_CLEANER_DEPLOYMENT_NAME + "." + TestConstants.DRAIN_CLEANER_DEPLOYMENT_NAME),
                        new GeneralName(GeneralName.dNSName, TestConstants.DRAIN_CLEANER_DEPLOYMENT_NAME + "." + TestConstants.DRAIN_CLEANER_DEPLOYMENT_NAME + ".svc"),
                        new GeneralName(GeneralName.dNSName, TestConstants.DRAIN_CLEANER_DEPLOYMENT_NAME + "." + TestConstants.DRAIN_CLEANER_DEPLOYMENT_NAME + ".svc.cluster.local")
                    });
            final CertAndKeyFiles drainCleanerKeyPairPemFormat = SystemTestCertGenerator.exportToPemFiles(drainCleanerKeyPair);

            final Map<String, String> certsPaths = new HashMap<>();
            certsPaths.put("tls.crt", drainCleanerKeyPairPemFormat.getCertPath());
            certsPaths.put("tls.key", drainCleanerKeyPairPemFormat.getKeyPath());

            customDrainCleanerSecretBuilder = SecretUtils.retrieveSecretBuilderFromFile(TestConstants.DRAIN_CLEANER_NAMESPACE, TestConstants.DRAIN_CLEANER_DEPLOYMENT_NAME, certsPaths,
                Collections.singletonMap(TestConstants.APP_POD_LABEL, TestConstants.DRAIN_CLEANER_DEPLOYMENT_NAME), "kubernetes.io/tls");
        }

        final Secret customDrainCleanerSecret = customDrainCleanerSecretBuilder == null ? null : customDrainCleanerSecretBuilder.build();

        drainCleanerFiles.forEach(file -> {
            if (!file.getName().contains("README") && !file.getName().contains("Namespace") && !file.getName().contains("Deployment")) {
                final String resourceType = file.getName().split("-")[1].split(".yaml")[0];

                switch (resourceType) {
                    case TestConstants.ROLE:
                        Role role = ReadWriteUtils.readObjectFromYamlFilepath(file, Role.class);
                        ResourceManager.getInstance().createResourceWithWait(new RoleBuilder(role)
                            .editMetadata()
                                .withNamespace(TestConstants.DRAIN_CLEANER_NAMESPACE)
                            .endMetadata()
                            .build());
                        break;
                    case TestConstants.ROLE_BINDING:
                        RoleBinding roleBinding = ReadWriteUtils.readObjectFromYamlFilepath(file, RoleBinding.class);
                        ResourceManager.getInstance().createResourceWithWait(new RoleBindingBuilder(roleBinding)
                            .editMetadata()
                                .withNamespace(TestConstants.DRAIN_CLEANER_NAMESPACE)
                            .endMetadata()
                            .editFirstSubject()
                                .withNamespace(TestConstants.DRAIN_CLEANER_NAMESPACE)
                            .endSubject()
                            .build());
                        break;
                    case TestConstants.CLUSTER_ROLE:
                        ClusterRole clusterRole = ReadWriteUtils.readObjectFromYamlFilepath(file, ClusterRole.class);
                        ResourceManager.getInstance().createResourceWithWait(clusterRole);
                        break;
                    case TestConstants.SERVICE_ACCOUNT:
                        ServiceAccount serviceAccount = ReadWriteUtils.readObjectFromYamlFilepath(file, ServiceAccount.class);
                        ResourceManager.getInstance().createResourceWithWait(new ServiceAccountBuilder(serviceAccount)
                            .editMetadata()
                                .withNamespace(TestConstants.DRAIN_CLEANER_NAMESPACE)
                            .endMetadata()
                            .build());
                        break;
                    case TestConstants.CLUSTER_ROLE_BINDING:
                        ClusterRoleBinding clusterRoleBinding = ReadWriteUtils.readObjectFromYamlFilepath(file, ClusterRoleBinding.class);
                        ResourceManager.getInstance().createResourceWithWait(new ClusterRoleBindingBuilder(clusterRoleBinding).build());
                        break;
                    case TestConstants.SECRET:
                        ResourceManager.getInstance().createResourceWithWait(customDrainCleanerSecret);
                        break;
                    case TestConstants.SERVICE:
                        Service service = ReadWriteUtils.readObjectFromYamlFilepath(file, Service.class);
                        ResourceManager.getInstance().createResourceWithWait(service);
                        break;
                    case TestConstants.VALIDATION_WEBHOOK_CONFIG:
                        ValidatingWebhookConfiguration webhookConfiguration = ReadWriteUtils.readObjectFromYamlFilepath(file, ValidatingWebhookConfiguration.class);

                        // in case that we are running on OpenShift-like cluster, we are not creating the Secret, thus this step is not needed
                        if (customDrainCleanerSecret != null) {
                            // we fetch public key from strimzi-drain-cleaner Secret and then patch ValidationWebhookConfiguration.
                            webhookConfiguration.getWebhooks().stream().findFirst().get().getClientConfig().setCaBundle(customDrainCleanerSecret.getData().get("tls.crt"));
                        }

                        ResourceManager.getInstance().createResourceWithWait(webhookConfiguration);
                        break;
                    default:
                        LOGGER.error("Unknown installation resource type: {}", resourceType);
                        throw new RuntimeException("Unknown installation resource type:" + resourceType);
                }
            }
        });
    }

    public void createDrainCleaner() {
        applyInstallFiles();
        ResourceManager.getInstance().createResourceWithWait(new DrainCleanerResource().buildDrainCleanerDeployment().build());
    }
}
