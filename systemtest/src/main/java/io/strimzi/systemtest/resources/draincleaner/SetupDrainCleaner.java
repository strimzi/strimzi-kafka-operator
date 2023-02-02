/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.draincleaner;

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
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.security.CertAndKeyFiles;
import io.strimzi.systemtest.security.SystemTestCertAndKey;
import io.strimzi.systemtest.security.SystemTestCertManager;
import io.strimzi.systemtest.utils.kubeUtils.objects.SecretUtils;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.asn1.ASN1Encodable;
import org.bouncycastle.asn1.x509.GeneralName;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SetupDrainCleaner {

    public static final String PATH_TO_DC_CONFIG = TestUtils.USER_PATH + "/../packaging/install/drain-cleaner/kubernetes";
    private static final Logger LOGGER = LogManager.getLogger(SetupDrainCleaner.class);

    public void applyInstallFiles(ExtensionContext extensionContext) {
        List<File> drainCleanerFiles = Arrays.stream(new File(PATH_TO_DC_CONFIG).listFiles()).sorted()
            .filter(File::isFile)
            .collect(Collectors.toList());

        // we need to create our own certificates before applying install-files
        final SystemTestCertAndKey drainCleanerKeyPair = SystemTestCertManager
            .generateRootCaCertAndKey("C=CZ, L=Prague, O=Strimzi Drain Cleaner, CN=StrimziDrainCleanerCA",
                // add hostnames (i.e., SANs) to the certificate
                new ASN1Encodable[] {
                    new GeneralName(GeneralName.dNSName, Constants.DRAIN_CLEANER_DEPLOYMENT_NAME),
                    new GeneralName(GeneralName.dNSName, Constants.DRAIN_CLEANER_DEPLOYMENT_NAME + "." + Constants.DRAIN_CLEANER_DEPLOYMENT_NAME),
                    new GeneralName(GeneralName.dNSName, Constants.DRAIN_CLEANER_DEPLOYMENT_NAME + "." + Constants.DRAIN_CLEANER_DEPLOYMENT_NAME + ".svc"),
                    new GeneralName(GeneralName.dNSName, Constants.DRAIN_CLEANER_DEPLOYMENT_NAME + "." + Constants.DRAIN_CLEANER_DEPLOYMENT_NAME + ".svc.cluster.local")
                });
        final CertAndKeyFiles drainCleanerKeyPairPemFormat = SystemTestCertManager.exportToPemFiles(drainCleanerKeyPair);

        final Map<String, String> certsPaths = new HashMap<>();
        certsPaths.put("tls.crt", drainCleanerKeyPairPemFormat.getCertPath());
        certsPaths.put("tls.key", drainCleanerKeyPairPemFormat.getKeyPath());

        final SecretBuilder customDrainCleanerSecretBuilder = SecretUtils.retrieveSecretBuilderFromFile(certsPaths,
            Constants.DRAIN_CLEANER_DEPLOYMENT_NAME, Constants.DRAIN_CLEANER_NAMESPACE,
            Collections.singletonMap("app", Constants.DRAIN_CLEANER_DEPLOYMENT_NAME), "kubernetes.io/tls");

        drainCleanerFiles.forEach(file -> {
            if (!file.getName().contains("README") && !file.getName().contains("Namespace") && !file.getName().contains("Deployment")) {
                final String resourceType = file.getName().split("-")[1].split(".yaml")[0];

                switch (resourceType) {
                    case Constants.ROLE:
                        Role role = TestUtils.configFromYaml(file, Role.class);
                        ResourceManager.getInstance().createResource(extensionContext, new RoleBuilder(role)
                            .editMetadata()
                                .withNamespace(Constants.DRAIN_CLEANER_NAMESPACE)
                            .endMetadata()
                            .build());
                        break;
                    case Constants.ROLE_BINDING:
                        RoleBinding roleBinding = TestUtils.configFromYaml(file, RoleBinding.class);
                        ResourceManager.getInstance().createResource(extensionContext, new RoleBindingBuilder(roleBinding)
                            .editMetadata()
                                .withNamespace(Constants.DRAIN_CLEANER_NAMESPACE)
                            .endMetadata()
                            .editFirstSubject()
                                .withNamespace(Constants.DRAIN_CLEANER_NAMESPACE)
                            .endSubject()
                            .build());
                        break;
                    case Constants.CLUSTER_ROLE:
                        ClusterRole clusterRole = TestUtils.configFromYaml(file, ClusterRole.class);
                        ResourceManager.getInstance().createResource(extensionContext, clusterRole);
                        break;
                    case Constants.SERVICE_ACCOUNT:
                        ServiceAccount serviceAccount = TestUtils.configFromYaml(file, ServiceAccount.class);
                        ResourceManager.getInstance().createResource(extensionContext, new ServiceAccountBuilder(serviceAccount)
                            .editMetadata()
                                .withNamespace(Constants.DRAIN_CLEANER_NAMESPACE)
                            .endMetadata()
                            .build());
                        break;
                    case Constants.CLUSTER_ROLE_BINDING:
                        ClusterRoleBinding clusterRoleBinding = TestUtils.configFromYaml(file, ClusterRoleBinding.class);
                        ResourceManager.getInstance().createResource(extensionContext, new ClusterRoleBindingBuilder(clusterRoleBinding).build());
                        break;
                    case Constants.SECRET:
                        ResourceManager.getInstance().createResource(extensionContext, customDrainCleanerSecretBuilder.build());
                        break;
                    case Constants.SERVICE:
                        Service service = TestUtils.configFromYaml(file, Service.class);
                        ResourceManager.getInstance().createResource(extensionContext, service);
                        break;
                    case Constants.VALIDATION_WEBHOOK_CONFIG:
                        ValidatingWebhookConfiguration webhookConfiguration = TestUtils.configFromYaml(file, ValidatingWebhookConfiguration.class);

                        // we fetch public key from strimzi-drain-cleaner Secret and then patch ValidationWebhookConfiguration.
                        webhookConfiguration.getWebhooks().stream().findFirst().get().getClientConfig().setCaBundle(customDrainCleanerSecretBuilder.getData().get("tls.crt"));

                        ResourceManager.getInstance().createResource(extensionContext, webhookConfiguration);
                        break;
                    default:
                        LOGGER.error("Unknown installation resource type: {}", resourceType);
                        throw new RuntimeException("Unknown installation resource type:" + resourceType);
                }
            }
        });
    }

    public void createDrainCleaner(ExtensionContext extensionContext) {
        applyInstallFiles(extensionContext);
        ResourceManager.getInstance().createResource(extensionContext, new DrainCleanerResource().buildDrainCleanerDeployment().build());
    }
}
