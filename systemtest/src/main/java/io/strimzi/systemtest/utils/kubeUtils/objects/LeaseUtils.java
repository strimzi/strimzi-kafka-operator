/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.utils.kubeUtils.objects;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.rbac.ClusterRole;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBuilder;
import io.fabric8.kubernetes.api.model.rbac.Role;
import io.fabric8.kubernetes.api.model.rbac.RoleBinding;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleBuilder;
import io.strimzi.systemtest.TestConstants;
import io.strimzi.test.ReadWriteUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

/**
 * Class containing methods for operations around Leases
 */
public class LeaseUtils {
    /**
     * It takes the path to the resource (specified in {@param yamlPath} and changes the file to correspond to particular
     * Lease name specified in `STRIMZI_LEADER_ELECTION_LEASE_NAME` (one of the environment variables in {@param envVars}.
     * Based on the resource type it changes the Lease name in specific fields.
     *
     * @param yamlPath  path to resource YAML
     * @param envVars   list of environment variables containing `STRIMZI_LEADER_ELECTION_LEASE_NAME` env variable
     *                  If there is none, the whole check is skipped
     * @return  path to the updated resource's YAML file
     */
    public static String changeLeaseNameInResourceIfNeeded(String yamlPath, List<EnvVar> envVars) {
        final EnvVar leaseEnvVar = envVars == null ? null : envVars.stream().filter(envVar -> envVar.getName().equals("STRIMZI_LEADER_ELECTION_LEASE_NAME")).findFirst().orElse(null);
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
                        RoleBuilder roleBuilder = new RoleBuilder(ReadWriteUtils.readObjectFromYamlFilepath(yamlPath, Role.class))
                            .editMetadata()
                                .withName(resourceName)
                            .endMetadata()
                            .editMatchingRule(rule -> rule.getFirstResourceName().equals(TestConstants.STRIMZI_DEPLOYMENT_NAME))
                                .withResourceNames(leaseEnvVar.getValue())
                            .endRule();

                        tmpFileContent = ReadWriteUtils.writeObjectToYamlString(roleBuilder.build());
                        break;
                    case TestConstants.CLUSTER_ROLE:
                        ClusterRoleBuilder clusterRoleBuilder = new ClusterRoleBuilder(ReadWriteUtils.readObjectFromYamlFilepath(yamlPath, ClusterRole.class))
                            .editMetadata()
                                .withName(resourceName)
                            .endMetadata()
                            .editMatchingRule(rule -> rule.getResourceNames().stream().findAny().orElse("").equals(
                                TestConstants.STRIMZI_DEPLOYMENT_NAME))
                                .withResourceNames(leaseEnvVar.getValue())
                            .endRule();

                        tmpFileContent = ReadWriteUtils.writeObjectToYamlString(clusterRoleBuilder.build());
                        break;
                    case TestConstants.ROLE_BINDING:
                        RoleBindingBuilder roleBindingBuilder = new RoleBindingBuilder(ReadWriteUtils.readObjectFromYamlFilepath(yamlPath, RoleBinding.class))
                            .editMetadata()
                                .withName(resourceName)
                            .endMetadata()
                            .editRoleRef()
                                .withName(resourceName)
                            .endRoleRef();

                        tmpFileContent = ReadWriteUtils.writeObjectToYamlString(roleBindingBuilder.build());
                        break;
                    default:
                        return yamlPath;
                }

                ReadWriteUtils.writeFile(tmpFile.toPath(), tmpFileContent);
                return tmpFile.getAbsolutePath();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            return yamlPath;
        }
    }
}
