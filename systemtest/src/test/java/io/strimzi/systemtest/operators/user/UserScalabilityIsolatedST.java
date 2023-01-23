/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators.user;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.AclOperation;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserAuthorizationSimple;
import io.strimzi.api.kafka.model.KafkaUserAuthorizationSimpleBuilder;
import io.strimzi.api.kafka.model.KafkaUserBuilder;
import io.strimzi.api.kafka.model.KafkaUserSpec;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.enums.UserAuthType;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.ArrayList;
import java.util.List;

import static io.strimzi.systemtest.Constants.SCALABILITY;

@Tag(SCALABILITY)
public class UserScalabilityIsolatedST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(UserScalabilityIsolatedST.class);
    private static String clusterName;
    private static String topicName;

    @IsolatedTest
    @KRaftNotSupported("Scram-sha is not supported by KRaft mode and is used in this test case")
    void testCreateAndAlterBigAmountOfScramShaUsers(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        testCreateAndAlterBigAmountOfUsers(extensionContext, testStorage, UserAuthType.ScramSha);
    }

    @IsolatedTest
    void testCreateAndAlterBigAmountOfTlsUsers(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);
        testCreateAndAlterBigAmountOfUsers(extensionContext, testStorage, UserAuthType.Tls);
    }

    void testCreateAndAlterBigAmountOfUsers(ExtensionContext extensionContext, final TestStorage testStorage, final UserAuthType authType) {
        int numberOfUsers = 1000;

        List<KafkaUser> usersList = getListOfKafkaUsers(testStorage.getUserName(), numberOfUsers, authType);

        createAllUsersInList(extensionContext, usersList, testStorage.getUserName());
        alterAllUsersInList(extensionContext, usersList, testStorage.getUserName());
    }

    private List<KafkaUser> getListOfKafkaUsers(final String userName, final int numberOfUsers, UserAuthType userAuthType) {
        List<KafkaUser> usersList = new ArrayList<>();

        KafkaUserAuthorizationSimple usersAcl = new KafkaUserAuthorizationSimpleBuilder()
            .addNewAcl()
                .withNewAclRuleTopicResource()
                    .withName(topicName)
                .endAclRuleTopicResource()
                .withOperations(AclOperation.WRITE, AclOperation.DESCRIBE)
            .endAcl()
            .build();

        for (int i = 0; i < numberOfUsers; i++) {
            if (userAuthType.equals(UserAuthType.Tls)) {
                usersList.add(
                    KafkaUserTemplates.tlsUser(clusterOperator.getDeploymentNamespace(), clusterName, userName + "-" + i)
                        .editOrNewSpec()
                            .withKafkaUserAuthorizationSimple(usersAcl)
                        .endSpec()
                        .build()
                );
            } else {
                usersList.add(
                    KafkaUserTemplates.scramShaUser(clusterOperator.getDeploymentNamespace(), clusterName, userName + "-" + i)
                        .editOrNewSpec()
                            .withKafkaUserAuthorizationSimple(usersAcl)
                        .endSpec()
                        .build()
                );
            }
        }

        return usersList;
    }

    private void createAllUsersInList(ExtensionContext extensionContext, List<KafkaUser> listOfUsers, String usersPrefix) {
        LOGGER.info("Creating {} KafkaUsers", listOfUsers.size());

        resourceManager.createResource(extensionContext, false, listOfUsers.toArray(new KafkaUser[listOfUsers.size()]));
        KafkaUserUtils.waitForAllUsersWithPrefixReady(clusterOperator.getDeploymentNamespace(), usersPrefix);
    }

    private void alterAllUsersInList(ExtensionContext extensionContext, List<KafkaUser> listOfUsers, String usersPrefix) {
        LOGGER.info("Altering {} KafkaUsers", listOfUsers.size());

        KafkaUserAuthorizationSimple updatedAcl = new KafkaUserAuthorizationSimpleBuilder()
            .addNewAcl()
                .withNewAclRuleTopicResource()
                    .withName(topicName)
                .endAclRuleTopicResource()
                .withOperations(AclOperation.READ, AclOperation.DESCRIBE)
            .endAcl()
            .build();

        listOfUsers.forEach(kafkaUser -> new KafkaUserBuilder(kafkaUser).editOrNewSpec().withKafkaUserAuthorizationSimple(updatedAcl).endSpec().build());

        // get one user spec as the template for wait
        KafkaUserSpec kafkaUserSpec = listOfUsers.stream().findFirst().get().getSpec();

        resourceManager.createResource(extensionContext, false, listOfUsers.toArray(new KafkaUser[listOfUsers.size()]));
        KafkaUserUtils.waitForConfigToBeChangedInAllUsersWithPrefix(clusterOperator.getDeploymentNamespace(), usersPrefix, kafkaUserSpec);
        KafkaUserUtils.waitForAllUsersWithPrefixReady(clusterOperator.getDeploymentNamespace(), usersPrefix);
    }

    @BeforeAll
    void setup(ExtensionContext extensionContext) {
        final TestStorage testStorage = new TestStorage(extensionContext);

        clusterName = testStorage.getClusterName();
        topicName = testStorage.getTopicName();

        clusterOperator.unInstall();
        clusterOperator.defaultInstallation()
            .createInstallation()
            .runInstallation();

        resourceManager.createResource(extensionContext, KafkaTemplates.kafkaEphemeral(clusterName, 3)
            .editOrNewSpec()
                .editOrNewKafka()
                    .withNewKafkaAuthorizationSimple()
                    .endKafkaAuthorizationSimple()
                    .withResources(new ResourceRequirementsBuilder()
                        .addToLimits("memory", new Quantity("2Gi"))
                        .addToRequests("memory", new Quantity("2Gi"))
                        .addToLimits("cpu", new Quantity("1"))
                        .addToRequests("cpu", new Quantity("500m"))
                        .build())
                .endKafka()
                .editEntityOperator()
                    .editUserOperator()
                        .withResources(new ResourceRequirementsBuilder()
                            .addToLimits("memory", new Quantity("512Mi"))
                            .addToRequests("memory", new Quantity("512Mi"))
                            .addToLimits("cpu", new Quantity("0.5"))
                            .addToRequests("cpu", new Quantity("0.2"))
                            .build())
                    .endUserOperator()
                .endEntityOperator()
            .endSpec()
            .build(),
            KafkaTopicTemplates.topic(clusterName, topicName).build()
        );
    }
}
