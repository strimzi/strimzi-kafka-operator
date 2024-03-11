/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators.user;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserAuthorizationSimple;
import io.strimzi.api.kafka.model.user.KafkaUserAuthorizationSimpleBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserQuotas;
import io.strimzi.api.kafka.model.user.KafkaUserQuotasBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserSpec;
import io.strimzi.api.kafka.model.user.acl.AclOperation;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Environment;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.enums.UserAuthType;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import java.util.ArrayList;
import java.util.List;

import static io.strimzi.systemtest.TestConstants.SCALABILITY;

@Tag(SCALABILITY)
public class UserScalabilityST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(UserScalabilityST.class);
    private TestStorage sharedTestStorage;
    
    @IsolatedTest
    void testCreateAndAlterBigAmountOfScramShaUsers() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        testCreateAndAlterBigAmountOfUsers(testStorage, UserAuthType.ScramSha);
    }

    @IsolatedTest
    void testCreateAndAlterBigAmountOfTlsUsers() {
        final TestStorage testStorage = new TestStorage(ResourceManager.getTestContext());
        testCreateAndAlterBigAmountOfUsers(testStorage, UserAuthType.Tls);
    }

    void testCreateAndAlterBigAmountOfUsers(final TestStorage testStorage, final UserAuthType authType) {
        int numberOfUsers = 1000;

        List<KafkaUser> usersList = getListOfKafkaUsers(testStorage.getUsername(), numberOfUsers, authType);

        createAllUsersInList(usersList, testStorage.getUsername());
        alterAllUsersInList(usersList, testStorage.getUsername());
    }

    private List<KafkaUser> getListOfKafkaUsers(final String userName, final int numberOfUsers, UserAuthType userAuthType) {
        List<KafkaUser> usersList = new ArrayList<>();

        KafkaUserAuthorizationSimple usersAcl = new KafkaUserAuthorizationSimpleBuilder()
            .addNewAcl()
                .withNewAclRuleTopicResource()
                    .withName(sharedTestStorage.getTopicName())
                .endAclRuleTopicResource()
                .withOperations(AclOperation.WRITE, AclOperation.DESCRIBE)
            .endAcl()
            .build();

        for (int i = 0; i < numberOfUsers; i++) {
            if (userAuthType.equals(UserAuthType.Tls)) {
                usersList.add(
                    KafkaUserTemplates.tlsUser(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getClusterName(), userName + "-" + i)
                        .editOrNewSpec()
                            .withAuthorization(usersAcl)
                        .endSpec()
                        .build()
                );
            } else {
                usersList.add(
                    KafkaUserTemplates.scramShaUser(Environment.TEST_SUITE_NAMESPACE, sharedTestStorage.getClusterName(), userName + "-" + i)
                        .editOrNewSpec()
                            .withAuthorization(usersAcl)
                        .endSpec()
                        .build()
                );
            }
        }

        return usersList;
    }

    private void createAllUsersInList(List<KafkaUser> listOfUsers, String usersPrefix) {
        LOGGER.info("Creating {} KafkaUsers", listOfUsers.size());

        resourceManager.createResourceWithoutWait(listOfUsers.toArray(new KafkaUser[listOfUsers.size()]));
        KafkaUserUtils.waitForAllUsersWithPrefixReady(Environment.TEST_SUITE_NAMESPACE, usersPrefix);
    }

    private void alterAllUsersInList(List<KafkaUser> listOfUsers, String usersPrefix) {
        LOGGER.info("Altering {} KafkaUsers", listOfUsers.size());

        KafkaUserQuotas kafkaUserQuotas = new KafkaUserQuotasBuilder()
                .withConsumerByteRate(1000)
                .withProducerByteRate(2000)
                .withRequestPercentage(42)
                .withControllerMutationRate(10d)
                .build();

        KafkaUserAuthorizationSimple updatedAcl = new KafkaUserAuthorizationSimpleBuilder()
            .addNewAcl()
                .withNewAclRuleTopicResource()
                    .withName(sharedTestStorage.getTopicName())
                .endAclRuleTopicResource()
                .withOperations(AclOperation.READ, AclOperation.DESCRIBE)
            .endAcl()
            .build();

        listOfUsers.replaceAll(kafkaUser -> new KafkaUserBuilder(kafkaUser)
            .editSpec()
                .withAuthorization(updatedAcl)
                .withQuotas(kafkaUserQuotas)
            .endSpec()
            .build());

        // get one user spec as the template for wait
        KafkaUserSpec kafkaUserSpec = listOfUsers.stream().findFirst().get().getSpec();

        resourceManager.updateResource(listOfUsers.toArray(new KafkaUser[listOfUsers.size()]));
        KafkaUserUtils.waitForConfigToBeChangedInAllUsersWithPrefix(Environment.TEST_SUITE_NAMESPACE, usersPrefix, kafkaUserSpec);
        KafkaUserUtils.waitForAllUsersWithPrefixReady(Environment.TEST_SUITE_NAMESPACE, usersPrefix);
    }

    @BeforeAll
    void setup() {
        sharedTestStorage = new TestStorage(ResourceManager.getTestContext());

        clusterOperator.defaultInstallation()
            .createInstallation()
            .runInstallation();

        resourceManager.createResourceWithWait(
            NodePoolsConverter.convertNodePoolsIfNeeded(
                KafkaNodePoolTemplates.brokerPool(sharedTestStorage.getNamespaceName(), sharedTestStorage.getBrokerPoolName(), sharedTestStorage.getClusterName(), 3).build(),
                KafkaNodePoolTemplates.controllerPool(sharedTestStorage.getNamespaceName(), sharedTestStorage.getControllerPoolName(), sharedTestStorage.getClusterName(), 1).build()
            )
        );

        resourceManager.createResourceWithWait(KafkaTemplates.kafkaEphemeral(sharedTestStorage.getClusterName(), 3)
            .editMetadata()
                .withNamespace(Environment.TEST_SUITE_NAMESPACE)
            .endMetadata()
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
            KafkaTopicTemplates.topic(sharedTestStorage.getClusterName(), sharedTestStorage.getTopicName(), Environment.TEST_SUITE_NAMESPACE).build()
        );
    }
}
