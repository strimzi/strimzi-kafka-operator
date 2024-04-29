/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.performance.utils;

import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserAuthorizationSimple;
import io.strimzi.api.kafka.model.user.KafkaUserAuthorizationSimpleBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserQuotas;
import io.strimzi.api.kafka.model.user.KafkaUserQuotasBuilder;
import io.strimzi.api.kafka.model.user.KafkaUserSpec;
import io.strimzi.api.kafka.model.user.acl.AclOperation;
import io.strimzi.systemtest.enums.UserAuthType;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for managing KafkaUser resources in performance testing scenarios. It provides methods to create,
 * alter, and monitor KafkaUsers efficiently, supporting performance evaluations of the User Operator.
 */
public class UserOperatorPerformanceUtils {

    private static final Logger LOGGER = LogManager.getLogger(UserOperatorPerformanceUtils.class);

    public static void alterAllUsersInList(final TestStorage testStorage, final List<KafkaUser> listOfUsers, final String usersPrefix) {
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
                .withName(testStorage.getTopicName())
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

        ResourceManager.getInstance().updateResource(listOfUsers.toArray(new KafkaUser[listOfUsers.size()]));
        KafkaUserUtils.waitForConfigToBeChangedInAllUsersWithPrefix(testStorage.getNamespaceName(), usersPrefix, kafkaUserSpec);
        KafkaUserUtils.waitForAllUsersWithPrefixReady(testStorage.getNamespaceName(), usersPrefix);
    }

    public static List<KafkaUser> getListOfKafkaUsers(final TestStorage testStorage, final String userName,
                                                      final int numberOfUsers, final UserAuthType userAuthType) {
        return getListOfKafkaUsers(testStorage, userName, 0, numberOfUsers, userAuthType);
    }

    public static List<KafkaUser> getListOfKafkaUsers(final TestStorage testStorage, final String userName,
                                                      final int startPointer, final int endPointer, final UserAuthType userAuthType) {
        List<KafkaUser> usersList = new ArrayList<>();

        KafkaUserAuthorizationSimple usersAcl = new KafkaUserAuthorizationSimpleBuilder()
            .addNewAcl()
                .withNewAclRuleTopicResource()
                    .withName(testStorage.getTopicName())
                .endAclRuleTopicResource()
                .withOperations(AclOperation.WRITE, AclOperation.DESCRIBE)
            .endAcl()
            .build();

        // Loop over the specific range from startPointer to endPointer
        for (int i = startPointer; i < endPointer; i++) {
            if (userAuthType.equals(UserAuthType.Tls)) {
                usersList.add(
                    KafkaUserTemplates.tlsUser(testStorage.getNamespaceName(), testStorage.getClusterName(), userName + "-" + i)
                        .editOrNewSpec()
                            .withAuthorization(usersAcl)
                        .endSpec()
                        .build()
                );
            } else {
                usersList.add(
                    KafkaUserTemplates.scramShaUser(testStorage.getNamespaceName(), testStorage.getClusterName(), userName + "-" + i)
                        .editOrNewSpec()
                            .withAuthorization(usersAcl)
                        .endSpec()
                        .build()
                );
            }
        }

        return usersList;
    }

    public static void createAllUsersInListWithWait(final TestStorage testStorage, final List<KafkaUser> listOfUsers, final String usersPrefix) {
        LOGGER.info("Creating {} KafkaUsers", listOfUsers.size());

        ResourceManager.getInstance().createResourceWithoutWait(listOfUsers.toArray(new KafkaUser[listOfUsers.size()]));
        KafkaUserUtils.waitForAllUsersWithPrefixReady(testStorage.getNamespaceName(), usersPrefix);
    }

}
