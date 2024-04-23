/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.operators.user;

import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.enums.UserAuthType;
import io.strimzi.systemtest.performance.utils.UserOperatorPerformanceUtils;
import io.strimzi.systemtest.resources.NodePoolsConverter;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaNodePoolTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTemplates;
import io.strimzi.systemtest.templates.crd.KafkaTopicTemplates;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

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

        List<KafkaUser> usersList = UserOperatorPerformanceUtils.getListOfKafkaUsers(sharedTestStorage, testStorage.getUsername(), numberOfUsers, authType);

        UserOperatorPerformanceUtils.createAllUsersInListWithWait(sharedTestStorage, usersList, testStorage.getUsername());
        UserOperatorPerformanceUtils.alterAllUsersInList(sharedTestStorage, usersList, testStorage.getUsername());
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
                .withNamespace(sharedTestStorage.getNamespaceName())
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
            KafkaTopicTemplates.topic(sharedTestStorage.getClusterName(), sharedTestStorage.getTopicName(), sharedTestStorage.getNamespaceName()).build()
        );
    }
}
