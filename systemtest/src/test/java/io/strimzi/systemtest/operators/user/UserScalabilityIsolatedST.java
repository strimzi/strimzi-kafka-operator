package io.strimzi.systemtest.operators.user;

import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserBuilder;
import io.strimzi.api.kafka.model.KafkaUserQuotas;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.systemtest.AbstractST;
import io.strimzi.systemtest.Constants;
import io.strimzi.systemtest.annotations.IsolatedTest;
import io.strimzi.systemtest.annotations.KRaftNotSupported;
import io.strimzi.systemtest.enums.UserAuthType;
import io.strimzi.systemtest.resources.ResourceManager;
import io.strimzi.systemtest.resources.crd.KafkaUserResource;
import io.strimzi.systemtest.storage.TestStorage;
import io.strimzi.systemtest.templates.crd.KafkaUserTemplates;
import io.strimzi.systemtest.utils.kafkaUtils.KafkaUserUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jose4j.jwk.Use;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.strimzi.systemtest.Constants.SCALABILITY;
import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;
import static io.strimzi.test.k8s.KubeClusterResource.cmdKubeClient;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@Tag(SCALABILITY)
public class UserScalabilityIsolatedST extends AbstractST {
    private static final Logger LOGGER = LogManager.getLogger(UserScalabilityIsolatedST.class);

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
        int numberOfUsers = 100;
        int producerRate = 1000;
        int consumerRate = 1500;
        int requestsPercentage = 42;
        double mutationRate = 3.0;

        List<KafkaUser> usersList = getListOfKafkaUsers(testStorage.getUserName(), numberOfUsers, authType);

        createAllUsersInList(extensionContext, usersList, testStorage.getUserName());
        alterBigAmountOfUsers(extensionContext, userName, "TLS", numberOfUsers,
                producerRate, consumerRate, requestsPercentage, mutationRate);
    }

    private List<KafkaUser> getListOfKafkaUsers(final String userName, final int numberOfUsers, UserAuthType userAuthType) {
        List<KafkaUser> usersList = new ArrayList<>();

        for (int i = 0; i < numberOfUsers; i++) {
            if (userAuthType.equals(UserAuthType.Tls)) {
                usersList.add(KafkaUserTemplates.tlsUser(Constants.INFRA_NAMESPACE, "kafkacluset", userName + "-" + i).build());
            } else {
                usersList.add(KafkaUserTemplates.scramShaUser(Constants.INFRA_NAMESPACE, "kafkacluset", userName + "-" + i).build());
            }
        }

        return usersList;
    }

    private void createAllUsersInList(ExtensionContext extensionContext, List<KafkaUser> listOfUsers, String usersPrefix) {
        LOGGER.info("Creating {} KafkaUsers", listOfUsers.size());

        resourceManager.createResource(extensionContext, false, listOfUsers.toArray(new KafkaUser[listOfUsers.size()]));
        KafkaUserUtils.waitForAllUsersWithPrefixReady(Constants.INFRA_NAMESPACE, usersPrefix);
    }

    private void alterAllUsersInList(ExtensionContext extensionContext, List<KafkaUser> listOfUsers) {

    }

    synchronized void alterBigAmountOfUsers(ExtensionContext extensionContext, String userName, String typeOfUser, int numberOfUsers,
                                            int producerRate, int consumerRate, int requestsPercentage, double mutationRate) {
        KafkaUserQuotas kuq = new KafkaUserQuotas();
        kuq.setConsumerByteRate(consumerRate);
        kuq.setProducerByteRate(producerRate);
        kuq.setRequestPercentage(requestsPercentage);
        kuq.setControllerMutationRate(mutationRate);

        LOGGER.info("Updating of existing KafkaUsers");
        for (int i = 0; i < numberOfUsers; i++) {
            String userNameWithSuffix = userName + "-" + i;
            if (typeOfUser.equals("TLS")) {
                resourceManager.createResource(extensionContext, KafkaUserTemplates.tlsUser(clusterOperator.getDeploymentNamespace(), userClusterName, userNameWithSuffix)
                        .editMetadata()
                        .withNamespace(namespace)
                        .endMetadata()
                        .editSpec()
                        .withQuotas(kuq)
                        .endSpec()
                        .build());
            } else {
                resourceManager.createResource(extensionContext, KafkaUserTemplates.scramShaUser(namespace, userClusterName, userNameWithSuffix)
                        .editMetadata()
                        .withNamespace(namespace)
                        .endMetadata()
                        .editSpec()
                        .withQuotas(kuq)
                        .endSpec()
                        .build());
            }

            LOGGER.info("[After update] Checking status of KafkaUser {}", userNameWithSuffix);
            Condition kafkaCondition = KafkaUserResource.kafkaUserClient().inNamespace(namespace).withName(userNameWithSuffix).get()
                    .getStatus().getConditions().get(0);
            LOGGER.debug("KafkaUser condition status: {}", kafkaCondition.getStatus());
            LOGGER.debug("KafkaUser condition type: {}", kafkaCondition.getType());
            assertThat(kafkaCondition.getType(), is(Ready.toString()));
            LOGGER.debug("KafkaUser {} is in desired state: {}", userNameWithSuffix, kafkaCondition.getType());

            KafkaUserQuotas kuqAfter = KafkaUserResource.kafkaUserClient().inNamespace(namespace).withName(userNameWithSuffix).get().getSpec().getQuotas();
            LOGGER.debug("Check altered KafkaUser {} new quotas.", userNameWithSuffix);
            assertThat(kuqAfter.getRequestPercentage(), is(requestsPercentage));
            assertThat(kuqAfter.getConsumerByteRate(), is(consumerRate));
            assertThat(kuqAfter.getProducerByteRate(), is(producerRate));
            assertThat(kuqAfter.getControllerMutationRate(), is(mutationRate));
        }
    }
}
