/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.resources.crd;

import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaUserList;
import io.strimzi.api.kafka.model.DoneableKafkaUser;
import io.strimzi.api.kafka.model.KafkaUser;
import io.strimzi.api.kafka.model.KafkaUserBuilder;
import io.strimzi.operator.common.model.Labels;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.strimzi.systemtest.resources.ResourceManager;

import java.util.function.Consumer;

import static io.strimzi.systemtest.enums.CustomResourceStatus.Ready;

public class KafkaUserResource {
    private static final Logger LOGGER = LogManager.getLogger(KafkaUserResource.class);

    public static MixedOperation<KafkaUser, KafkaUserList, DoneableKafkaUser, Resource<KafkaUser, DoneableKafkaUser>> kafkaUserClient() {
        return Crds.kafkaUserOperation(ResourceManager.kubeClient().getClient());
    }

    public static DoneableKafkaUser tlsUser(String clusterName, String name) {
        return user(defaultUser(clusterName, name)
            .withNewSpec()
                .withNewKafkaUserTlsClientAuthentication()
                .endKafkaUserTlsClientAuthentication()
            .endSpec()
            .build());
    }

    public static DoneableKafkaUser scramShaUser(String clusterName, String name) {
        return user(defaultUser(clusterName, name)
            .withNewSpec()
                .withNewKafkaUserScramSha512ClientAuthentication()
                .endKafkaUserScramSha512ClientAuthentication()
            .endSpec()
            .build());
    }

    public static KafkaUserBuilder defaultUser(String clusterName, String name) {
        return new KafkaUserBuilder()
            .withNewMetadata()
                .withClusterName(clusterName)
                .withName(name)
                .withNamespace(ResourceManager.kubeClient().getNamespace())
                .addToLabels(Labels.STRIMZI_CLUSTER_LABEL, clusterName)
            .endMetadata();
    }

    static DoneableKafkaUser user(KafkaUser user) {
        return new DoneableKafkaUser(user, ku -> {
            kafkaUserClient().inNamespace(ResourceManager.kubeClient().getNamespace()).createOrReplace(ku);
            LOGGER.info("Created KafkaUser {}", ku.getMetadata().getName());
            return waitFor(deleteLater(ku));
        });
    }

    public static KafkaUser kafkaUserWithoutWait(KafkaUser user) {
        kafkaUserClient().inNamespace(ResourceManager.kubeClient().getNamespace()).createOrReplace(user);
        return user;
    }

    private static KafkaUser waitFor(KafkaUser kafkaUser) {
        return ResourceManager.waitForResourceStatus(kafkaUserClient(), kafkaUser, Ready);
    }

    private static KafkaUser deleteLater(KafkaUser kafkaUser) {
        return ResourceManager.deleteLater(kafkaUserClient(), kafkaUser);
    }

    public static DoneableKafkaUser userWithQuota(KafkaUser user, Integer prodRate, Integer consRate, Integer requestPerc) {
        return user(new KafkaUserBuilder(user)
                .editSpec()
                    .withNewQuotas()
                        .withConsumerByteRate(consRate)
                        .withProducerByteRate(prodRate)
                        .withRequestPercentage(requestPerc)
                    .endQuotas()
                .endSpec()
                .build());
    }

    public static void replaceUserResource(String resourceName, Consumer<KafkaUser> editor) {
        ResourceManager.replaceCrdResource(KafkaUser.class, KafkaUserList.class, DoneableKafkaUser.class, resourceName, editor);
    }
}
