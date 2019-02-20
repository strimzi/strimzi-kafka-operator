/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest;

import io.fabric8.kubernetes.api.model.Doneable;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentList;
import io.fabric8.kubernetes.api.model.apps.DoneableDeployment;
import io.fabric8.kubernetes.api.model.rbac.DoneableKubernetesClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.DoneableKubernetesRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.KubernetesClusterRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.KubernetesClusterRoleBindingList;
import io.fabric8.kubernetes.api.model.rbac.KubernetesRoleBinding;
import io.fabric8.kubernetes.api.model.rbac.KubernetesRoleBindingList;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.internal.CustomResourceOperationsImpl;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.KafkaAssemblyList;
import io.strimzi.api.kafka.KafkaConnectAssemblyList;
import io.strimzi.api.kafka.KafkaConnectS2IAssemblyList;
import io.strimzi.api.kafka.KafkaMirrorMakerList;
import io.strimzi.api.kafka.KafkaTopicList;
import io.strimzi.api.kafka.KafkaUserList;
import io.strimzi.api.kafka.model.DoneableKafka;
import io.strimzi.api.kafka.model.DoneableKafkaConnect;
import io.strimzi.api.kafka.model.DoneableKafkaConnectS2I;
import io.strimzi.api.kafka.model.DoneableKafkaMirrorMaker;
import io.strimzi.api.kafka.model.DoneableKafkaTopic;
import io.strimzi.api.kafka.model.DoneableKafkaUser;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaConnect;
import io.strimzi.api.kafka.model.KafkaConnectS2I;
import io.strimzi.api.kafka.model.KafkaMirrorMaker;
import io.strimzi.api.kafka.model.KafkaTopic;
import io.strimzi.api.kafka.model.KafkaUser;

abstract class AbstractResources {

    final NamespacedKubernetesClient client;

    AbstractResources(NamespacedKubernetesClient client) {
        this.client = client;
    }

    NamespacedKubernetesClient client() {
        return client;
    }

    MixedOperation<Kafka, KafkaAssemblyList, DoneableKafka, Resource<Kafka, DoneableKafka>> kafka() {
        return customResourcesWithCascading(Kafka.class, KafkaAssemblyList.class, DoneableKafka.class);
    }

    // This logic is necessary only for the deletion of resources with `cascading: true`
    <T extends HasMetadata, L extends KubernetesResourceList, D extends Doneable<T>> MixedOperation<T, L, D, Resource<T, D>> customResourcesWithCascading(Class<T> resourceType, Class<L> listClass, Class<D> doneClass) {
        return new CustomResourceOperationsImpl<T, L, D>(((DefaultKubernetesClient) client()).getHttpClient(), client().getConfiguration(), Crds.kafka().getSpec().getGroup(), Crds.kafka().getSpec().getVersion(), "kafkas", true, client().getNamespace(), null, true, null, null, false, resourceType, listClass, doneClass);
    }

    MixedOperation<KafkaConnect, KafkaConnectAssemblyList, DoneableKafkaConnect, Resource<KafkaConnect, DoneableKafkaConnect>> kafkaConnect() {
        return client()
                .customResources(Crds.kafkaConnect(),
                        KafkaConnect.class, KafkaConnectAssemblyList.class, DoneableKafkaConnect.class);
    }

    MixedOperation<KafkaConnectS2I, KafkaConnectS2IAssemblyList, DoneableKafkaConnectS2I, Resource<KafkaConnectS2I, DoneableKafkaConnectS2I>> kafkaConnectS2I() {
        return client()
                .customResources(Crds.kafkaConnectS2I(),
                        KafkaConnectS2I.class, KafkaConnectS2IAssemblyList.class, DoneableKafkaConnectS2I.class);
    }

    MixedOperation<KafkaMirrorMaker, KafkaMirrorMakerList, DoneableKafkaMirrorMaker, Resource<KafkaMirrorMaker, DoneableKafkaMirrorMaker>> kafkaMirrorMaker() {
        return client()
                .customResources(Crds.mirrorMaker(),
                        KafkaMirrorMaker.class, KafkaMirrorMakerList.class, DoneableKafkaMirrorMaker.class);
    }

    MixedOperation<KafkaTopic, KafkaTopicList, DoneableKafkaTopic, Resource<KafkaTopic, DoneableKafkaTopic>> kafkaTopic() {
        return client()
                .customResources(Crds.topic(),
                        KafkaTopic.class, KafkaTopicList.class, DoneableKafkaTopic.class);
    }

    MixedOperation<KafkaUser, KafkaUserList, DoneableKafkaUser, Resource<KafkaUser, DoneableKafkaUser>> kafkaUser() {
        return client()
                .customResources(Crds.kafkaUser(),
                        KafkaUser.class, KafkaUserList.class, DoneableKafkaUser.class);
    }

    MixedOperation<Deployment, DeploymentList, DoneableDeployment, Resource<Deployment, DoneableDeployment>> clusterOperator() {
        return customResourcesWithCascading(Deployment.class, DeploymentList.class, DoneableDeployment.class);
    }

    MixedOperation<KubernetesClusterRoleBinding, KubernetesClusterRoleBindingList, DoneableKubernetesClusterRoleBinding, Resource<KubernetesClusterRoleBinding, DoneableKubernetesClusterRoleBinding>> kubernetesClusterRoleBinding() {
        return customResourcesWithCascading(KubernetesClusterRoleBinding.class, KubernetesClusterRoleBindingList.class, DoneableKubernetesClusterRoleBinding.class);
    }

    MixedOperation<KubernetesRoleBinding, KubernetesRoleBindingList, DoneableKubernetesRoleBinding, Resource<KubernetesRoleBinding, DoneableKubernetesRoleBinding>> kubernetesRoleBinding() {
        return customResourcesWithCascading(KubernetesRoleBinding.class, KubernetesRoleBindingList.class, DoneableKubernetesRoleBinding.class);
    }

}
