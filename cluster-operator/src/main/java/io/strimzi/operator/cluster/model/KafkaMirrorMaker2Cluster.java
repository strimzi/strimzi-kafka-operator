/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Spec;
import io.strimzi.operator.common.model.Labels;

public class KafkaMirrorMaker2Cluster extends KafkaConnectCluster {

    /**
     * Constructor
     *
     * @param namespace Kubernetes/OpenShift namespace where Kafka Connect cluster
     *                  resources are going to be created
     * @param cluster   overall cluster name
     */
    private KafkaMirrorMaker2Cluster(String namespace, String cluster, Labels labels) {
        super(namespace, cluster, labels);
        this.name = KafkaMirrorMaker2Resources.deploymentName(cluster);
        this.serviceName = KafkaMirrorMaker2Resources.serviceName(cluster);
        this.ancillaryConfigName = KafkaMirrorMaker2Resources.metricsAndLogConfigMapName(cluster);
    }

    public static KafkaMirrorMaker2Cluster fromCrd(KafkaMirrorMaker2 kafkaMirrorMaker2, KafkaVersion.Lookup versions) {
        KafkaMirrorMaker2Cluster cluster = new KafkaMirrorMaker2Cluster(kafkaMirrorMaker2.getMetadata().getNamespace(),
                kafkaMirrorMaker2.getMetadata().getName(),
                Labels.fromResource(kafkaMirrorMaker2).withKind(kafkaMirrorMaker2.getKind()));
        KafkaMirrorMaker2Spec spec = kafkaMirrorMaker2.getSpec();
        cluster.setOwnerReference(kafkaMirrorMaker2);
        cluster.setImage(versions.kafkaMirrorMaker2Version(spec.getImage(), spec.getVersion()));
        return fromSpec(spec, versions, cluster);
    }

    @Override
    protected String getDefaultLogConfigFileName() {
        return "kafkaMirrorMaker2DefaultLoggingProperties";
    }

    @Override
    protected String getServiceAccountName() {
        return KafkaMirrorMaker2Resources.serviceAccountName(cluster);
    }
}
