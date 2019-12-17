/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.KafkaConnectTls;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2ClusterSpec;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Resources;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Spec;
import io.strimzi.api.kafka.model.KafkaMirrorMaker2Tls;
import io.strimzi.operator.common.model.Labels;

public class KafkaMirrorMaker2Cluster extends KafkaConnectCluster {

    private List<KafkaMirrorMaker2ClusterSpec> clusters;

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
        cluster.setConfiguration(new KafkaMirrorMaker2Configuration(spec.getConfig().entrySet()));

        List<KafkaMirrorMaker2ClusterSpec> clustersList = Optional.ofNullable(spec.getClusters())
                .orElse(Collections.emptyList());
        cluster.setClusters(clustersList);

        String connectClusterAlias = spec.getConnectCluster();
        if (connectClusterAlias != null) {
            KafkaMirrorMaker2ClusterSpec connectCluster = clustersList.stream()
                    .filter(clustersListItem -> connectClusterAlias.equals(clustersListItem.getAlias()))
                    .findFirst()
                    .orElseThrow(() -> new InvalidResourceException("connectCluster with alias " + connectClusterAlias + " cannot be found in the list of clusters at spec.clusters"));     

            spec.setBootstrapServers(connectCluster.getBootstrapServers());
            spec.setAuthentication(connectCluster.getAuthentication());

            KafkaMirrorMaker2Tls mirrorMaker2ConnectClusterTls = connectCluster.getTls();
            if (mirrorMaker2ConnectClusterTls != null) {
                KafkaConnectTls connectTls = new KafkaConnectTls();
                connectTls.setTrustedCertificates(mirrorMaker2ConnectClusterTls.getTrustedCertificates());
                mirrorMaker2ConnectClusterTls.getAdditionalProperties().entrySet().stream()
                        .forEach(entry -> connectTls.setAdditionalProperty(entry.getKey(), entry.getValue()));
                spec.setTls(connectTls);
            }
        }

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

    @Override
    protected List<Volume> getVolumes(boolean isOpenShift) {
        List<Volume> volumeList = super.getVolumes(isOpenShift);

        clusters.stream().forEach(mirrorMaker2Cluster -> {
            KafkaMirrorMaker2Tls tls = mirrorMaker2Cluster.getTls();

            if (tls != null) {
                List<CertSecretSource> trustedCertificates = tls.getTrustedCertificates();
    
                if (trustedCertificates != null && trustedCertificates.size() > 0) {
                    for (CertSecretSource certSecretSource : trustedCertificates) {
                        // skipping if a volume with same Secret name was already added
                        if (!volumeList.stream().anyMatch(v -> v.getName().equals(certSecretSource.getSecretName()))) {
                            volumeList.add(createSecretVolume(certSecretSource.getSecretName(), certSecretSource.getSecretName(), isOpenShift));
                        }
                    }
                }
            }
    
            AuthenticationUtils.configureClientAuthenticationVolumes(mirrorMaker2Cluster.getAuthentication(), volumeList, isOpenShift);
        });
        return volumeList;
    }

    @Override
    protected List<VolumeMount> getVolumeMounts() {
        List<VolumeMount> volumeMountList = super.getVolumeMounts();

        clusters.stream().forEach(mirrorMaker2Cluster -> {
            KafkaMirrorMaker2Tls tls = mirrorMaker2Cluster.getTls();

            if (tls != null) {
                List<CertSecretSource> trustedCertificates = tls.getTrustedCertificates();
    
                if (trustedCertificates != null && trustedCertificates.size() > 0) {
                    for (CertSecretSource certSecretSource : trustedCertificates) {
                        // skipping if a volume mount with same Secret name was already added
                        if (!volumeMountList.stream().anyMatch(vm -> vm.getName().equals(certSecretSource.getSecretName()))) {
                            volumeMountList.add(createVolumeMount(certSecretSource.getSecretName(),
                                    TLS_CERTS_BASE_VOLUME_MOUNT + certSecretSource.getSecretName()));
                        }
                    }
                }
            }
    
            AuthenticationUtils.configureClientAuthenticationVolumeMounts(mirrorMaker2Cluster.getAuthentication(), volumeMountList, TLS_CERTS_BASE_VOLUME_MOUNT, PASSWORD_VOLUME_MOUNT, OAUTH_TLS_CERTS_BASE_VOLUME_MOUNT);
        });
        return volumeMountList;
    }

    /**
     * Sets the configured clusters for mirroring
     *
     * @param clusters The list of cluster configurations
     */
    protected void setClusters(List<KafkaMirrorMaker2ClusterSpec> clusters) {
        this.clusters = clusters;
    }
}
