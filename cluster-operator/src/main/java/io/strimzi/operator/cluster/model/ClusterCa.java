/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Secret;
import io.strimzi.api.kafka.model.CertificateExpirationPolicy;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.certs.CertAndKey;
import io.strimzi.certs.CertManager;
import io.strimzi.certs.Subject;
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.operator.common.PasswordGenerator;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;

public class ClusterCa extends Ca {

    private final String clusterName;
    private Secret entityOperatorSecret;
    private Secret clusterOperatorSecret;
    private Secret kafkaExporterSecret;
    private Secret cruiseControlSecret;

    private Secret brokersSecret;
    private Secret zkNodesSecret;

    private final Pattern ipv4Address = Pattern.compile("[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}");

    public ClusterCa(CertManager certManager, PasswordGenerator passwordGenerator, String clusterName, Secret caCertSecret, Secret caKeySecret) {
        this(certManager, passwordGenerator, clusterName, caCertSecret, caKeySecret, 365, 30, true, null);
    }

    public ClusterCa(CertManager certManager,
                     PasswordGenerator passwordGenerator,
                     String clusterName,
                     Secret clusterCaCert,
                     Secret clusterCaKey,
                     int validityDays,
                     int renewalDays,
                     boolean generateCa,
                     CertificateExpirationPolicy policy) {
        super(certManager, passwordGenerator, "cluster-ca",
                AbstractModel.clusterCaCertSecretName(clusterName),
                forceRenewal(clusterCaCert, clusterCaKey, "cluster-ca.key"),
                AbstractModel.clusterCaKeySecretName(clusterName),
                adapt060ClusterCaSecret(clusterCaKey),
                validityDays, renewalDays, generateCa, policy);
        this.clusterName = clusterName;
    }

    /**
     * In Strimzi 0.6.0 the Secrets and keys used a different convention.
     * Here we adapt the keys in the {@code *-cluster-ca} Secret to match what
     * 0.7.0 expects.
     * @param clusterCaKey The cluster CA key Secret
     * @return The same Secret.
     */
    public static Secret adapt060ClusterCaSecret(Secret clusterCaKey) {
        if (clusterCaKey != null && clusterCaKey.getData() != null) {
            String key = clusterCaKey.getData().get("cluster-ca.key");
            if (key != null) {
                clusterCaKey.getData().put("ca.key", key);
            }
        }
        return clusterCaKey;
    }


    @Override
    public String toString() {
        return "cluster-ca";
    }

    @SuppressWarnings("deprecation")
    public void initCaSecrets(List<Secret> secrets) {
        for (Secret secret: secrets) {
            String name = secret.getMetadata().getName();
            if (KafkaCluster.brokersSecretName(clusterName).equals(name)) {
                brokersSecret = secret;
            } else if (EntityOperator.secretName(clusterName).equals(name)) {
                entityOperatorSecret = secret;
            } else if (ZookeeperCluster.nodesSecretName(clusterName).equals(name)) {
                zkNodesSecret = secret;
            } else if (ClusterOperator.secretName(clusterName).equals(name)) {
                clusterOperatorSecret = secret;
            } else if (KafkaExporter.secretName(clusterName).equals(name)) {
                kafkaExporterSecret = secret;
            } else if (CruiseControl.secretName(clusterName).equals(name)) {
                cruiseControlSecret = secret;
            }
        }
    }

    public Secret entityOperatorSecret() {
        return entityOperatorSecret;
    }

    public Secret clusterOperatorSecret() {
        return clusterOperatorSecret;
    }

    public Secret kafkaExporterSecret() {
        return kafkaExporterSecret;
    }

    public Secret cruiseControlSecret() {
        return cruiseControlSecret;
    }

    public Map<String, CertAndKey> generateZkCerts(Kafka kafka, boolean isMaintenanceTimeWindowsSatisfied) throws IOException {
        String cluster = kafka.getMetadata().getName();
        String namespace = kafka.getMetadata().getNamespace();
        Function<Integer, Subject> subjectFn = i -> {
            Map<String, String> sbjAltNames = new HashMap<>(6);
            sbjAltNames.put("DNS.1", ZookeeperCluster.serviceName(cluster));
            sbjAltNames.put("DNS.2", String.format("%s.%s", ZookeeperCluster.serviceName(cluster), namespace));
            sbjAltNames.put("DNS.3", ModelUtils.serviceDnsNameWithoutClusterDomain(namespace, ZookeeperCluster.serviceName(cluster)));
            sbjAltNames.put("DNS.4", ModelUtils.serviceDnsName(namespace, ZookeeperCluster.serviceName(cluster)));
            sbjAltNames.put("DNS.5", ZookeeperCluster.podDnsName(namespace, cluster, i));
            sbjAltNames.put("DNS.6", ZookeeperCluster.podDnsNameWithoutSuffix(namespace, cluster, i));
            sbjAltNames.put("DNS.7", ModelUtils.wildcardServiceDnsNameWithoutClusterDomain(namespace, ZookeeperCluster.serviceName(cluster)));
            sbjAltNames.put("DNS.8", ModelUtils.wildcardServiceDnsName(namespace, ZookeeperCluster.serviceName(cluster)));
            sbjAltNames.put("DNS.9", ModelUtils.wildcardServiceDnsNameWithoutClusterDomain(namespace, ZookeeperCluster.headlessServiceName(cluster)));
            sbjAltNames.put("DNS.10", ModelUtils.wildcardServiceDnsName(namespace, ZookeeperCluster.headlessServiceName(cluster)));

            Subject subject = new Subject();
            subject.setOrganizationName("io.strimzi");
            subject.setCommonName(ZookeeperCluster.zookeeperClusterName(cluster));
            subject.setSubjectAltNames(sbjAltNames);

            return subject;
        };

        log.debug("{}: Reconciling zookeeper certificates", this);
        return maybeCopyOrGenerateCerts(
            kafka.getSpec().getZookeeper().getReplicas(),
            subjectFn,
            zkNodesSecret,
            podNum -> ZookeeperCluster.zookeeperPodName(cluster, podNum),
            isMaintenanceTimeWindowsSatisfied);
    }

    public Map<String, CertAndKey> generateBrokerCerts(Kafka kafka, Set<String> externalBootstrapAddresses,
            Map<Integer, Set<String>> externalAddresses, boolean isMaintenanceTimeWindowsSatisfied) throws IOException {
        String cluster = kafka.getMetadata().getName();
        String namespace = kafka.getMetadata().getNamespace();
        Function<Integer, Subject> subjectFn = i -> {
            Map<String, String> sbjAltNames = new HashMap<>();
            sbjAltNames.put("DNS.1", KafkaCluster.serviceName(cluster));
            sbjAltNames.put("DNS.2", String.format("%s.%s", KafkaCluster.serviceName(cluster), namespace));
            sbjAltNames.put("DNS.3", ModelUtils.serviceDnsNameWithoutClusterDomain(namespace, KafkaCluster.serviceName(cluster)));
            sbjAltNames.put("DNS.4", ModelUtils.serviceDnsName(namespace, KafkaCluster.serviceName(cluster)));
            sbjAltNames.put("DNS.5", KafkaCluster.headlessServiceName(cluster));
            sbjAltNames.put("DNS.6", String.format("%s.%s", KafkaCluster.headlessServiceName(cluster), namespace));
            sbjAltNames.put("DNS.7", ModelUtils.serviceDnsNameWithoutClusterDomain(namespace, KafkaCluster.headlessServiceName(cluster)));
            sbjAltNames.put("DNS.8", ModelUtils.serviceDnsName(namespace, KafkaCluster.headlessServiceName(cluster)));
            sbjAltNames.put("DNS.9", KafkaCluster.podDnsName(namespace, cluster, i));
            sbjAltNames.put("DNS.10", KafkaCluster.podDnsNameWithoutClusterDomain(namespace, cluster, i));
            int nextDnsId = 11;
            int nextIpId = 1;

            if (externalBootstrapAddresses != null)   {
                for (String dnsName : externalBootstrapAddresses) {
                    String sna = !ipv4Address.matcher(dnsName).matches() ?
                            String.format("DNS.%d", nextDnsId++) :
                            String.format("IP.%d", nextIpId++);

                    sbjAltNames.put(sna, dnsName);
                }
            }

            if (externalAddresses.get(i) != null)   {
                for (String dnsName : externalAddresses.get(i)) {
                    String sna = !ipv4Address.matcher(dnsName).matches() ?
                            String.format("DNS.%d", nextDnsId++) :
                            String.format("IP.%d", nextIpId++);

                    sbjAltNames.put(sna, dnsName);
                }
            }

            Subject subject = new Subject();
            subject.setOrganizationName("io.strimzi");
            subject.setCommonName(KafkaCluster.kafkaClusterName(cluster));
            subject.setSubjectAltNames(sbjAltNames);

            return subject;
        };
        log.debug("{}: Reconciling kafka broker certificates", this);
        return maybeCopyOrGenerateCerts(
            kafka.getSpec().getKafka().getReplicas(),
            subjectFn,
            brokersSecret,
            podNum -> KafkaCluster.kafkaPodName(cluster, podNum),
            isMaintenanceTimeWindowsSatisfied);
    }

}
