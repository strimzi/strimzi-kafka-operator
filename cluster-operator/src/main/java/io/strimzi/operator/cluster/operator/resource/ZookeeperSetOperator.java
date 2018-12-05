/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.apps.StatefulSet;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.certs.CertAndKey;
import io.strimzi.operator.cluster.ClusterOperator;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.ArrayList;
import java.util.Base64;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Specialization of {@link StatefulSetOperator} for StatefulSets of Zookeeper nodes
 */
public class ZookeeperSetOperator extends StatefulSetOperator {

    private static final Logger log = LogManager.getLogger(ZookeeperSetOperator.class);

    /**
     * Constructor
     *
     * @param vertx  The Vertx instance
     * @param client The Kubernetes client
     */
    public ZookeeperSetOperator(Vertx vertx, KubernetesClient client, long operationTimeoutMs) {
        super(vertx, client, operationTimeoutMs);
    }

    @Override
    protected boolean shouldIncrementGeneration(StatefulSet current, StatefulSet desired) {
        StatefulSetDiff diff = new StatefulSetDiff(current, desired);
        if (diff.changesVolumeClaimTemplates()) {
            log.warn("Changing Zookeeper storage type or size is not possible. The changes will be ignored.");
            diff = revertStorageChanges(current, desired);
        }
        return !diff.isEmpty() && needsRollingUpdate(diff);
    }

    public static boolean needsRollingUpdate(StatefulSetDiff diff) {
        // Because for ZK the brokers know about each other via the config, and rescaling requires a rolling update
        if (diff.changesSpecReplicas()) {
            log.debug("Changed #replicas => needs rolling update");
            return true;
        }
        if (diff.changesLabels()) {
            log.debug("Changed labels => needs rolling update");
            return true;
        }
        if (diff.changesSpecTemplateSpec()) {
            log.debug("Changed template spec => needs rolling update");
            return true;
        }
        return false;
    }

    @Override
    public Future<Void> maybeRollingUpdate(StatefulSet ss, Predicate<Pod> podRestart) {
        String namespace = ss.getMetadata().getNamespace();
        String name = ss.getMetadata().getName();
        final int replicas = ss.getSpec().getReplicas();
        log.debug("Considering rolling update of {}/{}", namespace, name);
        Future<Void> f = Future.succeededFuture();
        boolean zkRoll = false;
        ArrayList<Pod> pods = new ArrayList<>();
        String cluster = ss.getMetadata().getLabels().get(Labels.STRIMZI_CLUSTER_LABEL);
        for (int i = 0; i < replicas; i++) {
            Pod pod = podOperations.get(ss.getMetadata().getNamespace(), KafkaResources.zookeeperPodName(cluster, i));
            zkRoll |= podRestart.test(pod);
            pods.add(pod);
        }

        if (zkRoll) {
            Future<Integer> leader = zookeeperLeader(cluster, namespace, pods);
            f = leader.compose(lead -> { // we know a leader, so we can roll up all other zk pods
                if (lead == -1) {
                    return Future.failedFuture("Zookeeper leader could not be found");
                }
                log.debug("Zookeeper leader is pod: " + lead);
                Future<Void> f2 = Future.succeededFuture();
                for (int i = 0; i < replicas; i++) {
                    String podName = KafkaResources.zookeeperPodName(cluster, i);
                    if (i != lead) {
                        log.debug("maybe restarting non leader pod " + i);
                        // roll the pod and wait until it is ready
                        // this prevents rolling into faulty state (note: this applies just for ZK pods)
                        f2 = f2.compose(ignore -> maybeRestartPod(ss, podName, podRestart))
                                .compose(ignore -> podOperations.readiness(namespace, podName, 1_000, operationTimeoutMs));
                    }
                }
                return f2.compose(ar -> {
                    // the leader is rolled as the last
                    log.debug("maybe restarting leader pod " + lead);
                    return maybeRestartPod(ss, name + "-" + lead, podRestart)
                            .compose(ignore -> podOperations.readiness(namespace,
                                    KafkaResources.zookeeperPodName(cluster, lead), 1_000, operationTimeoutMs));
                });
            });
        }
        return f;
    }

    private KeyStore setupKeyStore(Secret clusterSecretKey, CertificateFactory x509, char[] password,
                                   X509Certificate clientCert, CertAndKey coCertKey) throws CertificateException {
        Base64.Decoder decoder = Base64.getDecoder();
        KeyStore keyStore = null;
        try {
            keyStore = KeyStore.getInstance("PKCS12");
            keyStore.load(null, password);

            String keyText = new String(decoder.decode(clusterSecretKey.getData().get("ca.key")), StandardCharsets.ISO_8859_1);
            Pattern parse = Pattern.compile("^---*BEGIN.*---*$(.*)^---*END.*---*$.*", Pattern.MULTILINE | Pattern.DOTALL);
            Matcher matcher = parse.matcher(keyText);
            if (!matcher.find()) {
                throw new RuntimeException("Bad client (CO) key. Key misses BEGIN or END markers");
            }
            PrivateKey clientKey = KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(
                    Base64.getMimeDecoder().decode(matcher.group(1))));

            keyStore.setEntry("tls-probe",
                    new KeyStore.PrivateKeyEntry(clientKey,
                            new Certificate[]{clientCert}),
                    new KeyStore.PasswordProtection(password));

            X509Certificate coCert = (X509Certificate) x509.generateCertificate(new ByteArrayInputStream(coCertKey.cert()));

            String coCertKeyText = new String(coCertKey.key(), StandardCharsets.ISO_8859_1);
            Matcher matcher2 = parse.matcher(coCertKeyText);
            if (!matcher2.find()) {
                throw new RuntimeException("Bad client (CO) key. Key misses BEGIN or END markers");
            }
            PrivateKey coKey = KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(
                    Base64.getMimeDecoder().decode(matcher2.group(1))));

            keyStore.setEntry("tls-probe2",
                    new KeyStore.PrivateKeyEntry(coKey,
                            new Certificate[]{coCert}),
                    new KeyStore.PasswordProtection(password));
        } catch (KeyStoreException
                | NoSuchAlgorithmException
                | IOException
                | InvalidKeySpecException e) {
            log.error("Error while generator Cluster Operator key store", e);
        }
        return keyStore;
    }

    private KeyStore setupTrustStore(char[] password, X509Certificate caCertCO) {
        KeyStore trustStore = null;
        try {
            trustStore = KeyStore.getInstance("PKCS12");
            trustStore.load(null, password);

            trustStore.setEntry(caCertCO.getSubjectDN().getName(), new KeyStore.TrustedCertificateEntry(caCertCO), null);
            ByteArrayOutputStream stream2 = new ByteArrayOutputStream();
            trustStore.store(stream2, password);
            trustStore.load(new ByteArrayInputStream(stream2.toByteArray()), password);
        } catch (KeyStoreException | CertificateException | NoSuchAlgorithmException | IOException e) {
            log.error("Error while generator Cluster Operator trust store", e);
        }
        return trustStore;
    }

    private SSLSocketFactory socketFactory(String cluster, String namespace) throws CertificateException {
        SSLSocketFactory factory = null;
        try {
            CertificateFactory x509 = CertificateFactory.getInstance("X.509");
            Base64.Decoder decoder = Base64.getDecoder();
            char[] password = new char[0];
            SecretOperator secretOperations = new SecretOperator(vertx, client);
            Secret brokersSecret = secretOperations.get(namespace, ClusterOperator.secretName(cluster));
            Secret clusterCaKeySecret = secretOperations.get(namespace, cluster + "-cluster-ca");
            Secret clusterCaCertificateSecret = secretOperations.get(namespace, cluster + "-cluster-ca-cert");
            CertAndKey coCertKey = Ca.asCertAndKey(brokersSecret, "cluster-operator.key", "cluster-operator.crt");
            X509Certificate caCertCO = (X509Certificate) x509.generateCertificate(
                    new ByteArrayInputStream(decoder.decode(clusterCaCertificateSecret.getData().get("ca.crt"))));
            KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
            kmf.init(setupKeyStore(clusterCaKeySecret, x509, password, caCertCO, coCertKey), password);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance("X509");
            tmf.init(setupTrustStore(password, caCertCO));
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
            factory = ctx.getSocketFactory();
        } catch (NoSuchAlgorithmException
                | UnrecoverableKeyException
                | KeyStoreException
                | KeyManagementException e) {
            log.error("Error while creating Cluster Operator SocketFactory", e);
        }
        return factory;
    }

    private boolean isLeader(Pod pod, SSLSocketFactory factory) {
        int port = 2181;
        boolean leader = false;
        try {
            if (pod.getStatus() == null) {
                log.debug("Pod has no status (test run)");
                return true;
            }
            String host = pod.getStatus().getPodIP();

            SSLSocket socket = null;
            Socket plainSocket = factory.createSocket();
            if (plainSocket instanceof SSLSocket) {
                // findbugs plugin does not like direct overtyping
                socket = (SSLSocket) plainSocket;
            }
            if (socket == null) {
                log.error("Could not create socket for getting Zookeeper data");
                return false;
            }
            log.debug("Connecting client to {}:{}", host, port);
            try {
                socket.connect(new InetSocketAddress(host, port), 10_000);
            } catch (ConnectException | SocketTimeoutException e) {
                log.error("Could not connect " + e.getMessage());
            }
            try {
                log.debug("Starting handshake with {}", socket.getRemoteSocketAddress());
                try {
                    socket.startHandshake();
                    PrintWriter out = new PrintWriter(
                            new BufferedWriter(
                                    new OutputStreamWriter(
                                            socket.getOutputStream(), StandardCharsets.UTF_8)));

                    out.println("stat");
                    out.flush();

                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(
                                    socket.getInputStream(), StandardCharsets.UTF_8));

                    String inputLine;
                    while ((inputLine = in.readLine()) != null) {
                        log.debug(inputLine);
                        if (inputLine.equals("Mode: leader")) {
                            leader = true;
                        }
                    }
                    in.close();
                    out.close();
                } catch (SSLHandshakeException e) {
                    log.error("Error while performing TLS handshake with pod {} in namespace {}",
                            pod.getMetadata().getName(), pod.getMetadata().getNamespace(), e);
                }
            } finally {
                socket.close();
            }
        } catch (IOException e) {
            log.debug("Error while getting Zookeeper leader " + e.getMessage());
        }
        return leader;
    }

    public Future<Integer> zookeeperLeader(String cluster, String namespace, ArrayList<Pod> pods) {
        Future<Integer> result = Future.future();
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(f -> {
            int leader = -1;
            if (pods.size() == 1) { // standalone
                leader = 0;
            } else {
                SSLSocketFactory factory = null;
                try {
                    factory = socketFactory(cluster, namespace);
                } catch (CertificateException e) {
                    leader = -1;
                }
                for (int i = 0; i < pods.size(); i++) {
                    log.debug("Checking ZookeeperLeader " + i);
                    if (isLeader(pods.get(i), factory)) {
                        leader = i;
                        log.info("Zookeeper leader found " + pods.get(i).getMetadata().getName());
                        break;
                    } else {
                        log.info(pods.get(i).getMetadata().getName() + " is not a leader");
                    }
                }
            }
            f.complete(leader);
        }, true, result.completer());
        return result;
    }
}
