/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource;

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.extensions.DoneableStatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSet;
import io.fabric8.kubernetes.api.model.extensions.StatefulSetList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.RollableScalableResource;
import io.strimzi.certs.CertAndKey;
import io.strimzi.operator.cluster.model.AbstractModel;
import io.strimzi.operator.cluster.model.Ca;
import io.strimzi.operator.common.operator.resource.AbstractScalableResourceOperator;
import io.strimzi.operator.common.operator.resource.PodOperator;
import io.strimzi.operator.common.operator.resource.PvcOperator;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.strimzi.operator.common.operator.resource.SecretOperator;
import io.vertx.core.CompositeFuture;
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
import java.net.SocketException;
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
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Operations for {@code StatefulSets}s, which supports {@link #maybeRollingUpdate(StatefulSet, Predicate)}
 * in addition to the usual operations.
 */
public abstract class StatefulSetOperator extends AbstractScalableResourceOperator<KubernetesClient, StatefulSet, StatefulSetList, DoneableStatefulSet, RollableScalableResource<StatefulSet, DoneableStatefulSet>> {

    private static final int NO_GENERATION = -1;
    private static final String NO_UID = "NULL";
    private static final int INIT_GENERATION = 0;

    private static final Logger log = LogManager.getLogger(StatefulSetOperator.class.getName());
    private final PodOperator podOperations;
    private final PvcOperator pvcOperations;
    private final long operationTimeoutMs;

    /**
     * Constructor
     * @param vertx The Vertx instance
     * @param client The Kubernetes client
     */
    public StatefulSetOperator(Vertx vertx, KubernetesClient client, long operationTimeoutMs) {
        this(vertx, client, operationTimeoutMs, new PodOperator(vertx, client), new PvcOperator(vertx, client));
    }

    public StatefulSetOperator(Vertx vertx, KubernetesClient client, long operationTimeoutMs, PodOperator podOperator, PvcOperator pvcOperator) {
        super(vertx, client, "StatefulSet");
        this.podOperations = podOperator;
        this.operationTimeoutMs = operationTimeoutMs;
        this.pvcOperations = pvcOperator;
    }

    @Override
    protected MixedOperation<StatefulSet, StatefulSetList, DoneableStatefulSet, RollableScalableResource<StatefulSet, DoneableStatefulSet>> operation() {
        return client.apps().statefulSets();
    }

    /**
     * Asynchronously perform a rolling update of all the pods in the StatefulSet identified by the given
     * {@code namespace} and {@code name}, returning a Future that will complete when the rolling update
     * is complete. Starting with pod 0, each pod will be deleted and re-created automatically by the ReplicaSet,
     * once the pod has been recreated then given {@code isReady} function will be polled until it returns true,
     * before the process proceeds with the pod with the next higher number.
     */
    public Future<Void> maybeRollingUpdate(StatefulSet ss, Predicate<Pod> podRestart) {
        String namespace = ss.getMetadata().getNamespace();
        String name = ss.getMetadata().getName();
        final int replicas = ss.getSpec().getReplicas();
        log.debug("Considering rolling update of {}/{}", namespace, name);
        Future<Void> f = Future.succeededFuture();

        if (ss.getMetadata().getLabels().get("strimzi.io/name").endsWith("zookeeper")) { //todo do we need to check everytime or just when scaleup?
            Future<Integer> leader = zookeeperLeader(namespace, name, replicas);

            f = leader.compose(lead -> { // we have a leader, so we can roll up all other zk pods
                if (lead == -1) {
                    log.error("Could not determine who the zookeeper leader is");
                    return Future.failedFuture("Could not determine who the zookeeper leader is");
                }
                log.debug("Zookeeper leader is pod: " + lead);
                List<Future> nonLeaders = new ArrayList<>();
                for (int i = 0; i < replicas; i++) {
                    String podName = name + "-" + i;
                    if (i != lead) {
                        log.debug("maybe restarting non leader pod " + i);
                        // old followers can be restarted at the same time
                        nonLeaders.add(maybeRestartPod(ss, podName, podRestart));
                    }
                }
                return CompositeFuture.join(nonLeaders).compose(ar -> {
                    // the leader is restarted as the last
                    log.debug("maybe restarting leader pod " + lead);
                    return maybeRestartPod(ss, name + "-" + lead, podRestart);
                });
            });
        } else {
            // Then for each replica, maybe restart it
            for (int i = 0; i < replicas; i++) {
                String podName = name + "-" + i;
                f = f.compose(ignored -> maybeRestartPod(ss, podName, podRestart));
            }
        }
        return f;
    }

    public Future<Void> maybeDeletePodAndPvc(StatefulSet ss) {
        String namespace = ss.getMetadata().getNamespace();
        String name = ss.getMetadata().getName();
        final int replicas = ss.getSpec().getReplicas();
        log.debug("Considering manual deletion and restart of pods for {}/{}", namespace, name);
        Future<Void> f = Future.succeededFuture();
        for (int i = 0; i < replicas; i++) {
            String podName = name + "-" + i;
            String pvcName = AbstractModel.getPersistentVolumeClaimName(name, i);
            Pod pod = podOperations.get(namespace, podName);
            String value = pod.getMetadata().getAnnotations().get(ANNOTATION_MANUAL_DELETE_POD_AND_PVC);
            if (value != null && Boolean.valueOf(value)) {
                f = f.compose(ignored -> deletePvc(ss, pvcName))
                        .compose(ignored -> maybeRestartPod(ss, podName, p -> true));

            }
        }
        return f;
    }

    public Future<Void> deletePvc(StatefulSet ss, String pvcName) {
        String namespace = ss.getMetadata().getNamespace();
        Future<Void> f = Future.future();
        Future<ReconcileResult<PersistentVolumeClaim>> r = pvcOperations.reconcile(namespace, pvcName, null);
        r.setHandler(h -> {
            if (h.succeeded()) {
                f.complete();
            } else {
                f.fail(h.cause());
            }
        });
        return f;
    }

    private KeyStore setupKeyStore(Secret clusterSecretKey, CertificateFactory x509, char[] password, X509Certificate clientCert, CertAndKey kafkaCertKey) {
        Base64.Decoder decoder = Base64.getDecoder();

        KeyStore keyStore = null;
        try {
            keyStore = KeyStore.getInstance("PKCS12");
            keyStore.load(null, password);

            String keyText = new String(decoder.decode(clusterSecretKey.getData().get("ca.key")), StandardCharsets.ISO_8859_1);
            Pattern parse = Pattern.compile("^---*BEGIN.*---*$(.*)^---*END.*---*$.*", Pattern.MULTILINE | Pattern.DOTALL);
            Matcher matcher = parse.matcher(keyText);
            if (!matcher.find()) {
                throw new RuntimeException("Bad client (CO) key");
            }
            PrivateKey clientKey = KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(
                    Base64.getMimeDecoder().decode(matcher.group(1))));


            keyStore.setEntry("tls-probe",
                new KeyStore.PrivateKeyEntry(clientKey,
                        new Certificate[]{clientCert}),
                new KeyStore.PasswordProtection(password));

            X509Certificate kafkaCert = (X509Certificate) x509.generateCertificate(new ByteArrayInputStream(kafkaCertKey.cert()));

            String keyText2 = new String(kafkaCertKey.key(), StandardCharsets.ISO_8859_1);
            Matcher matcher2 = parse.matcher(keyText2);
            if (!matcher2.find()) {
                throw new RuntimeException("Bad kafka key");
            }
            PrivateKey kafkaKey = KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(
                        Base64.getMimeDecoder().decode(matcher2.group(1))));

            keyStore.setEntry("tls-probe2",
                    new KeyStore.PrivateKeyEntry(kafkaKey,
                            new Certificate[]{kafkaCert}),
                    new KeyStore.PasswordProtection(password));
        } catch (KeyStoreException e) {
            e.printStackTrace();
        } catch (CertificateException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InvalidKeySpecException e) {
            e.printStackTrace();
        }
        return keyStore;
    }

    private KeyStore setupTrustStore(X509Certificate caCertKafka, char[] password, X509Certificate caCertCO) {
        KeyStore trustStore = null;
        try {
            trustStore = KeyStore.getInstance("PKCS12");
            trustStore.load(null, password);

            trustStore.setEntry(caCertKafka.getSubjectDN().getName(), new KeyStore.TrustedCertificateEntry(caCertKafka), null);
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            trustStore.store(stream, password);
            trustStore.load(new ByteArrayInputStream(stream.toByteArray()), password);

            trustStore.setEntry(caCertCO.getSubjectDN().getName(), new KeyStore.TrustedCertificateEntry(caCertCO), null);
            ByteArrayOutputStream stream2 = new ByteArrayOutputStream();
            trustStore.store(stream2, password);
            trustStore.load(new ByteArrayInputStream(stream2.toByteArray()), password);
        } catch (KeyStoreException | CertificateException | NoSuchAlgorithmException | IOException e) {
            e.printStackTrace();
        }
        return trustStore;
    }

    public Future<Integer> zookeeperLeader(String namespace, String name, int replicas) {
        Future<Integer> resu = Future.future();

        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(f -> {
            int result = -1;
            for (int i = 0; i < replicas; i++) {
                if (result != -1) {
                    break;
                }
                log.debug("Checking ZookeeperLeader " + i);
                SSLSocketFactory factory;
                try {
                    CertificateFactory x509 = CertificateFactory.getInstance("X.509");
                    Base64.Decoder decoder = Base64.getDecoder();
                    char[] password = "password".toCharArray();

                    // hack
                    Pod pod = podOperations.get(namespace, name + "-" + i);
                    SecretOperator secretOperations = new SecretOperator(vertx, client);
                    String cluster = pod.getMetadata().getLabels().get("strimzi.io/cluster");
                    Secret clusterSecret = secretOperations.get(namespace,  cluster + "-kafka-brokers");
                    Secret clusterSecretKey = secretOperations.get(namespace, cluster + "-cluster-ca");
                    Secret clusterSecretCert = secretOperations.get(namespace, cluster + "-cluster-ca-cert");
                    CertAndKey kafkaCertKey = Ca.asCertAndKey(clusterSecret, cluster + "-kafka-0.key", cluster + "-kafka-0.crt");
                    X509Certificate caCertCO = (X509Certificate) x509.generateCertificate(new ByteArrayInputStream(decoder.decode(clusterSecretCert.getData().get("ca.crt"))));
                    X509Certificate caCertKafka = (X509Certificate) x509.generateCertificate(new ByteArrayInputStream(kafkaCertKey.cert()));

                    KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
                    kmf.init(setupKeyStore(clusterSecretKey, x509, password, caCertCO, kafkaCertKey), password);


                    TrustManagerFactory tmf = TrustManagerFactory.getInstance("X509");
                    tmf.init(setupTrustStore(caCertKafka, password, caCertCO));
                    SSLContext ctx = SSLContext.getInstance("TLS");
                    ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

                    factory = ctx.getSocketFactory();

                    String host = pod.getStatus().getPodIP();
                    int port = 2181;

                    SSLSocket socket = (SSLSocket) factory.createSocket();
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
                                    result = i;
                                }
                            }
                            in.close();
                            out.close();
                        } catch (SSLHandshakeException e) {
                            log.error("handshake exception - " + e.getMessage());
                            e.printStackTrace();
                        } catch (SocketException w) {

                        }
                    } finally {
                        socket.close();
                    }
                } catch (NullPointerException
                        | IOException
                        | KeyStoreException
                        | NoSuchAlgorithmException
                        | CertificateException
                        | UnrecoverableKeyException
                        | KeyManagementException e) {
                    //e.printStackTrace();
                    result = 0;
                    log.error("Error while getting Zookeeper leader");
                }
            }
            f.complete(result);
        }, true, resu.completer());
        return resu;
    }

    public Future<Void> maybeRestartPod(StatefulSet ss, String podName, Predicate<Pod> podRestart) {
        long pollingIntervalMs = 1_000;
        long timeoutMs = operationTimeoutMs;
        String namespace = ss.getMetadata().getNamespace();
        String name = ss.getMetadata().getName();
        Pod pod = podOperations.get(ss.getMetadata().getNamespace(), podName);
        if (podRestart.test(pod)) {
            Future<Void> result = Future.future();
            Future<ReconcileResult<Pod>> deleteFinished = Future.future();
            log.info("Rolling update of {}/{}: Rolling pod {}", namespace, name, podName);

            // Determine generation of deleted pod
            Future<String> deleted = getUid(namespace, podName);

            // Delete the pod
            Future<ReconcileResult<Pod>> podReconcileFuture = deleted.compose(l -> {
                log.debug("Rolling update of {}/{}: Waiting for pod {} to be deleted", namespace, name, podName);
                // null as desired parameter means pod will be deleted
                return podOperations.reconcile(namespace, podName, null);
            }).compose(ignore -> {
                Future del = podOperations.waitFor(namespace, name, pollingIntervalMs, timeoutMs, (ignore1, ignore2) -> {
                    // predicate - changed generation means pod has been updated
                    String newUid = getPodUid(podOperations.get(namespace, podName));
                    boolean done = !deleted.result().equals(newUid);
                    if (done) {
                        log.debug("Rolling pod {} finished", podName);
                    }
                    return done;
                });
                return del;
            });

            podReconcileFuture.setHandler(deleteResult -> {
                if (deleteResult.succeeded()) {
                    log.debug("Rolling update of {}/{}: Pod {} was deleted", namespace, name, podName);
                }
                deleteFinished.handle(deleteResult);
            });
            deleteFinished.compose(ix -> podOperations.readiness(namespace, podName, pollingIntervalMs, timeoutMs)).setHandler(result);
            return result;
        } else {
            log.debug("Rolling update of {}/{}: pod {} no need to roll", namespace, name, podName);
            return Future.succeededFuture();
        }
    }

    @Override
    protected Integer currentScale(String namespace, String name) {
        StatefulSet statefulSet = get(namespace, name);
        if (statefulSet != null) {
            return statefulSet.getSpec().getReplicas();
        } else {
            return null;
        }
    }

    private static ObjectMeta templateMetadata(StatefulSet resource) {
        return resource.getSpec().getTemplate().getMetadata();
    }

    public String getPodName(StatefulSet desired, int podId) {
        return templateMetadata(desired).getName() + "-" + podId;
    }

    private void setGeneration(StatefulSet desired, int nextGeneration) {
        templateMetadata(desired).getAnnotations().put(ANNOTATION_GENERATION, String.valueOf(nextGeneration));
    }

    private static int getGeneration(ObjectMeta objectMeta) {
        if (objectMeta.getAnnotations().get(ANNOTATION_GENERATION) == null) {
            return NO_GENERATION;
        }
        String generationAnno = objectMeta.getAnnotations().get(ANNOTATION_GENERATION);
        if (generationAnno == null) {
            return NO_GENERATION;
        } else {
            return Integer.parseInt(generationAnno);
        }
    }

    protected void incrementGeneration(StatefulSet current, StatefulSet desired) {
        final int generation = Integer.parseInt(templateMetadata(current).getAnnotations().getOrDefault(ANNOTATION_GENERATION, String.valueOf(INIT_GENERATION)));
        final int nextGeneration = generation + 1;
        setGeneration(desired, nextGeneration);
    }

    protected abstract boolean shouldIncrementGeneration(StatefulSet current, StatefulSet desired);

    public static int getSsGeneration(StatefulSet resource) {
        if (resource == null) {
            return NO_GENERATION;
        }
        return getGeneration(templateMetadata(resource));
    }

    public static int getPodGeneration(Pod resource) {
        if (resource == null) {
            return NO_GENERATION;
        }
        return getGeneration(resource.getMetadata());
    }

    @Override
    protected Future<ReconcileResult<StatefulSet>> internalCreate(String namespace, String name, StatefulSet desired) {
        // Create the SS...
        Future<ReconcileResult<StatefulSet>> result = Future.future();
        setGeneration(desired, INIT_GENERATION);
        Future<ReconcileResult<StatefulSet>> crt = super.internalCreate(namespace, name, desired);

        // ... then wait for the SS to be ready...
        crt.compose(res -> readiness(namespace, desired.getMetadata().getName(), 1_000, operationTimeoutMs).map(res))
        // ... then wait for all the pods to be ready
            .compose(res -> podReadiness(namespace, desired, 1_000, operationTimeoutMs).map(res))
            .compose(res -> result.complete(res), result);
        return result;
    }

    /**
     * Returns a future that completes when all the pods [0..replicas-1] in the given statefulSet are ready.
     */
    protected Future<?> podReadiness(String namespace, StatefulSet desired, long pollInterval, long operationTimeoutMs) {
        final int replicas = desired.getSpec().getReplicas();
        List<Future> waitPodResult = new ArrayList<>(replicas);
        for (int i = 0; i < replicas; i++) {
            String podName = getPodName(desired, i);
            waitPodResult.add(podOperations.readiness(namespace, podName, pollInterval, operationTimeoutMs));
        }
        return CompositeFuture.join(waitPodResult);
    }

    /**
     * Overridden to not cascade to dependent resources (e.g. pods).
     *
     * {@inheritDoc}
     */
    @Override
    protected Future<ReconcileResult<StatefulSet>> internalPatch(String namespace, String name, StatefulSet current, StatefulSet desired) {
        if (shouldIncrementGeneration(current, desired)) {
            incrementGeneration(current, desired);
        } else {
            setGeneration(desired, getSsGeneration(current));
        }
        // Don't scale via patch
        desired.getSpec().setReplicas(current.getSpec().getReplicas());
        if (log.isTraceEnabled()) {
            log.trace("Patching {} {}/{} to match desired state {}", resourceKind, namespace, name, desired);
        } else {
            log.debug("Patching {} {}/{}", resourceKind, namespace, name);
        }
        return super.internalPatch(namespace, name, current, desired, false);
    }

    /**
     * Reverts the changes done storage configuration of running cluster. Such changes are not allowed.
     *
     * @param current Current StatefulSet
     * @param desired New StatefulSet
     *
     * @return Updated StatefulSetDiff after the storage patching
     */
    protected StatefulSetDiff revertStorageChanges(StatefulSet current, StatefulSet desired) {
        desired.getSpec().setVolumeClaimTemplates(current.getSpec().getVolumeClaimTemplates());
        desired.getSpec().getTemplate().getSpec().setInitContainers(current.getSpec().getTemplate().getSpec().getInitContainers());
        desired.getSpec().getTemplate().getSpec().setSecurityContext(current.getSpec().getTemplate().getSpec().getSecurityContext());

        if (current.getSpec().getVolumeClaimTemplates().isEmpty()) {
            // We are on ephemeral storage and changing to persistent
            List<Volume> volumes = current.getSpec().getTemplate().getSpec().getVolumes();
            for (int i = 0; i < volumes.size(); i++) {
                Volume vol = volumes.get(i);
                if (AbstractModel.VOLUME_NAME.equals(vol.getName()) && vol.getEmptyDir() != null) {
                    desired.getSpec().getTemplate().getSpec().getVolumes().add(0, volumes.get(i));
                    break;
                }
            }
        } else {
            // We are on persistent storage and changing to ephemeral
            List<Volume> volumes = desired.getSpec().getTemplate().getSpec().getVolumes();
            for (int i = 0; i < volumes.size(); i++) {
                Volume vol = volumes.get(i);
                if (AbstractModel.VOLUME_NAME.equals(vol.getName()) && vol.getEmptyDir() != null) {
                    volumes.remove(i);
                    break;
                }
            }
        }

        return new StatefulSetDiff(current, desired);
    }

    protected Future<String> getUid(String namespace, String podName) {
        Future<String> result = Future.future();
        vertx.createSharedWorkerExecutor("kubernetes-ops-tool").executeBlocking(
            future -> {
                String uid = getPodUid(podOperations.get(namespace, podName));
                future.complete(uid);
            }, true, result.completer()
        );
        return result;
    }

    private static String getPodUid(Pod resource) {
        if (resource == null || resource.getMetadata() == null) {
            return NO_UID;
        }
        return resource.getMetadata().getUid();
    }
}
