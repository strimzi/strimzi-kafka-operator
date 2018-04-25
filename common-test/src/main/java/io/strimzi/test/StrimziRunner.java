/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.strimzi.test.k8s.KubeClient;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.Minishift;
import io.strimzi.test.k8s.OpenShift;
import org.junit.ClassRule;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.Annotatable;
import org.junit.runners.model.FrameworkField;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;
import org.junit.runners.model.TestClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.annotation.Repeatable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.strimzi.test.TestUtils.indent;
import static io.strimzi.test.k8s.BaseKubeClient.CM;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * A test runner which sets up Strimzi resources in a Kubernetes cluster
 * according to annotations ({@link Namespace}, {@link Resources}, {@link ClusterController}, {@link KafkaCluster})
 * on the test class and/or test methods. {@link OpenShiftOnly} can be used to ignore tests when not running on
 * OpenShift (if the thing under test is OpenShift-specific).
 */
public class StrimziRunner extends BlockJUnit4ClassRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(StrimziRunner.class);

    /**
     * If env var NOTEARDOWN is set to any value then teardown for resources supported by annotations
     * won't happen. This can be useful in debugging a single test, because it leaves the cluster
     * in the state it was in when the test failed.
     */
    public static final String NOTEARDOWN = "NOTEARDOWN";
    public static final String KAFKA_PERSISTENT_CM = "../examples/configmaps/cluster-controller/kafka-persistent.yaml";
    public static final String KAFKA_CONNECT_CM = "../examples/configmaps/cluster-controller/kafka-connect.yaml";
    public static final String CC_INSTALL_DIR = "../examples/install/cluster-controller";
    public static final String CC_DEPLOYMENT_NAME = "strimzi-cluster-controller";
    public static final String TOPIC_CM = "../examples/configmaps/topic-controller/kafka-topic-configmap.yaml";

    private KubeClusterResource clusterResource;

    public StrimziRunner(Class<?> klass) throws InitializationError {
        super(klass);
    }

    @Override
    protected boolean isIgnored(FrameworkMethod child) {
        if (super.isIgnored(child)) {
            return true;
        } else {
            return isWrongClusterType(getTestClass(), child) || isWrongClusterType(child, child)
                    || isIgnoredByEnvVar(getTestClass(), child) || isIgnoredByEnvVar(child, child);
        }
    }

    private boolean isWrongClusterType(Annotatable annotated, FrameworkMethod test) {
        boolean result = annotated.getAnnotation(OpenShiftOnly.class) != null
                && !(clusterResource().cluster() instanceof OpenShift
                    || clusterResource().cluster() instanceof Minishift);
        if (result) {
            LOGGER.info("{} is @OpenShiftOnly, but the running cluster is not OpenShift: Ignoring {}", name(annotated), name(test));
        }
        return result;
    }

    private boolean isIgnoredByEnvVar(Annotatable annotated, FrameworkMethod test) {
        IgnoreIfDef anno = annotated.getAnnotation(IgnoreIfDef.class);
        boolean result = anno != null
                && System.getenv(anno.value()) != null;
        if (result) {
            LOGGER.info("{} is @IgnoreIfDef, and {} has value {}: Ignoring {}", name(annotated), anno.value(), System.getenv(anno.value()), name(test));
        }
        return result;
    }

    @Override
    protected Statement methodBlock(FrameworkMethod method) {
        Statement statement = super.methodBlock(method);
        statement = withConnectClusters(method, statement);
        statement = withKafkaClusters(method, statement);
        statement = withClusterController(method, statement);
        statement = withResources(method, statement);
        statement = withTopic(method, statement);
        statement = withNamespaces(method, statement);
        statement = withLogging(method, statement);
        return statement;
    }

    /**
     * Get the (possibly @Repeatable) annotations on the given element.
     * @param element
     * @param annotationType
     * @param <A>
     * @return
     */
    <A extends Annotation> List<A> annotations(Annotatable element, Class<A> annotationType) {
        final List<A> list;
        A c = element.getAnnotation(annotationType);
        if (c != null) {
            list = singletonList(c);
        } else {
            Repeatable r = annotationType.getAnnotation(Repeatable.class);
            if (r != null) {
                Class<? extends Annotation> ra = r.value();
                Annotation container = element.getAnnotation(ra);
                if (container != null) {
                    try {
                        Method value = ra.getDeclaredMethod("value");
                        list = asList((A[]) value.invoke(container));
                    } catch (ReflectiveOperationException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    list = emptyList();
                }
            } else {
                list = emptyList();
            }
        }

        return list;
    }

    abstract class Bracket extends Statement implements Runnable {
        private final Statement statement;
        private final Thread hook = new Thread(this);
        public Bracket(Statement statement) {
            this.statement = statement;
        }
        @Override
        public void evaluate() throws Throwable {
            try {
                Runtime.getRuntime().addShutdownHook(hook);
                before();
                statement.evaluate();
            } catch (Throwable e) {
                if (!(e instanceof VirtualMachineError)) {
                    try {
                        onError(e);
                    } catch (Throwable t) {
                        e.addSuppressed(t);
                    }
                }
                throw e;

            } finally {
                Runtime.getRuntime().removeShutdownHook(hook);
                runAfter();
            }

        }
        protected abstract void before();
        protected abstract void after();

        List<String> ccFirst(List<String> l) {
            List<String> head = new ArrayList<>();
            List<String> tail = new ArrayList<>();
            for (String n : l) {
                if (n.startsWith("strimzi-cluster-controller-")) {
                    head.add(n);
                } else {
                    tail.add(n);
                }
            }
            head.addAll(tail);
            return head;
        }

        protected void onError(Throwable t) {
            LOGGER.info("The test is failing/erroring due to {}, here's some diagnostic output{}{}",
                    t, System.lineSeparator(), "----------------------------------------------------------------------");
            for (String pod : ccFirst(kubeClient().list("pod"))) {
                LOGGER.info("Logs from pod {}:{}{}", pod, System.lineSeparator(), indent(kubeClient().logs(pod)));
            }
            for (String resourceType : asList("pod", "deployment", "statefulset", "cm")) {
                for (String resourceName : ccFirst(kubeClient().list(resourceType))) {
                    LOGGER.info("Description of {} '{}':{}{}", resourceType, resourceName,
                            System.lineSeparator(), indent(kubeClient().describe(resourceType, resourceName)));
                }
            }

            LOGGER.info("That's all the diagnostic info, the exception {} will now propagate and the test will fail{}{}",
                    t,
                    t, System.lineSeparator(), "----------------------------------------------------------------------");
        }

        @Override
        public void run() {
            runAfter();
        }
        public void runAfter() {
            if (System.getenv(NOTEARDOWN) == null) {
                after();
            }
        }
    }

    protected KubeClient<?> kubeClient() {
        return clusterResource().client();
    }

    String name(Annotatable a) {
        if (a instanceof TestClass) {
            return "class " + ((TestClass) a).getJavaClass().getSimpleName();
        } else if (a instanceof FrameworkMethod) {
            FrameworkMethod method = (FrameworkMethod) a;
            return "method " + method.getDeclaringClass().getSimpleName() + "." + method.getName() + "()";
        } else if (a instanceof FrameworkField) {
            FrameworkField field = (FrameworkField) a;
            return "field " + field.getDeclaringClass().getSimpleName() + "." + field.getName();
        } else {
            return a.toString();
        }
    }

    String getContent(File file, Consumer<JsonNode> edit) {
        YAMLMapper mapper = new YAMLMapper();
        try {
            JsonNode node = mapper.readTree(file);
            edit.accept(node);
            return mapper.writeValueAsString(node);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Statement withConnectClusters(Annotatable element,
                                        Statement statement) {
        Statement last = statement;
        for (ConnectCluster cluster : annotations(element, ConnectCluster.class)) {
            // use the example kafka-connect.yaml as a template, but modify it according to the annotation
            String yaml = getContent(new File(KAFKA_CONNECT_CM), node -> {
                JsonNode metadata = node.get("metadata");
                ((ObjectNode) metadata).put("name", cluster.name());
                JsonNode data = node.get("data");
                ((ObjectNode) data).put("nodes", String.valueOf(cluster.nodes()));
                ((ObjectNode) data).put("KAFKA_CONNECT_BOOTSTRAP_SERVERS", cluster.bootstrapServers());
                // updates values for config map
                for (CmData cmData : cluster.config()) {
                    ((ObjectNode) data).put(cmData.key(), cmData.value());
                }
            });
            last = new Bracket(last) {
                private final String deploymentName = cluster.name() + "-connect";
                @Override
                protected void before() {
                    LOGGER.info("Creating connect cluster '{}' before test per @ConnectCluster annotation on {}", cluster.name(), name(element));
                    // create cm
                    kubeClient().createContent(yaml);
                    // wait for deployment
                    kubeClient().waitForDeployment(deploymentName);
                }

                @Override
                protected void after() {
                    LOGGER.info("Deleting connect cluster '{}' after test per @ConnectCluster annotation on {}", cluster.name(), name(element));
                    // delete cm
                    kubeClient().deleteContent(yaml);
                    // wait for ss to go
                    kubeClient().waitForResourceDeletion("deployment", deploymentName);
                }
            };
        }
        return last;
    }

    private Statement withKafkaClusters(Annotatable element,
                                    Statement statement) {
        Statement last = statement;
        for (KafkaCluster cluster : annotations(element, KafkaCluster.class)) {
            // use the example kafka-ephemeral as a template, but modify it according to the annotation
            String yaml = getContent(new File(KAFKA_PERSISTENT_CM), node -> {
                JsonNode metadata = node.get("metadata");
                ((ObjectNode) metadata).put("name", cluster.name());
                JsonNode data = node.get("data");
                ((ObjectNode) data).put("kafka-nodes", String.valueOf(cluster.kafkaNodes()));
                ((ObjectNode) data).put("zookeeper-nodes", String.valueOf(cluster.zkNodes()));
                // updates values for config map
                for (CmData cmData : cluster.config()) {
                    ((ObjectNode) data).put(cmData.key(), cmData.value());
                }
            });
            last = new Bracket(last) {
                private final String kafkaStatefulSetName = cluster.name() + "-kafka";
                private final String zkStatefulSetName = cluster.name() + "-zookeeper";
                private final String tcDeploymentName = cluster.name() + "-topic-controller";
                @Override
                protected void before() {
                    LOGGER.info("Creating kafka cluster '{}' before test per @KafkaCluster annotation on {}", cluster.name(), name(element));
                    // create cm
                    kubeClient().createContent(yaml);
                    // wait for ss
                    kubeClient().waitForStatefulSet(kafkaStatefulSetName, cluster.kafkaNodes());
                    // wait fot TC
                    kubeClient().waitForDeployment(tcDeploymentName);
                }

                @Override
                protected void after() {
                    LOGGER.info("Deleting kafka cluster '{}' after test per @KafkaCluster annotation on {}", cluster.name(), name(element));
                    // delete cm
                    kubeClient().deleteContent(yaml);
                    // wait for ss to go
                    try {
                        kubeClient().waitForResourceDeletion("statefulset", zkStatefulSetName);
                    } catch (Exception e) {
                        LOGGER.info("Exception {} while cleaning up. ", e.toString());
                        onError(e);
                    }
                }
            };
        }
        return last;
    }

    private String changeOrgAndTag(String image, String newOrg, String newTag) {
        return image.replaceFirst("^strimzi/", newOrg + "/").replaceFirst(":[^:]+$", ":" + newTag);
    }

    private Statement withClusterController(Annotatable element,
                                    Statement statement) {
        Statement last = statement;
        for (ClusterController cc : annotations(element, ClusterController.class)) {
            List<String> yamls = Arrays.stream(new File(CC_INSTALL_DIR).listFiles()).sorted().map(f -> getContent(f, node -> {
                // Change the docker org of the images in the 04-deployment.yaml
                if ("04-deployment.yaml".equals(f.getName())) {
                    String dockerOrg = System.getenv().getOrDefault("DOCKER_ORG", "strimzi");
                    String dockerTag = System.getenv().getOrDefault("DOCKER_TAG", "latest");
                    ObjectNode containerNode = (ObjectNode) node.get("spec").get("template").get("spec").get("containers").get(0);
                    containerNode.put("imagePullPolicy", "Always");
                    JsonNode ccImageNode = containerNode.get("image");
                    ((ObjectNode) containerNode).put("image", changeOrgAndTag(ccImageNode.asText(), dockerOrg, dockerTag));
                    for (JsonNode envVar : containerNode.get("env")) {
                        String varName = envVar.get("name").textValue();
                        // Replace all the default images with ones from the $DOCKER_ORG org and with the $DOCKER_TAG tag
                        if (varName.matches("STRIMZI_DEFAULT_.*_IMAGE")) {
                            String value = envVar.get("value").textValue();
                            ((ObjectNode) envVar).put("value", changeOrgAndTag(value, dockerOrg, dockerTag));
                        }
                        // Updates default values of env variables
                        for (EnvVariables envVariable : cc.envVariables()) {
                            if (varName.equals(envVariable.key())) {
                                ((ObjectNode) envVar).put("value", envVariable.value());
                            }
                        }
                    }
                }
            })).collect(Collectors.toList());
            last = new Bracket(last) {
                @Override
                protected void before() {
                    // Here we record the state of the cluster
                    LOGGER.info("Creating cluster controller {} before test per @ClusterController annotation on {}", cc, name(element));
                    for (String yaml: yamls) {
                        kubeClient().clientWithAdmin().createContent(yaml);
                    }
                    kubeClient().waitForDeployment(CC_DEPLOYMENT_NAME);
                }

                @Override
                protected void after() {
                    LOGGER.info("Deleting cluster controller {} after test per @ClusterController annotation on {}", cc, name(element));
                    for (int i = yamls.size() - 1; i >= 0; i--) {
                        kubeClient().clientWithAdmin().deleteContent(yamls.get(i));
                    }
                    kubeClient().waitForResourceDeletion("deployment", CC_DEPLOYMENT_NAME);
                }
            };
        }
        return last;
    }

    private Statement withResources(Annotatable element,
                                    Statement statement) {
        Statement last = statement;
        for (Resources resources : annotations(element, Resources.class)) {
            last = new Bracket(last) {
                @Override
                protected void before() {
                    // Here we record the state of the cluster
                    LOGGER.info("Creating resources {}, before test per @Resources annotation on {}", Arrays.toString(resources.value()), name(element));

                    kubeClient().create(resources.value());
                }

                private KubeClient kubeClient() {
                    KubeClient client = StrimziRunner.this.kubeClient();
                    if (resources.asAdmin()) {
                        client = client.clientWithAdmin();
                    }
                    return client;
                }

                @Override
                protected void after() {
                    LOGGER.info("Deleting resources {}, after test per @Resources annotation on {}", Arrays.toString(resources.value()), name(element));
                    // Here we verify the cluster is in the same state
                    kubeClient().delete(resources.value());
                }
            };
        }
        return last;
    }

    private Statement withNamespaces(Annotatable element,
                                     Statement statement) {
        Statement last = statement;
        for (Namespace namespace : annotations(element, Namespace.class)) {
            last = new Bracket(last) {
                String previousNamespace = null;
                @Override
                protected void before() {
                    LOGGER.info("Creating namespace '{}' before test per @Namespace annotation on {}", namespace.value(), name(element));
                    kubeClient().createNamespace(namespace.value());
                    previousNamespace = kubeClient().namespace(namespace.value());
                }

                @Override
                protected void after() {
                    LOGGER.info("Deleting namespace '{}' after test per @Namespace annotation on {}", namespace.value(), name(element));
                    kubeClient().deleteNamespace(namespace.value());
                    kubeClient().namespace(previousNamespace);
                }
            };
        }
        return last;
    }

    private boolean areAllChildrenIgnored() {
        for (FrameworkMethod child : getChildren()) {
            if (!isIgnored(child)) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected Statement classBlock(final RunNotifier notifier) {
        Statement statement = super.classBlock(notifier);
        TestClass testClass = getTestClass();
        if (!areAllChildrenIgnored()) {
            statement = withConnectClusters(testClass, statement);
            statement = withKafkaClusters(testClass, statement);
            statement = withClusterController(testClass, statement);
            statement = withResources(testClass, statement);
            statement = withTopic(testClass, statement);
            statement = withNamespaces(testClass, statement);
            statement = withLogging(testClass, statement);
        }
        return statement;
    }

    /** Get the value of the @ClassRule-annotated KubeClusterResource field*/
    private KubeClusterResource clusterResource() {
        if (clusterResource == null) {
            List<KubeClusterResource> fieldValues = getTestClass().getAnnotatedFieldValues(null, ClassRule.class, KubeClusterResource.class);
            if (fieldValues == null || fieldValues.isEmpty()) {
                fieldValues = getTestClass().getAnnotatedMethodValues(null, ClassRule.class, KubeClusterResource.class);
            }
            if (fieldValues == null || fieldValues.isEmpty()) {
                clusterResource = new KubeClusterResource();
                clusterResource.before();
            } else {
                clusterResource = fieldValues.get(0);
            }
        }
        return clusterResource;
    }

    private Statement withTopic(Annotatable element, Statement statement) {
        Statement last = statement;
        for (Topic topic : annotations(element, Topic.class)) {
            final JsonNodeFactory factory = JsonNodeFactory.instance;
            final ObjectNode node = factory.objectNode();
            node.put("apiVersion", "v1");
            node.put("kind", "ConfigMap");
            node.putObject("metadata");
            JsonNode metadata = node.get("metadata");
            ((ObjectNode) metadata).put("name", topic.name());
            ((ObjectNode) metadata).putObject("labels");
            JsonNode labels = metadata.get("labels");
            ((ObjectNode) labels).put("strimzi.io/kind", "topic");
            ((ObjectNode) labels).put("strimzi.io/cluster", topic.clusterName());
            node.putObject("data");
            JsonNode data = node.get("data");
            ((ObjectNode) data).put("name", topic.name());
            ((ObjectNode) data).put("partitions", topic.partitions());
            ((ObjectNode) data).put("replicas", topic.replicas());
            String configMap = node.toString();
            last = new Bracket(last) {
                @Override
                protected void before() {
                    LOGGER.info("Creating Topic {} {}", topic.name(), name(element));
                    // create cm
                    kubeClient().createContent(configMap);
                    kubeClient().waitForResourceCreation(CM, topic.name());
                }
                @Override
                protected void after() {
                    LOGGER.info("Deleting ConfigMap '{}' after test per @Topic annotation on {}", topic.clusterName(), name(element));
                    // delete cm
                    kubeClient().deleteContent(configMap);
                    kubeClient().waitForResourceDeletion(CM, topic.name());
                }
            };
        }
        return last;
    }


    private Statement withLogging(Annotatable element, Statement statement) {
        return new Bracket(statement) {
            private long t0;
            @Override
            protected void before() {
                t0 = System.currentTimeMillis();
                LOGGER.info("Starting {}", name(element));
            }

            @Override
            protected void after() {
                LOGGER.info("Finished {}: took {}",
                        name(element),
                        duration(System.currentTimeMillis() - t0));
            }
        };
    }

    private static String duration(long millis) {
        long ms = millis % 1_000;
        long time = millis / 1_000;
        long minutes = time / 60;
        long seconds = time % 60;
        return minutes + "m" + seconds + "." + ms + "s";
    }

}
