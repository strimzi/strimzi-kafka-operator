/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test;

import com.fasterxml.jackson.databind.JsonNode;
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
import java.util.Arrays;
import java.util.List;

import static io.strimzi.test.TestUtils.indent;
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
    public static final String KAFKA_EPHEMERAL_CM = "../examples/configmaps/cluster-controller/kafka-ephemeral.yaml";
    public static final String KAFKA_CONNECT_CM = "../examples/configmaps/cluster-controller/kafka-connect.yaml";
    public static final String CC_INSTALL_PATH = "../examples/install/cluster-controller";
    public static final String CC_DEPLOYMENT_NAME = "strimzi-cluster-controller";

    private KubeClusterResource clusterResource;

    public StrimziRunner(Class<?> klass) throws InitializationError {
        super(klass);
    }

    @Override
    protected boolean isIgnored(FrameworkMethod child) {
        if (super.isIgnored(child)) {
            return true;
        } else {
            return isWrongClusterType(getTestClass(), child) || isWrongClusterType(child, child);
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

    @Override
    protected Statement methodBlock(FrameworkMethod method) {
        Statement statement = super.methodBlock(method);
        statement = withConnectClusters(method, statement);
        statement = withKafkaClusters(method, statement);
        statement = withClusterController(method, statement);
        statement = withResources(method, statement);
        statement = withNamespaces(method, statement);
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
                    onError(e);
                }
                throw e;

            } finally {
                Runtime.getRuntime().removeShutdownHook(hook);
                runAfter();
            }

        }
        protected abstract void before();
        protected abstract void after();

        protected void onError(Throwable t) {
            LOGGER.info("The test is failing/erroring due to {}, here's some diagnostic output{}{}",
                    t, System.lineSeparator(), "----------------------------------------------------------------------");
            for (String resourceType : asList("pod", "deployment", "statefulset", "cm")) {
                for (String resourceName : kubeClient().list(resourceType)) {
                    LOGGER.info("Description of {} '{}':{}{}", resourceType, resourceName,
                            System.lineSeparator(), indent(kubeClient().describe(resourceType, resourceName)));
                }
            }
            for (String pod : kubeClient().list("pod")) {
                LOGGER.info("Logs from pod {}:{}{}", pod, System.lineSeparator(), indent(kubeClient().logs(pod)));
            }
            LOGGER.info("That's all the diagnostic info, the exception {} will now propoagate and the test will fail{}{}",
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

    private Statement withConnectClusters(Annotatable element,
                                        Statement statement) {
        Statement last = statement;
        for (ConnectCluster cluster : annotations(element, ConnectCluster.class)) {
            // use the example kafka-connect.yaml as a template, but modify it according to the annotation
            YAMLMapper mapper = new YAMLMapper();
            String yaml;
            try {
                JsonNode node = mapper.readTree(new File(KAFKA_CONNECT_CM));
                JsonNode metadata = node.get("metadata");
                ((ObjectNode) metadata).put("name", cluster.name());
                JsonNode data = node.get("data");
                ((ObjectNode) data).put("nodes", String.valueOf(cluster.nodes()));
                ((ObjectNode) data).put("KAFKA_CONNECT_BOOTSTRAP_SERVERS", cluster.bootstrapServers());
                yaml = mapper.writeValueAsString(node);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
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
            YAMLMapper mapper = new YAMLMapper();
            String yaml;
            try {
                JsonNode node = mapper.readTree(new File(KAFKA_EPHEMERAL_CM));
                JsonNode metadata = node.get("metadata");
                ((ObjectNode) metadata).put("name", cluster.name());
                JsonNode data = node.get("data");
                ((ObjectNode) data).put("kafka-nodes", String.valueOf(cluster.kafkaNodes()));
                ((ObjectNode) data).put("zookeeper-nodes", String.valueOf(cluster.zkNodes()));
                yaml = mapper.writeValueAsString(node);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            last = new Bracket(last) {
                private final String kafkaStatefulSetName = cluster.name() + "-kafka";
                private final String zkStatefulSetName = cluster.name() + "-zookeeper";
                @Override
                protected void before() {
                    LOGGER.info("Creating kafka cluster '{}' before test per @KafkaCluster annotation on {}", cluster.name(), name(element));
                    // create cm
                    kubeClient().createContent(yaml);
                    // wait for ss
                    kubeClient().waitForStatefulSet(kafkaStatefulSetName, cluster.kafkaNodes());
                }

                @Override
                protected void after() {
                    LOGGER.info("Deleting kafka cluster '{}' after test per @KafkaCluster annotation on {}", cluster.name(), name(element));
                    // delete cm
                    kubeClient().deleteContent(yaml);
                    // wait for ss to go
                    kubeClient().waitForResourceDeletion("statefulset", zkStatefulSetName);
                }
            };
        }
        return last;
    }

    private Statement withClusterController(Annotatable element,
                                    Statement statement) {
        Statement last = statement;
        for (ClusterController cc : annotations(element, ClusterController.class)) {
            last = new Bracket(last) {
                @Override
                protected void before() {
                    // Here we record the state of the cluster
                    LOGGER.info("Creating cluster controller {} before test per @ClusterController annotation on {}", cc, name(element));
                    kubeClient().clientWithAdmin().create(CC_INSTALL_PATH);
                    kubeClient().waitForDeployment(CC_DEPLOYMENT_NAME);
                }


                @Override
                protected void after() {
                    LOGGER.info("Deleting cluster controller {} after test per @ClusterController annotation on {}", cc, name(element));
                    // Here we verify the cluster is in the same state
                    kubeClient().clientWithAdmin().delete(CC_INSTALL_PATH);
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
            statement = withNamespaces(testClass, statement);
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
}
