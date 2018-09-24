/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.KubeClient;
import io.strimzi.test.k8s.HelmClient;
import io.strimzi.test.k8s.OpenShift;
import io.strimzi.test.k8s.Minishift;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Stack;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.strimzi.test.TestUtils.indent;
import static io.strimzi.test.TestUtils.entriesToMap;
import static io.strimzi.test.TestUtils.entry;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class StrimziExtension implements AfterAllCallback, BeforeAllCallback, AfterEachCallback, BeforeEachCallback,
        ExecutionCondition, AfterTestExecutionCallback, BeforeTestExecutionCallback {
    private static final Logger LOGGER = LogManager.getLogger(StrimziExtension.class);

    public static final String KAFKA_PERSISTENT_YAML = "../examples/kafka/kafka-persistent.yaml";
    public static final String KAFKA_CONNECT_YAML = "../examples/kafka-connect/kafka-connect.yaml";
    public static final String KAFKA_CONNECT_S2I_CM = "../examples/configmaps/cluster-operator/kafka-connect-s2i.yaml";
    public static final String CO_INSTALL_DIR = "../examples/install/cluster-operator";
    public static final String CO_DEPLOYMENT_NAME = "strimzi-cluster-operator";
    public static final String TOPIC_CM = "../examples/topic/kafka-topic.yaml";
    public static final String HELM_CHART = "../helm-charts/strimzi-kafka-operator/";
    public static final String HELM_RELEASE_NAME = "strimzi-systemtests";
    public static final String STRIMZI_ORG = "strimzi";
    public static final String STRIMZI_TAG = "latest";
    public static final String IMAGE_PULL_POLICY = "Always";
    public static final String REQUESTS_MEMORY = "512Mi";
    public static final String REQUESTS_CPU = "200m";
    public static final String LIMITS_MEMORY = "512Mi";
    public static final String LIMITS_CPU = "1000m";
    public static final String OPERATOR_LOG_LEVEL = "INFO";
    public static final String NOTEARDOWN = "NOTEARDOWN";
    private static final String DEFAULT_TAG = "all";
    private static final String TAG_LIST_NAME = "tags";
    private static final String START_TIME = "start time";

    private static long startTime;

    private KubeClusterResource clusterResource;
    private Class testClass;
    private Statement classStatement;
    private Statement methodStatement;

    public StrimziExtension() {
    }

    @Override
    public void afterAll(ExtensionContext context) {
        deleteResource((Bracket) classStatement);
    }

    @Override
    public void beforeAll(ExtensionContext context) {
        testClass = context.getTestClass().orElse(null);

        if (!areAllChildrenIgnored()) {
            classStatement = new Bracket(null, () -> e -> {
                LOGGER.info("Failed to set up test class {}, due to {}", testClass.getName(), e, e);
            }) {
                @Override
                protected void before() {
                }

                @Override
                protected void after() {
                }
            };
            classStatement = withClusterOperator(testClass, classStatement);
            classStatement = withResources(testClass, classStatement);
            classStatement = withNamespaces(testClass, classStatement);
            classStatement = withLogging(testClass, classStatement);
            try {
                Bracket current = (Bracket) classStatement;
                while (current != null) {
                    current.before();
                    current = (Bracket) current.statement;
                }
            } catch (Throwable throwable) {
                throwable.printStackTrace();
            }
        }
    }

    @Override
    public void afterEach(ExtensionContext context) {
        deleteResource((Bracket) methodStatement);
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        Method testMethod = context.getTestMethod().get();

        methodStatement = new Bracket(null, () -> e -> {
            LOGGER.info("Failed to set up test class {}, due to {}", testClass.getName(), e, e);
        }) {
            @Override
            protected void before() {
            }

            @Override
            protected void after() {
            }
        };

        methodStatement = withClusterOperator(testMethod, methodStatement);
        methodStatement = withResources(testMethod, methodStatement);
        methodStatement = withNamespaces(testMethod, methodStatement);
        methodStatement = withLogging(testMethod, methodStatement);
        try {
            Bracket current = (Bracket) methodStatement;
            while (current != null) {
                current.before();
                current = (Bracket) current.statement;
            }
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        if (!isIgnoredByTag(context)) {
            return ConditionEvaluationResult.enabled("Test method is enabled");
        }
        return ConditionEvaluationResult.disabled("Test method is disabled");
    }

    private void deleteResource(Bracket resource) {
        if (resource != null) {
            deleteResource((Bracket) resource.statement);
            resource.after();
        }
    }

    @Override
    public void afterTestExecution(ExtensionContext extensionContext) throws Exception {
//        Method testMethod = extensionContext.getRequiredTestMethod();
//        long startTime = getStore(extensionContext).remove(START_TIME, long.class);
//        long duration = System.currentTimeMillis() - startTime;
//
//        LOGGER.info(() -> String.format("Method [%s] took %s ms.", testMethod.getName(), duration));
    }

    @Override
    public void beforeTestExecution(ExtensionContext extensionContext) throws Exception {
        startTime = System.currentTimeMillis();
    }

    public static int getRuntimeInSeconds() {
        return (int) (System.currentTimeMillis() - startTime * 1000);
    }


    abstract class Bracket extends Statement implements Runnable {
        public final Statement statement;
        private final Thread hook = new Thread(this);
        private final Supplier<Consumer<? super Throwable>> onError;

        public Bracket(Statement statement, Supplier<Consumer<? super Throwable>> onError) {
            this.statement = statement;
            this.onError = onError;
        }

        @Override
        public void evaluate() throws Throwable {
            // All this fuss just to ensure that the first thrown exception is what propagates
            Throwable thrown = null;
            try {
                Runtime.getRuntime().addShutdownHook(hook);
                before();
                statement.evaluate();
            } catch (Throwable e) {
                thrown = e;
                if (onError != null) {
                    try {
                        onError.get().accept(e);
                    } catch (Throwable t) {
                        thrown.addSuppressed(t);
                    }
                }
            } finally {
                try {
                    Runtime.getRuntime().removeShutdownHook(hook);
                    runAfter();
                } catch (Throwable e) {
                    if (thrown != null) {
                        thrown.addSuppressed(e);
                        throw thrown;
                    } else {
                        thrown = e;
                    }
                }
                if (thrown != null) {
                    throw thrown;
                }
            }
        }

        /**
         * Runs before the test
         */
        protected abstract void before();

        /**
         * Runs after the test, even it if failed or the JVM can killed
         */
        protected abstract void after();

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

    private String name(AnnotatedElement a) {
        if (a instanceof Class) {
            return "class " + ((Class) a).getSimpleName();
        } else if (a instanceof Method) {
            Method method = (Method) a;
            return "method " + method.getDeclaringClass().getSimpleName() + "." + method.getName() + "()";
        } else if (a instanceof Field) {
            Field field = (Field) a;
            return "field " + field.getDeclaringClass().getSimpleName() + "." + field.getName();
        } else {
            return a.toString();
        }
    }

    private boolean areAllChildrenIgnored() {
        for (Method method : testClass.getDeclaredMethods()) {
            if (!isWrongClusterType(method)) {
                return false;
            }
        }
        return true;
    }

    private boolean isWrongClusterType(AnnotatedElement element) {
        boolean result = element.getAnnotation(OpenShiftOnly.class) != null
                && !(clusterResource().cluster() instanceof OpenShift
                || clusterResource().cluster() instanceof Minishift);
        if (result) {
            LOGGER.info("{} is @OpenShiftOnly, but the running cluster is not OpenShift: Ignoring {}",
                    name(testClass),
                    name(element)
            );
        }
        return result;
    }

    private boolean isIgnoredByTag(ExtensionContext context) {
        Collection<String> declaredTags = context.getTags();
        Collection<String> enabledTags = getEnabledTags();
        if (declaredTags.isEmpty()) {
            LOGGER.info("Test method {} does not have any tag restrictions", context.getDisplayName());
            return false;
        }
        if (isTagEnabled(declaredTags, enabledTags)) {
            LOGGER.info("One of the test group {} is enabled for test: {}", enabledTags, context.getDisplayName());
            return false;
        }
        LOGGER.info("None of the test group {} are enabled for test: {}", enabledTags, context.getDisplayName());
        return true;
    }

    /**
     * Get the value of the @ClassRule-annotated KubeClusterResource field
     */
    private KubeClusterResource clusterResource() {
        if (clusterResource == null) {
            try {
                Field field = testClass.getField("cluster");
                clusterResource = (KubeClusterResource) field.get(KubeClusterResource.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                e.printStackTrace();
            }

            if (clusterResource == null) {
                clusterResource = new KubeClusterResource();
                clusterResource.before();
            }
        }
        return clusterResource;
    }

    private static Collection<String> getEnabledTags() {
        return splitProperties((String) System.getProperties().getOrDefault(TAG_LIST_NAME, DEFAULT_TAG));
    }

    private static Collection<String> splitProperties(String commaSeparated) {
        if (commaSeparated == null || commaSeparated.trim().isEmpty()) {
            return Collections.emptySet();
        }
        return new HashSet<>(Arrays.asList(commaSeparated.split(",+")));
    }

    private static boolean isTagEnabled(Collection<String> declaredTags, Collection<String> enabledTags) {
        if (enabledTags.contains(DEFAULT_TAG) || enabledTags.isEmpty()) {
            return true;
        }
        for (String enabledTag : enabledTags) {
            if (declaredTags.contains(enabledTag)) {
                return true;
            }
        }
        return false;
    }

    protected KubeClient<?> kubeClient() {
        return clusterResource().client();
    }

    protected HelmClient helmClient() {
        return clusterResource().helmClient();
    }

    private String getContent(File file, Consumer<JsonNode> edit) {
        YAMLMapper mapper = new YAMLMapper();
        try {
            JsonNode node = mapper.readTree(file);
            edit.accept(node);
            return mapper.writeValueAsString(node);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    class ResourceAction<T extends ResourceAction<T>> implements Supplier<Consumer<Throwable>> {

        protected List<Consumer<Throwable>> list = new ArrayList<>();

        public ResourceAction getResources(ResourceMatcher resources) {
            list.add(new DescribeErrorAction(resources));
            return this;
        }

        public ResourceAction getResources(String kind, String pattern) {
            return getResources(new ResourceMatcher(kind, pattern));
        }

        public ResourceAction getPo() {
            return getPo(".*");
        }

        public ResourceAction getPo(String pattern) {
            return getResources(new ResourceMatcher("pod", pattern));
        }

        public ResourceAction getDep() {
            return getDep(".*");
        }

        public ResourceAction getDep(String pattern) {
            return getResources(new ResourceMatcher("deployment", pattern));
        }

        public ResourceAction getSs() {
            return getSs(".*");
        }

        public ResourceAction getSs(String pattern) {
            return getResources(new ResourceMatcher("statefulset", pattern));
        }

        public ResourceAction logs(String pattern, String container) {
            list.add(new DumpLogsErrorAction(new ResourceMatcher("pod", pattern), container));
            return this;
        }

        /**
         * Gets a result.
         *
         * @return a result
         */
        @Override
        public Consumer<Throwable> get() {
            return t -> {
                for (Consumer<Throwable> x : list) {
                    x.accept(t);
                }
            };
        }
    }

    class ResourceName {
        public final String kind;
        public final String name;

        public ResourceName(String kind, String name) {
            this.kind = kind;
            this.name = name;
        }
    }

    class ResourceMatcher implements Supplier<List<ResourceName>> {
        public final String kind;
        public final String namePattern;

        public ResourceMatcher(String kind, String namePattern) {
            this.kind = kind;
            this.namePattern = namePattern;
        }

        @Override
        public List<ResourceName> get() {
            return kubeClient().list(kind).stream()
                    .filter(name -> name.matches(namePattern))
                    .map(name -> new ResourceName(kind, name))
                    .collect(Collectors.toList());
        }
    }

    class DumpLogsErrorAction implements Consumer<Throwable> {

        private final Supplier<List<ResourceName>> podNameSupplier;
        private final String container;

        public DumpLogsErrorAction(Supplier<List<ResourceName>> podNameSupplier, String container) {
            this.podNameSupplier = podNameSupplier;
            this.container = container;
        }

        @Override
        public void accept(Throwable t) {
            for (ResourceName pod : podNameSupplier.get()) {
                if (pod.kind.equals("pod") || pod.kind.equals("pods") || pod.kind.equals("po")) {
                    LOGGER.info("Logs from pod {}:{}{}", pod.name, System.lineSeparator(), indent(kubeClient().logs(pod.name, container)));
                }
            }
        }
    }

    class DescribeErrorAction implements Consumer<Throwable> {

        private final Supplier<List<ResourceName>> resources;

        public DescribeErrorAction(Supplier<List<ResourceName>> resources) {
            this.resources = resources;
        }

        @Override
        public void accept(Throwable t) {
            for (ResourceName resource : resources.get()) {
                LOGGER.info("Description of {} '{}':{}{}", resource.kind, resource.name,
                        System.lineSeparator(), indent(kubeClient().getResourceAsYaml(resource.kind, resource.name)));
            }
        }
    }

    /**
     * Get the (possibly @Repeatable) annotations on the given element.
     *
     * @param annotatedElement
     * @param annotationType
     * @param <A>
     * @return
     */
    @SuppressWarnings("unchecked")
    <A extends Annotation> List<A> annotations(AnnotatedElement annotatedElement, Class<A> annotationType) {
        final List<A> list;
        A annotation = annotatedElement.getAnnotation(annotationType);

        if (annotation != null) {
            list = singletonList(annotation);
        } else {
            A[] annotations = annotatedElement.getAnnotationsByType(annotationType);
            if (annotations != null) {
                list = asList(annotations);
            } else {
                list = emptyList();
            }

        }
//        } else {
//            Repeatable repeatable = annotationType.getAnnotation(Repeatable.class);
//            Annotation[] x = annotatedElement.getAnnotationsByType(annotationType);
//            System.out.println(Arrays.toString(x));
//            if (repeatable != null) {
//                Class<? extends Annotation> repeatableAnnotation = repeatable.value();
//                Annotation container = repeatableAnnotation.getAnnotation(annotationType);
//                if (container != null) {
//                    try {
//                        Method value = repeatableAnnotation.getDeclaredMethod("value");
//                        list = asList((A[]) value.invoke(container));
//                    } catch (ReflectiveOperationException e) {
//                        throw new RuntimeException(e);
//                    }
//                } else {
//                    list = emptyList();
//                }
//            } else {
//                list = emptyList();
//            }
//        }

        return list;
    }

    private Statement withClusterOperator(AnnotatedElement element,
                                          Statement statement) {
        Statement last = statement;
        for (ClusterOperator cc : annotations(element, ClusterOperator.class)) {
            boolean useHelmChart = cc.useHelmChart() || Boolean.parseBoolean(System.getProperty("useHelmChart", Boolean.FALSE.toString()));
            if (useHelmChart) {
                last = installOperatorFromHelmChart(element, last, cc);
            } else {
                last = installOperatorFromExamples(element, last, cc);
            }
        }
        return last;
    }

    private Statement withNamespaces(AnnotatedElement element,
                                     Statement statement) {
        Statement last = statement;
        for (Namespace namespace : annotations(element, Namespace.class)) {
            last = new Bracket(last, null) {
                String previousNamespace = null;

                @Override
                protected void before() {
                    LOGGER.info("Creating namespace '{}' before test per @Namespace annotation on {}", namespace.value(), name(element));
                    kubeClient().createNamespace(namespace.value());
                    previousNamespace = namespace.use() ? kubeClient().namespace(namespace.value()) : kubeClient().namespace();
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

    @SuppressWarnings("unchecked")
    private Statement installOperatorFromExamples(AnnotatedElement element, Statement last, ClusterOperator cc) {
        Map<File, String> yamls = Arrays.stream(new File(CO_INSTALL_DIR).listFiles()).sorted().collect(Collectors.toMap(file -> file, f -> getContent(f, node -> {
            // Change the docker org of the images in the 04-deployment.yaml
            if ("050-Deployment-strimzi-cluster-operator.yaml".equals(f.getName())) {
                ObjectNode containerNode = (ObjectNode) node.get("spec").get("template").get("spec").get("containers").get(0);
                containerNode.put("imagePullPolicy", IMAGE_PULL_POLICY);
                JsonNodeFactory factory = new JsonNodeFactory(false);
                ObjectNode resources = new ObjectNode(factory);
                ObjectNode requests = new ObjectNode(factory);
                requests.put("cpu", "200m").put(REQUESTS_CPU, REQUESTS_MEMORY);
                ObjectNode limits = new ObjectNode(factory);
                limits.put("cpu", "1000m").put(LIMITS_CPU, LIMITS_MEMORY);
                resources.set("requests", requests);
                resources.set("limits", limits);
                containerNode.replace("resources", resources);
                containerNode.remove("resources");
                JsonNode ccImageNode = containerNode.get("image");
                ((ObjectNode) containerNode).put("image", TestUtils.changeOrgAndTag(ccImageNode.asText()));
                for (JsonNode envVar : containerNode.get("env")) {
                    String varName = envVar.get("name").textValue();
                    // Replace all the default images with ones from the $DOCKER_ORG org and with the $DOCKER_TAG tag
                    if (varName.matches("STRIMZI_DEFAULT_.*_IMAGE")) {
                        String value = envVar.get("value").textValue();
                        ((ObjectNode) envVar).put("value", TestUtils.changeOrgAndTag(value));
                    }
                    // Set log level
                    if (varName.equals("STRIMZI_LOG_LEVEL")) {
                        String logLevel = System.getenv().getOrDefault("TEST_STRIMZI_LOG_LEVEL", OPERATOR_LOG_LEVEL);
                        ((ObjectNode) envVar).put("value", logLevel);
                    }
                    // Updates default values of env variables
                    for (EnvVariables envVariable : cc.envVariables()) {
                        if (varName.equals(envVariable.key())) {
                            ((ObjectNode) envVar).put("value", envVariable.value());
                        }
                    }
                }
            }

            if (f.getName().matches(".*RoleBinding.*")) {
                String ns = annotations(element, Namespace.class).get(0).value();
                ArrayNode subjects = (ArrayNode) node.get("subjects");
                ObjectNode subject = (ObjectNode) subjects.get(0);
                subject.put("kind", "ServiceAccount")
                        .put("name", "strimzi-cluster-operator")
                        .put("namespace", ns);
//                LOGGER.info("Modified binding from {}: {}", f, node);
            }
        }), (x, y) -> x, LinkedHashMap::new));
        last = new Bracket(last, new ResourceAction().getPo(CO_DEPLOYMENT_NAME + ".*")
                .logs(CO_DEPLOYMENT_NAME + ".*", "strimzi-cluster-operator")
                .getDep(CO_DEPLOYMENT_NAME)) {
            Stack<String> deletable = new Stack<>();

            @Override
            protected void before() {
                // Here we record the state of the cluster
                LOGGER.info("Creating cluster operator {} before test per @ClusterOperator annotation on {}", cc, name(element));
                for (Map.Entry<File, String> entry : yamls.entrySet()) {
                    LOGGER.info("creating possibly modified version of {}", entry.getKey());
                    deletable.push(entry.getValue());
                    kubeClient().namespace(annotations(element, Namespace.class).get(0).value());
                    kubeClient().clientWithAdmin().applyContent(entry.getValue());
                }
                kubeClient().waitForDeployment(CO_DEPLOYMENT_NAME, 1);
            }

            @Override
            protected void after() {
                LOGGER.info("Deleting cluster operator {} after test per @ClusterOperator annotation on {}", cc, name(element));
                while (!deletable.isEmpty()) {
                    kubeClient().clientWithAdmin().deleteContent(deletable.pop());
                }
                kubeClient().waitForResourceDeletion("deployment", CO_DEPLOYMENT_NAME);
            }
        };
        return last;
    }

    @SuppressWarnings("unchecked")
    private Statement installOperatorFromHelmChart(AnnotatedElement element, Statement last, ClusterOperator cc) {
        String dockerOrg = System.getenv().getOrDefault("DOCKER_ORG", STRIMZI_ORG);
        String dockerTag = System.getenv().getOrDefault("DOCKER_TAG", STRIMZI_TAG);

        Map<String, String> values = Collections.unmodifiableMap(Stream.of(
                entry("imageRepositoryOverride", dockerOrg),
                entry("imageTagOverride", dockerTag),
                entry("image.pullPolicy", IMAGE_PULL_POLICY),
                entry("resources.requests.memory", REQUESTS_MEMORY),
                entry("resources.requests.cpu", REQUESTS_CPU),
                entry("resources.limits.memory", LIMITS_MEMORY),
                entry("resources.limits.cpu", LIMITS_CPU),
                entry("logLevel", OPERATOR_LOG_LEVEL))
                .collect(entriesToMap()));

        /** These entries aren't applied to the deployment yaml at this time */
        Map<String, String> envVars = Collections.unmodifiableMap(Arrays.stream(cc.envVariables())
                .map(var -> entry(String.format("env.%s", var.key()), var.value()))
                .collect(entriesToMap()));

        Map<String, String> allValues = Stream.of(values, envVars).flatMap(m -> m.entrySet().stream())
                .collect(entriesToMap());

        last = new Bracket(last, new ResourceAction().getPo(CO_DEPLOYMENT_NAME + ".*")
                .logs(CO_DEPLOYMENT_NAME + ".*", null)
                .getDep(CO_DEPLOYMENT_NAME)) {
            @Override
            protected void before() {
                // Here we record the state of the cluster
                LOGGER.info("Creating cluster operator with Helm Chart {} before test per @ClusterOperator annotation on {}", cc, name(element));
                Path pathToChart = new File(HELM_CHART).toPath();
                String oldNamespace = kubeClient().namespace("kube-system");
                String pathToHelmServiceAccount = Objects.requireNonNull(getClass().getClassLoader().getResource("helm/helm-service-account.yaml")).getPath();
                String helmServiceAccount = TestUtils.getFileAsString(pathToHelmServiceAccount);
                kubeClient().applyContent(helmServiceAccount);
                helmClient().init();
                kubeClient().namespace(oldNamespace);
                helmClient().install(pathToChart, HELM_RELEASE_NAME, allValues);
            }

            @Override
            protected void after() {
                LOGGER.info("Deleting cluster operator with Helm Chart {} after test per @ClusterOperator annotation on {}", cc, name(element));
                helmClient().delete(HELM_RELEASE_NAME);
            }
        };
        return last;
    }

    private Statement withResources(AnnotatedElement element,
                                    Statement statement) {
        Statement last = statement;
        for (Resources resources : annotations(element, Resources.class)) {
            last = new Bracket(last, null) {
                @Override
                protected void before() {
                    // Here we record the state of the cluster
                    LOGGER.info("Creating resources {}, before test per @Resources annotation on {}", Arrays.toString(resources.value()), name(element));
                    kubeClient().create(resources.value());
                }

                private KubeClient kubeClient() {
                    KubeClient client = StrimziExtension.this.kubeClient();
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

    private Statement withLogging(AnnotatedElement element, Statement statement) {
        return new Bracket(statement, null) {
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
