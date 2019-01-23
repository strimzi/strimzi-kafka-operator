/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import io.strimzi.test.TestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.lang.String.join;
import static java.util.Arrays.asList;

public abstract class BaseKubeClient<K extends BaseKubeClient<K>> implements KubeClient<K> {

    private static final Logger LOGGER = LogManager.getLogger(BaseKubeClient.class);

    public static final String CREATE = "create";
    public static final String APPLY = "apply";
    public static final String DELETE = "delete";
    public static final String DEPLOYMENT = "deployment";
    public static final String STATEFUL_SET = "statefulset";
    public static final String SERVICE = "service";
    public static final String CM = "cm";

    private final String context;
    private String namespace;

    BaseKubeClient(String context, String namespace) {
        this.context = context;
        this.namespace = namespace;
    }

    protected abstract String cmd();

    @Override
    public KubeClient<K> deleteByName(String resourceType, String resourceName) {
        Exec.exec(cmdWithContext(DELETE, resourceType, resourceName));
        return this;
    }

    @Override
    public void namespace(String namespace) {
        if (namespace == null) {
            throw new NullPointerException("namespace may not be null");
        }
        this.namespace = namespace;
    }

    @Override
    public String namespace() {
        return namespace;
    }

    @Override
    public final KubeClient<K> clientWithAdmin() {
        String adminCtxName = System.getenv(KubeCluster.ENV_VAR_TEST_CLUSTER_ADMIN);
        if (adminCtxName != null) {
            return clientWithContext(adminCtxName);
        } else {
            return this;
        }
    }

    /**
     * Create a KubeClient of the same type, with a different context name
     * @param ctxName The name of the context to use
     * @return A KubeClient
     */
    abstract KubeClient<K> clientWithContext(String ctxName);

    @Override
    public boolean clientAvailable() {
        return Exec.isExecutableOnPath(cmd());
    }

    public List<String> cmdWithContext(String... params) {
        List<String> result = new ArrayList<>();
        result.add(cmd());
        result.add("--namespace");
        result.add(namespace());
        if (context != null) {
            result.add("--context");
            result.add(context);
        }
        addAll(result, params);
        return result;
    }

    private static void addAll(List<String> dest, String... src) {
        for (String param : src) {
            dest.add(param);
        }
    }

    @Override
    public String get(String resource, String resourceName) {
        return Exec.exec(cmdWithContext("get", resource, resourceName, "-o", "yaml")).out();
    }

    @Override
    public String getEvents() {
        return Exec.exec(cmdWithContext("get", "events")).out();
    }

    @Override
    public KubeClient<K> create(File... files) {
        KubeClusterException error = execRecursive(CREATE, files, Comparator.comparing(File::getName));
        if (error != null) {
            throw error;
        }
        return this;
    }

    @Override
    public KubeClient<K> apply(File... files) {
        KubeClusterException error = execRecursive(APPLY, files, Comparator.comparing(File::getName));
        if (error != null) {
            throw error;
        }
        return this;
    }

    @Override
    public KubeClient<K> delete(File... files) {
        KubeClusterException error = execRecursive(DELETE, files, Comparator.comparing(File::getName).reversed());
        if (error != null) {
            throw error;
        }
        return this;
    }

    private KubeClusterException execRecursive(String subcommand, File[] files, Comparator<File> cmp) {
        KubeClusterException error = null;
        for (File f : files) {
            if (f.isFile()) {
                if (f.getName().endsWith(".yaml")) {
                    try {
                        Exec.exec(cmdWithContext(subcommand, "-f", f.getAbsolutePath()));
                    } catch (KubeClusterException e) {
                        if (error == null) {
                            error = e;
                        }
                    }
                }
            } else if (f.isDirectory()) {
                File[] children = f.listFiles();
                if (children != null) {
                    Arrays.sort(children, cmp);
                    KubeClusterException e = execRecursive(subcommand, children, cmp);
                    if (error == null) {
                        error = e;
                    }
                }
            } else if (!f.exists()) {
                throw new RuntimeException(new NoSuchFileException(f.getPath()));
            }
        }
        return error;
    }

    @Override
    public KubeClient<K> replace(File... files) {
        execRecursive("replace", files, (f1, f2) -> f1.getName().compareTo(f2.getName()));
        return this;
    }

    @Override
    public KubeClient<K> applyContent(String yamlContent) {
        Exec.exec(yamlContent, cmdWithContext(APPLY, "-f", "-"));
        return this;
    }

    @Override
    public KubeClient<K> deleteContent(String yamlContent) {
        Exec.exec(yamlContent, cmdWithContext(DELETE, "-f", "-"));
        return this;
    }

    @Override
    public KubeClient<K> createNamespace(String name) {
        Exec.exec(clientWithAdmin().cmdWithContext(CREATE, "namespace", name));
        return this;
    }

    @Override
    public KubeClient<K> deleteNamespace(String name) {
        Exec.exec(clientWithAdmin().cmdWithContext(DELETE, "namespace", name));
        return this;
    }

    @Override
    public ProcessResult execInPod(String pod, String... command) {
        List<String> cmd = cmdWithContext("exec", pod, "--");
        addAll(cmd, command);
        return Exec.exec(cmd);
    }

    @Override
    public ProcessResult execInPodContainer(String pod, String container, String... command) {
        List<String> cmd = cmdWithContext("exec", pod, "-c", container, "--");
        addAll(cmd, command);
        return Exec.exec(cmd);
    }

    @Override
    public ProcessResult exec(String... command) {
        return Exec.exec(asList(command));
    }

    private KubeClient<K> waitFor(String resource, String name, Predicate<JsonNode> ready) {
        long timeoutMs = 570_000L;
        long pollMs = 1_000L;
        ObjectMapper mapper = new ObjectMapper();
        TestUtils.waitFor(resource + " " + name, pollMs, timeoutMs, () -> {
            try {
                String jsonString = Exec.exec(cmdWithContext("get", resource, name, "-o", "json")).out();
                LOGGER.trace("{}", jsonString);
                JsonNode actualObj = mapper.readTree(jsonString);
                return ready.test(actualObj);
            } catch (KubeClusterException.NotFound e) {
                return false;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return this;
    }

    @Override
    public KubeClient<K> waitForDeployment(String name, int expected) {
        return waitFor("deployment", name, actualObj -> {
            JsonNode replicasNode = actualObj.get("status").get("replicas");
            JsonNode readyReplicasName = actualObj.get("status").get("readyReplicas");
            return replicasNode != null && readyReplicasName != null
                    && replicasNode.asInt() == readyReplicasName.asInt() && replicasNode.asInt() == expected;

        });
    }

    @Override
    public KubeClient<K> waitForDeploymentConfig(String name) {
        return waitFor("deploymentConfig", name, actualObj -> {
            JsonNode replicasNode = actualObj.get("status").get("replicas");
            JsonNode readyReplicasName = actualObj.get("status").get("readyReplicas");
            return replicasNode != null && readyReplicasName != null
                    && replicasNode.asInt() == readyReplicasName.asInt();
        });
    }

    @Override
    public KubeClient<K> waitForPod(String name) {
        // wait when all pods are ready
        return waitFor("pod", name,
            actualObj -> {
                JsonNode containerStatuses = actualObj.get("status").get("containerStatuses");
                if (containerStatuses != null && containerStatuses.isArray()) {
                    for (final JsonNode objNode : containerStatuses) {
                        if (!objNode.get("ready").asBoolean()) {
                            return false;
                        }
                    }
                    return true;
                }
                return false;
            }
        );
    }

    @Override
    public KubeClient<K> waitForStatefulSet(String name, int expectPods) {
        return waitFor("statefulset", name,
            actualObj -> {
                int rep = actualObj.get("status").get("replicas").asInt();
                JsonNode currentReplicas = actualObj.get("status").get("currentReplicas");

                if (currentReplicas != null &&
                        ((expectPods >= 0 && expectPods == currentReplicas.asInt() && expectPods == rep)
                        || (expectPods < 0 && rep == currentReplicas.asInt()))) {
                    LOGGER.debug("Waiting for pods of statefulset {}", name);
                    if (expectPods >= 0) {
                        for (int ii = 0; ii < expectPods; ii++) {
                            waitForPod(name + "-" + ii);
                        }
                    }
                    return true;
                }
                return false;
            });
    }

    @Override
    public KubeClient<K> waitForResourceCreation(String resourceType, String resourceName) {
        // wait when resource to be created
        return waitFor(resourceType, resourceName,
            actualObj -> true
        );
    }

    @Override
    public KubeClient<K> waitForResourceDeletion(String resourceType, String resourceName) {
        TestUtils.waitFor(resourceType + " " + resourceName + " removal",
            1_000L, 480_000L, () -> {
                try {
                    get(resourceType, resourceName);
                    return false;
                } catch (KubeClusterException.NotFound e) {
                    return true;
                }
            });
        return this;
    }

    @Override
    public KubeClient<K> waitForResourceUpdate(String resourceType, String resourceName, Date startTime) {

        TestUtils.waitFor(resourceType + " " + resourceName + " update",
                1_000L, 240_000L, () -> {
                try {
                    return startTime.before(getResourceCreateTimestamp(resourceType, resourceName));
                } catch (KubeClusterException.NotFound e) {
                    return false;
                }
            });
        return this;
    }

    @Override
    public Date getResourceCreateTimestamp(String resourceType, String resourceName) {
        DateFormat df = new SimpleDateFormat("yyyyMMdd'T'kkmmss'Z'");
        Date parsedDate = null;
        try {
            parsedDate = df.parse(JsonPath.parse(getResourceAsJson(resourceType, resourceName)).
                    read("$.metadata.creationTimestamp").toString().replaceAll("\\p{P}", ""));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return parsedDate;
    }

    @Override
    public String toString() {
        return cmd();
    }

    @Override
    public List<String> list(String resourceType) {
        return asList(Exec.exec(cmdWithContext("get", resourceType, "-o", "jsonpath={range .items[*]}{.metadata.name} ")).out().trim().split(" +")).stream().filter(s -> !s.trim().isEmpty()).collect(Collectors.toList());
    }

    @Override
    public String getResourceAsJson(String resourceType, String resourceName) {
        return Exec.exec(cmdWithContext("get", resourceType, resourceName, "-o", "json")).out();
    }

    @Override
    public String getResourceAsYaml(String resourceType, String resourceName) {
        return Exec.exec(cmdWithContext("get", resourceType, resourceName, "-o", "yaml")).out();
    }

    @Override
    public String logs(String pod, String container) {
        String[] args;
        if (container != null) {
            args = new String[]{"logs", pod, "-c", container};
        } else {
            args = new String[]{"logs", pod};
        }
        return Exec.exec(cmdWithContext(args)).out();
    }

    @Override
    public String searchInLog(String resourceType, String resourceName, long sinceSeconds, String... grepPattern) {
        try {
            return Exec.exec("bash", "-c", join(" ", cmdWithContext("logs", resourceType + "/" + resourceName, "--since=" + String.valueOf(sinceSeconds) + "s",
                    "|", "grep", " -e " + join(" -e ", grepPattern)))).out();
        } catch (KubeClusterException e) {
            if (e.result != null && e.result.exitStatus() == 1) {
                LOGGER.info("{} not found", grepPattern);
            } else {
                LOGGER.error("Caught exception while searching {} in logs", grepPattern);
            }
        }
        return "";
    }

    public List<String> listResourcesByLabel(String resourceType, String label) {
        return asList(Exec.exec(cmdWithContext("get", resourceType, "-l", label, "-o", "jsonpath={range .items[*]}{.metadata.name} ")).out().split("\\s+"));
    }
}
