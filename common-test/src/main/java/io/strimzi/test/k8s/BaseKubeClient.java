/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.strimzi.test.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;

import static java.util.Arrays.asList;

public abstract class BaseKubeClient<K extends BaseKubeClient<K>> implements KubeClient<K> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseKubeClient.class);

    public static final String CREATE = "create";
    public static final String DELETE = "delete";
    private String namespace = defaultNamespace();

    protected abstract String cmd();

    public K deleteByName(String resourceType, String resourceName) {
        Exec.exec(cmd(), DELETE, resourceType, resourceName);
        return (K) this;
    }

    protected static class Context implements AutoCloseable {
        @Override
        public void close() {

        }
    }

    private static final Context NOOP = new Context();

    @Override
    public String namespace(String namespace) {
        String previous = this.namespace;
        this.namespace = namespace;
        return previous;
    }

    public String namespace() {
        return namespace;
    }

    @Override
    public abstract K clientWithAdmin();

    protected Context defaultContext() {
        return NOOP;
    }

    protected Context adminContext() {
        return defaultContext();
    }

    @Override
    public boolean clientAvailable() {
        return Exec.isExecutableOnPath(cmd());
    }

    protected List<String> namespacedCommand(String... rest) {
        return namespacedCommand(asList(rest));
    }

    private List<String> namespacedCommand(List<String> rest) {
        List<String> result = new ArrayList<>();
        result.add(cmd());
        result.add("--namespace");
        result.add(namespace());
        result.addAll(rest);
        return result;
    }

    @Override
    public String get(String resource, String resourceName) {
        return Exec.exec(namespacedCommand("get", resource, resourceName, "-o", "yaml")).out();
    }

    @Override
    public K create(File... files) {
        try (Context context = defaultContext()) {
            KubeClusterException error = execRecursive(CREATE, files, (f1, f2) -> f1.getName().compareTo(f2.getName()));
            if (error != null) {
                throw error;
            }
            return (K) this;
        }
    }

    @Override
    public K delete(File... files) {
        try (Context context = defaultContext()) {
            KubeClusterException error = execRecursive(DELETE, files, (f1, f2) -> f2.getName().compareTo(f1.getName()));
            if (error != null) {
                throw error;
            }
            return (K) this;
        }
    }

    private KubeClusterException execRecursive(String subcommand, File[] files, Comparator<File> cmp) {
        KubeClusterException error = null;
        for (File f : files) {
            if (f.isFile()) {
                if (f.getName().endsWith(".yaml")) {
                    try {
                        Exec.exec(namespacedCommand(subcommand, "-f", f.getAbsolutePath()));
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
    public K replace(File... files) {
        try (Context context = defaultContext()) {
            execRecursive("replace", files, (f1, f2) -> f1.getName().compareTo(f2.getName()));
            return (K) this;
        }
    }

    @Override
    public K replaceContent(String yamlContent) {
        try (Context context = defaultContext()) {
            Exec.exec(yamlContent, namespacedCommand("replace", "-f", "-"));
            return (K) this;
        }
    }

    @Override
    public K createContent(String yamlContent) {
        try (Context context = defaultContext()) {
            Exec.exec(yamlContent, namespacedCommand(CREATE, "-f", "-"));
            return (K) this;
        }
    }

    @Override
    public K deleteContent(String yamlContent) {
        try (Context context = defaultContext()) {
            Exec.exec(yamlContent, namespacedCommand(DELETE, "-f", "-"));
            return (K) this;
        }
    }

    @Override
    public K createNamespace(String name) {
        try (Context context = adminContext()) {
            Exec.exec(namespacedCommand(CREATE, "namespace", name));
        }
        return (K) this;
    }

    @Override
    public K deleteNamespace(String name) {
        try (Context context = adminContext()) {
            Exec.exec(namespacedCommand(DELETE, "namespace", name));
        }
        return (K) this;
    }

    @Override
    public ProcessResult exec(String pod, String... command) {
        List<String> cmd = namespacedCommand("exec", pod, "--");
        cmd.addAll(asList(command));
        return Exec.exec(cmd);
    }

    enum ExType {
        BREAK,
        CONTINUE,
        THROW
    }

    private K waitFor(String resource, String name, Predicate<JsonNode> ready) {
        long timeoutMs = 600_000L;
        long pollMs = 1_000L;
        ObjectMapper mapper = new ObjectMapper();
        TestUtils.waitFor(resource + " " + name, pollMs, timeoutMs, () -> {
            try {
                String jsonString = Exec.exec(namespacedCommand("get", resource, name, "-o", "json")).out();
                LOGGER.trace("{}", jsonString);
                JsonNode actualObj = mapper.readTree(jsonString);
                return ready.test(actualObj);
            } catch (KubeClusterException.NotFound e) {
                return false;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return (K) this;
    }

    @Override
    public K waitForDeployment(String name) {
        return waitFor("deployment", name, actualObj -> {
            JsonNode replicasNode = actualObj.get("status").get("replicas");
            JsonNode readyReplicasName = actualObj.get("status").get("readyReplicas");
            return replicasNode != null && readyReplicasName != null
                    && replicasNode.asInt() == readyReplicasName.asInt();

        });
    }

    @Override
    public K waitForPod(String name) {
        // wait when all pods are ready
        return waitFor("pod", name,
            actualObj -> {
                JsonNode containerStatuses = actualObj.get("status").get("containerStatuses");
                ArrayList<Boolean> ready = new ArrayList<>();
                if (containerStatuses.isArray()) {
                    for (final JsonNode objNode : containerStatuses) {
                        ready.add(objNode.get("ready").asBoolean());
                    }
                    return !ready.contains(false);
                }
                return false;
            }
        );
    }

    @Override
    public K waitForStatefulSet(String name, int expectPods) {
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
    public K waitForResourceDeletion(String resourceType, String resourceName) {
        TestUtils.waitFor(resourceType + " " + resourceName + " removal",
            1_000L, 240_000L, () -> {
                try {
                    get(resourceType, resourceName);
                    return false;
                } catch (KubeClusterException.NotFound e) {
                    return true;
                }
            });
        return (K) this;
    }

    @Override
    public String toString() {
        return cmd();
    }
}
