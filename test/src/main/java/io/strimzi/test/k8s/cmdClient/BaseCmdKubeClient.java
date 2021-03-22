/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.k8s.cmdClient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import io.strimzi.test.TestUtils;
import io.strimzi.test.executor.Exec;
import io.strimzi.test.executor.ExecResult;
import io.strimzi.test.k8s.exceptions.KubeClusterException;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.lang.String.join;
import static java.util.Arrays.asList;

public abstract class BaseCmdKubeClient<K extends BaseCmdKubeClient<K>> implements KubeCmdClient<K> {

    private static final Logger LOGGER = LogManager.getLogger(BaseCmdKubeClient.class);

    private static final String CREATE = "create";
    private static final String APPLY = "apply";
    private static final String DELETE = "delete";
    private static final String REPLACE = "replace";

    public static final String STATEFUL_SET = "statefulset";
    public static final String CM = "cm";

    String namespace = defaultNamespace();

    public abstract String cmd();

    @Override
    @SuppressWarnings("unchecked")
    public K deleteByName(String resourceType, String resourceName) {
        Exec.exec(namespacedCommand(DELETE, resourceType, resourceName));
        return (K) this;
    }

    protected static class Context implements AutoCloseable {
        @Override
        public void close() { }
    }

    private static final Context NOOP = new Context();

    protected Context defaultContext() {
        return NOOP;
    }

    // Admin contex tis not implemented now, because it's not needed
    // In case it will be neded in future, we should change the kubeconfig and apply it for both oc and kubectl
    protected Context adminContext() {
        return defaultContext();
    }

    protected List<String> namespacedCommand(String... rest) {
        return namespacedCommand(asList(rest));
    }

    private List<String> namespacedCommand(List<String> rest) {
        List<String> result = new ArrayList<>();
        result.add(cmd());
        result.add("--namespace");
        result.add(namespace);
        result.addAll(rest);
        return result;
    }

    @Override
    public String get(String resource, String resourceName) {
        return Exec.exec(namespacedCommand("get", resource, resourceName, "-o", "yaml")).out();
    }

    @Override
    public String getEvents() {
        return Exec.exec(namespacedCommand("get", "events")).out();
    }

    @Override
    @SuppressWarnings("unchecked")
    public K create(File file) {
        Exec.exec(null, namespacedCommand(CREATE, "-f", file.getAbsolutePath()), 0, false, true);

        return (K) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public K createOrReplace(File file) {
        try (Context context = defaultContext()) {
            try {
                create(file);
            } catch (KubeClusterException.AlreadyExists e) {
                Exec.exec(null, namespacedCommand(REPLACE, "-f", file.getAbsolutePath()), 0, false);
            }

            return (K) this;
        }
    }
    
    @Override
    @SuppressWarnings("unchecked")
    public K create(File... files) {
        try (Context context = defaultContext()) {
            Map<File, ExecResult> execResults = execRecursive(CREATE, files, Comparator.comparing(File::getName).reversed());
            for (Map.Entry<File, ExecResult> entry : execResults.entrySet()) {
                if (!entry.getValue().exitStatus()) {
                    LOGGER.warn("Failed to create {}!", entry.getKey().getAbsolutePath());
                    LOGGER.debug(entry.getValue().err());
                }
            }
            return (K) this;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public K apply(File... files) {
        try (Context context = defaultContext()) {
            Map<File, ExecResult> execResults = execRecursive(APPLY, files, Comparator.comparing(File::getName).reversed());
            for (Map.Entry<File, ExecResult> entry : execResults.entrySet()) {
                if (!entry.getValue().exitStatus()) {
                    LOGGER.warn("Failed to apply {}!", entry.getKey().getAbsolutePath());
                    LOGGER.debug(entry.getValue().err());
                }
            }
            return (K) this;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public K delete(File... files) {
        try (Context context = defaultContext()) {
            Map<File, ExecResult> execResults = execRecursive(DELETE, files, Comparator.comparing(File::getName).reversed());
            for (Map.Entry<File, ExecResult> entry : execResults.entrySet()) {
                if (!entry.getValue().exitStatus()) {
                    LOGGER.warn("Failed to delete {}!", entry.getKey().getAbsolutePath());
                    LOGGER.debug(entry.getValue().err());
                }
            }
            return (K) this;
        }
    }

    private Map<File, ExecResult> execRecursive(String subcommand, File[] files, Comparator<File> cmp) {
        Map<File, ExecResult> execResults = new HashMap<>(25);
        for (File f : files) {
            if (f.isFile()) {
                if (f.getName().endsWith(".yaml")) {
                    execResults.put(f, Exec.exec(null, namespacedCommand(subcommand, "-f", f.getAbsolutePath()), 0, false, false));
                }
            } else if (f.isDirectory()) {
                File[] children = f.listFiles();
                if (children != null) {
                    Arrays.sort(children, cmp);
                    execResults.putAll(execRecursive(subcommand, children, cmp));
                }
            } else if (!f.exists()) {
                throw new RuntimeException(new NoSuchFileException(f.getPath()));
            }
        }
        return execResults;
    }

    @Override
    @SuppressWarnings("unchecked")
    public K replace(File... files) {
        try (Context context = defaultContext()) {
            Map<File, ExecResult> execResults = execRecursive(REPLACE, files, Comparator.comparing(File::getName));
            for (Map.Entry<File, ExecResult> entry : execResults.entrySet()) {
                if (!entry.getValue().exitStatus()) {
                    LOGGER.warn("Failed to replace {}!", entry.getKey().getAbsolutePath());
                    LOGGER.debug(entry.getValue().err());
                }
            }
            return (K) this;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public K applyContent(String yamlContent) {
        try (Context context = defaultContext()) {
            Exec.exec(yamlContent, namespacedCommand(APPLY, "-f", "-"), 0, true);
            return (K) this;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public K createContent(String yamlContent) {
        try (Context context = defaultContext()) {
            Exec.exec(yamlContent, namespacedCommand(CREATE, "-f", "-"), 0, true);
            return (K) this;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public K replaceContent(String yamlContent) {
        try (Context context = defaultContext()) {
            try {
                createContent(yamlContent);
            } catch (KubeClusterException.AlreadyExists e) {
                Exec.exec(yamlContent, namespacedCommand(REPLACE, "-f", "-"), 0, true);
            }

            return (K) this;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public K deleteContent(String yamlContent) {
        try (Context context = defaultContext()) {
            Exec.exec(yamlContent, namespacedCommand(DELETE, "-f", "-"), 0, true, false);
            return (K) this;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public K createNamespace(String name) {
        try (Context context = adminContext()) {
            Exec.exec(null, namespacedCommand(CREATE, "namespace", name), 0, true);
        }
        return (K) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public K deleteNamespace(String name) {
        try (Context context = adminContext()) {
            Exec.exec(null, namespacedCommand(DELETE, "namespace", name), 0, true, false);
        }
        return (K) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public K scaleByName(String kind, String name, int replicas) {
        try (Context context = defaultContext()) {
            Exec.exec(null, namespacedCommand("scale", kind, name, "--replicas", Integer.toString(replicas)));
            return (K) this;
        }
    }

    @Override
    public ExecResult execInPod(String pod, String... command) {
        List<String> cmd = namespacedCommand("exec", pod, "--");
        cmd.addAll(asList(command));
        return Exec.exec(cmd);
    }

    @Override
    public ExecResult execInPodContainer(String pod, String container, String... command) {
        return execInPodContainer(true, pod, container, command);
    }

    @Override
    public ExecResult execInPodContainer(boolean logToOutput, String pod, String container, String... command) {
        List<String> cmd = namespacedCommand("exec", pod, "-c", container, "--");
        cmd.addAll(asList(command));
        return Exec.exec(null, cmd, 0, logToOutput);
    }

    @Override
    public ExecResult exec(String... command) {
        return exec(true, command);
    }

    @Override
    public ExecResult exec(boolean throwError, String... command) {
        List<String> cmd = new ArrayList<>();
        cmd.add(cmd());
        cmd.addAll(asList(command));
        return Exec.exec(null, cmd, 0, true, throwError);
    }

    @Override
    public ExecResult exec(boolean throwError, boolean logToOutput, String... command) {
        List<String> cmd = new ArrayList<>();
        cmd.add(cmd());
        cmd.addAll(asList(command));
        return Exec.exec(null, cmd, 0, logToOutput, throwError);
    }

    @Override
    public ExecResult execInCurrentNamespace(String... commands) {
        return Exec.exec(namespacedCommand(commands));
    }

    @Override
    public ExecResult execInCurrentNamespace(boolean logToOutput, String... commands) {
        return Exec.exec(null, namespacedCommand(commands), 0, logToOutput);
    }

    enum ExType {
        BREAK,
        CONTINUE,
        THROW
    }

    @SuppressWarnings("unchecked")
    public K waitFor(String resource, String name, Predicate<JsonNode> condition) {
        long timeoutMs = 570_000L;
        long pollMs = 1_000L;
        ObjectMapper mapper = new ObjectMapper();
        TestUtils.waitFor(resource + " " + name, pollMs, timeoutMs, () -> {
            try {
                String jsonString = Exec.exec(namespacedCommand("get", resource, name, "-o", "json")).out();
                LOGGER.trace("{}", jsonString);
                JsonNode actualObj = mapper.readTree(jsonString);
                return condition.test(actualObj);
            } catch (KubeClusterException.NotFound e) {
                return false;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return (K) this;
    }

    @Override
    public K waitForResourceCreation(String resourceType, String resourceName) {
        // wait when resource to be created
        return waitFor(resourceType, resourceName,
            actualObj -> true
        );
    }

    @Override
    @SuppressWarnings("unchecked")
    public K waitForResourceDeletion(String resourceType, String resourceName) {
        TestUtils.waitFor(resourceType + " " + resourceName + " removal",
            1_000L, 480_000L, () -> {
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
    @SuppressWarnings("unchecked")
    public K waitForResourceUpdate(String resourceType, String resourceName, Date startTime) {

        TestUtils.waitFor(resourceType + " " + resourceName + " update",
                1_000L, 240_000L, () -> {
                try {
                    return startTime.before(getResourceCreateTimestamp(resourceType, resourceName));
                } catch (KubeClusterException.NotFound e) {
                    return false;
                }
            });
        return (K) this;
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
        return asList(Exec.exec(namespacedCommand("get", resourceType, "-o", "jsonpath={range .items[*]}{.metadata.name} ")).out().trim().split(" +")).stream().filter(s -> !s.trim().isEmpty()).collect(Collectors.toList());
    }

    @Override
    public String getResourceAsJson(String resourceType, String resourceName) {
        return Exec.exec(namespacedCommand("get", resourceType, resourceName, "-o", "json")).out();
    }

    @Override
    public String getResourceAsYaml(String resourceType, String resourceName) {
        return Exec.exec(namespacedCommand("get", resourceType, resourceName, "-o", "yaml")).out();
    }

    @Override
    public String getResources(String resourceType) {
        return Exec.exec(namespacedCommand("get", resourceType)).out();
    }

    @Override
    public String getResourcesAsYaml(String resourceType) {
        return Exec.exec(namespacedCommand("get", resourceType, "-o", "yaml")).out();
    }

    @Override
    synchronized public void createResourceAndApply(String template, Map<String, String> params) {
        List<String> cmd = namespacedCommand("process", template, "-l", "app=" + template, "-o", "yaml");
        for (Map.Entry<String, String> entry : params.entrySet()) {
            cmd.add("-p");
            cmd.add(entry.getKey() + "=" + entry.getValue());
        }

        String yaml = Exec.exec(cmd).out();
        applyContent(yaml);
    }

    @Override
    public String describe(String resourceType, String resourceName) {
        return Exec.exec(namespacedCommand("describe", resourceType, resourceName)).out();
    }

    @Override
    public String logs(String pod, String container) {
        String[] args;
        if (container != null) {
            args = new String[]{"logs", pod, "-c", container};
        } else {
            args = new String[]{"logs", pod};
        }
        return Exec.exec(namespacedCommand(args)).out();
    }

    @Override
    public String searchInLog(String resourceType, String resourceName, long sinceSeconds, String... grepPattern) {
        try {
            return Exec.exec("bash", "-c", join(" ", namespacedCommand("logs", resourceType + "/" + resourceName, "--since=" + sinceSeconds + "s",
                    "|", "grep", " -e " + join(" -e ", grepPattern), "-B", "1"))).out();
        } catch (KubeClusterException e) {
            if (e.result != null && e.result.returnCode() == 1) {
                LOGGER.info("{} not found", grepPattern);
            } else {
                LOGGER.error("Caught exception while searching {} in logs", grepPattern);
            }
        }
        return "";
    }

    @Override
    public String searchInLog(String resourceType, String resourceName, String resourceContainer, long sinceSeconds, String... grepPattern) {
        try {
            return Exec.exec("bash", "-c", join(" ", namespacedCommand("logs", resourceType + "/" + resourceName, "-c " + resourceContainer, "--since=" + sinceSeconds + "s",
                    "|", "grep", " -e " + join(" -e ", grepPattern), "-B", "1"))).out();
        } catch (KubeClusterException e) {
            if (e.result != null && e.result.exitStatus()) {
                LOGGER.info("{} not found", grepPattern);
            } else {
                LOGGER.error("Caught exception while searching {} in logs", grepPattern);
            }
        }
        return "";
    }

    public List<String> listResourcesByLabel(String resourceType, String label) {
        return asList(Exec.exec(namespacedCommand("get", resourceType, "-l", label, "-o", "jsonpath={range .items[*]}{.metadata.name} ")).out().split("\\s+"));
    }

    @Override
    public String getResourceJsonPath(String resourceType, String resourceName, String path) {
        return Exec.exec(namespacedCommand("get", resourceType, "-o", "jsonpath={.items[?(.metadata.name==\"" + resourceName + "\")]" + path + "}")).out().trim();
    }

    @Override
    public boolean getResourceReadiness(String resourceType, String resourceName) {
        return Exec.exec(namespacedCommand("get", resourceType, "-o", "jsonpath={.items[?(.metadata.name==\"" + resourceName + "\")].status.conditions[?(.type==\"Ready\")].status}")).out().contains("True");
    }

    @Override
    public void patchResource(String resourceType, String resourceName, String patchPath, String value) {
        Exec.exec(namespacedCommand("patch", resourceType, resourceName, "--type=json", "-p=[{\"op\": \"replace\",\"path\":\"" + patchPath + "\",\"value\":\"" + value + "\"}]"));
    }
}
