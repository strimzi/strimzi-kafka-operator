package io.strimzi.controller.cluster.model;

import io.strimzi.controller.cluster.operator.resource.StatefulSetDiff;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;

public abstract class AbstractConfiguration {
    private static final Logger log = LoggerFactory.getLogger(AbstractConfiguration.class.getName());

    private Map<String, Object> options;

    public AbstractConfiguration(String configurationFile, List<String> FORBIDDEN_OPTIONS) {
        Map<String, Object> options = new HashMap<String, Object>();
        String[] lines = configurationFile.split("\n");

        for (String line : lines)   {
            String key = line.substring(0, line.indexOf("=")).trim();
            String value = line.substring(line.indexOf("=") + 1).trim();
            options.put(key, value);
        }

        this.options = filterForbidden(options, FORBIDDEN_OPTIONS);
    }

    public AbstractConfiguration(JsonObject jsonOptions, List<String> FORBIDDEN_OPTIONS) {
        this.options = filterForbidden(jsonOptions.getMap(), FORBIDDEN_OPTIONS);
    }

    private Map<String, Object> filterForbidden(Map<String, Object> options, List<String> FORBIDDEN_OPTIONS)   {
        Map<String, Object> filtered = new HashMap<>();

        outer: for (Map.Entry<String, Object> entry : options.entrySet()) {
            String key = entry.getKey();

            for (String forbiddenKey : FORBIDDEN_OPTIONS) {
                if (key.toLowerCase().startsWith(forbiddenKey)) {
                    log.warn("Configuration option \"{}\" is forbidden and will be ignored", key);
                    continue outer;
                }
            }

            log.trace("Configuration option \"{}\" is allowed and will be passed to Kafka", key);
            filtered.put(entry.getKey(), entry.getValue());
        }

        return filtered;
    }

    public String getConfigurationFile() {
        String configuration = "";
        final Charset charset = StandardCharsets.UTF_8;

        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            PrintStream printStream = new PrintStream(outputStream, true, charset.name());

            for (Map.Entry<String, Object> entry : options.entrySet()) {
                printStream.format("%s=%s%n", entry.getKey(), entry.getValue().toString());
            }

            configuration = new String(outputStream.toByteArray(), charset);
            printStream.close();
            outputStream.close();
        } catch (UnsupportedEncodingException e)    {
            log.error("Encoding {} is not supported", charset.name(), e);
        } catch (IOException e)    {
            log.error("Failed to close stream", e);
        }

        return configuration;
    }
}
