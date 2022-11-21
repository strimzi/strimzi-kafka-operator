/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.build.kafka.metadata;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.strimzi.kafka.config.model.ConfigModel;
import io.strimzi.kafka.config.model.ConfigModels;
import io.strimzi.kafka.config.model.Scope;
import io.strimzi.kafka.config.model.Type;
import kafka.Kafka;
import kafka.server.DynamicBrokerConfig$;
import kafka.server.KafkaConfig$;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.raft.RaftConfig;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.MetadataVersionValidator;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Generates the Kafka Config Model
 */
@SuppressWarnings("unchecked")
public class KafkaConfigModelGenerator {
    /**
     * The main method to run the config model generator
     *
     * @param args  Arguments
     *
     * @throws Exception    Throws an exception if generating the config model fails
     */
    public static void main(String[] args) throws Exception {
        String version = kafkaVersion();
        Map<String, ConfigModel> configs = configs(version);
        ObjectMapper mapper = JsonMapper.builder().enable(SerializationFeature.INDENT_OUTPUT).enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY).build();
        ConfigModels root = new ConfigModels();
        root.setVersion(version);
        root.setConfigs(configs);
        mapper.writeValue(new File(args[0]), root);
    }

    private static String kafkaVersion() throws IOException {
        Properties p = new Properties();
        try (InputStream resourceAsStream = Kafka.class.getClassLoader().getResourceAsStream("kafka/kafka-version.properties")) {
            p.load(resourceAsStream);
        }
        return p.getProperty("version");
    }

    private static Map<String, ConfigModel> configs(String version) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        ConfigDef def = brokerConfigs();
        Map<String, String> dynamicUpdates = brokerDynamicUpdates();
        Method getConfigValueMethod = def.getClass().getDeclaredMethod("getConfigValue", ConfigDef.ConfigKey.class, String.class);
        getConfigValueMethod.setAccessible(true);

        Method sortedConfigs = ConfigDef.class.getDeclaredMethod("sortedConfigs");
        sortedConfigs.setAccessible(true);

        List<ConfigDef.ConfigKey> keys = (List) sortedConfigs.invoke(def);
        Map<String, ConfigModel> result = new TreeMap<>();
        for (ConfigDef.ConfigKey key : keys) {
            String configName = String.valueOf(getConfigValueMethod.invoke(def, key, "Name"));
            Type type = parseType(String.valueOf(getConfigValueMethod.invoke(def, key, "Type")));
            Scope scope = parseScope(dynamicUpdates.getOrDefault(key.name, "read-only"));
            ConfigModel descriptor = new ConfigModel();
            descriptor.setType(type);
            descriptor.setScope(scope);

            if (key.validator instanceof ConfigDef.Range) {
                descriptor = range(key, descriptor);
            } else if (key.validator instanceof ConfigDef.ValidString) {
                descriptor.setValues(enumer(key.validator));
            } else if (key.validator instanceof ConfigDef.ValidList) {
                descriptor.setItems(validList(key));
            } else if (key.validator instanceof MetadataVersionValidator) {
                // Versions for Kafka 3.3.0 and newer are in MetadataVersion
                // We extract it from there
                Iterator<MetadataVersion> iterator = Arrays.stream(MetadataVersion.VERSIONS).iterator();
                LinkedHashSet<String> versions = new LinkedHashSet<>();
                while (iterator.hasNext()) {
                    MetadataVersion next = iterator.next();
                    versions.add(Pattern.quote(next.shortVersion()) + "(\\.[0-9]+)*");
                    versions.add(Pattern.quote(next.version()));
                }
                descriptor.setPattern(String.join("|", versions));
            } else if (key.validator != null && "class kafka.api.ApiVersionValidator$".equals(key.validator.getClass().toString())) {
                // Versions for Kafka 3.2.x and older are in ApiVersions. We cannot use this class because it does not
                // exist anymore in Kafka 3.3.0. So we extract the versions from Kafka 3.3.0, but we filter them to have
                // the short version equal or less the version for which we generate the config. This should filter out
                // the older versions and keep only the valid versions for given release.
                Iterator<MetadataVersion> iterator = Arrays.stream(MetadataVersion.VERSIONS).iterator();
                LinkedHashSet<String> versions = new LinkedHashSet<>();
                while (iterator.hasNext()) {
                    MetadataVersion next = iterator.next();
                    if (compareDottedVersions(version, next.shortVersion()) >= 0) {
                        versions.add(Pattern.quote(next.shortVersion()) + "(\\.[0-9]+)*");
                        versions.add(Pattern.quote(next.version()));
                    }
                }
                descriptor.setPattern(String.join("|", versions));
            } else if (key.validator instanceof ConfigDef.NonNullValidator) {
                descriptor.setPattern(".+");
            } else if (key.validator instanceof ConfigDef.NonEmptyString) {
                descriptor.setPattern(".+");
            } else if (key.validator instanceof RaftConfig.ControllerQuorumVotersValidator)   {
                continue;
            } else if (key.validator != null) {
                throw new IllegalStateException("Invalid validator class " + key.validator.getClass() + " for option " + configName);
            }

            result.put(configName, descriptor);
        }
        return result;
    }

    private static Type parseType(String typeStr) {
        Type type;
        if ("boolean".equals(typeStr)) {
            type = Type.BOOLEAN;
        } else if ("string".equals(typeStr)) {
            type = Type.STRING;
        } else if ("password".equals(typeStr)) {
            type = Type.PASSWORD;
        } else if ("class".equals(typeStr)) {
            type = Type.CLASS;
        } else if ("int".equals(typeStr)) {
            type = Type.INT;
        } else if ("short".equals(typeStr)) {
            type = Type.SHORT;
        } else if ("long".equals(typeStr)) {
            type = Type.LONG;
        } else if ("double".equals(typeStr)) {
            type = Type.DOUBLE;
        } else if ("list".equals(typeStr)) {
            type = Type.LIST;
        } else {
            throw new RuntimeException("Unsupported type: " + typeStr);
        }
        return type;
    }

    private static Scope parseScope(String scopeStr) {
        Scope scope;
        if ("per-broker".equals(scopeStr)) {
            scope = Scope.PER_BROKER;
        } else if ("cluster-wide".equals(scopeStr)) {
            scope = Scope.CLUSTER_WIDE;
        } else if ("read-only".equals(scopeStr)) {
            scope = Scope.READ_ONLY;
        } else {
            throw new RuntimeException("Unsupported scope: " + scopeStr);
        }
        return scope;
    }

    private static List<String> validList(ConfigDef.ConfigKey key) {
        try {
            Field f = ConfigDef.ValidList.class.getDeclaredField("validString");
            f.setAccessible(true);
            ConfigDef.ValidString itemValidator = (ConfigDef.ValidString) f.get(key.validator);
            List<String> validItems = enumer(itemValidator);
            return validItems;
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<String> enumer(ConfigDef.Validator validator) {
        try {
            Field f = getField(ConfigDef.ValidString.class, "validStrings");
            return (List) f.get(validator);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static Field getOneOfFields(Class<?> cls, String... alternativeFields) {
        for (String field : alternativeFields)  {
            try {
                return getField(KafkaConfig$.class, field);
            } catch (RuntimeException e)    {
                if (e.getCause() instanceof NoSuchFieldException)   {
                    continue;
                } else {
                    throw e;
                }
            }
        }

        throw new RuntimeException("None of the alternative fields were found.");
    }

    private static Field getField(Class<?> cls, String fieldName) {
        return AccessController.doPrivileged(new PrivilegedAction<Field>() {
                @Override
                public Field run() {
                    try {
                        Field f1 = cls.getDeclaredField(fieldName);
                        f1.setAccessible(true);
                        return f1;
                    } catch (ReflectiveOperationException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
    }

    private static ConfigModel range(ConfigDef.ConfigKey key, ConfigModel descriptor) {
        String str = key.validator.toString();
        try {
            Pattern rangePattern = Pattern.compile("\\[([0-9.e+-]+|\\.\\.\\.),?([0-9.e+-]+|\\.\\.\\.)?,?([0-9.e+-]+|\\.\\.\\.)?\\]", Pattern.CASE_INSENSITIVE);
            Matcher matcher = rangePattern.matcher(str);
            if (matcher.matches()) {
                if (matcher.groupCount() == 3 && matcher.group(3) == null) {
                    openRange(matcher, descriptor);
                } else if (matcher.groupCount() == 3) {
                    closedRange(matcher, descriptor);
                } else if (matcher.groupCount() != 1) {
                    throw new IllegalStateException(str);
                }
            } else {
                throw new IllegalStateException(str);
            }
            return descriptor;
        } catch (RuntimeException e) {
            throw new RuntimeException("Invalid range " + str + " for key " + key.name, e);
        }
    }

    private static void closedRange(Matcher matcher, ConfigModel descriptor) {
        String maxStr = matcher.group(3);
        if (maxStr.contains("e") || maxStr.contains(".") && !maxStr.contains("..")) {
            descriptor.setMaximum(Double.parseDouble(maxStr));
        } else if (!"...".equals(maxStr)) {
            descriptor.setMaximum(Long.parseLong(maxStr));
        }

        String midStr = matcher.group(2);
        if (!"...".equals(midStr)) {
            throw new IllegalStateException();
        }

        String minStr = matcher.group(1);
        if (minStr.contains("e") || minStr.contains(".") && !minStr.contains("..")) {
            descriptor.setMinimum(Double.parseDouble(minStr));
        } else if (!"...".equals(minStr)) {
            descriptor.setMinimum(Long.parseLong(minStr));
        }
    }

    private static void openRange(Matcher matcher, ConfigModel descriptor) {
        String maxStr = matcher.group(2);
        if (maxStr.contains("e") || maxStr.contains(".") && !maxStr.contains("..")) {
            descriptor.setMaximum(Double.parseDouble(maxStr));
        } else if (!"...".equals(maxStr)) {
            descriptor.setMaximum(Long.parseLong(maxStr));
        }

        String minStr = matcher.group(1);
        if (minStr.contains("e") || minStr.contains(".") && !minStr.contains("..")) {
            descriptor.setMinimum(Double.parseDouble(minStr));
        } else if (!"...".equals(minStr)) {
            descriptor.setMinimum(Long.parseLong(minStr));
        }
    }

    static Map<String, String> brokerDynamicUpdates() {
        return DynamicBrokerConfig$.MODULE$.dynamicConfigUpdateModes();
    }

    static ConfigDef brokerConfigs() {
        try {
            Field instance = getField(KafkaConfig$.class, "MODULE$");
            KafkaConfig$ x = (KafkaConfig$) instance.get(null);
            Field config = getOneOfFields(KafkaConfig$.class, "kafka$server$KafkaConfig$$configDef", "configDef");
            return (ConfigDef) config.get(x);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Compare two decimal version strings, e.g. 1.10.1 &gt; 1.9.2
     * @param version1 The first version.
     * @param version2 The second version.
     * @return Zero if version1 == version2;
     * -1 if version1 &lt; version2;
     * 1 if version1 &gt; version2.
     */
    static int compareDottedVersions(String version1, String version2) {
        String[] components = version1.split("\\.");
        String[] otherComponents = version2.split("\\.");
        for (int i = 0; i < Math.min(components.length, otherComponents.length); i++) {
            int x = Integer.parseInt(components[i]);
            int y = Integer.parseInt(otherComponents[i]);
            if (x == y) {
                continue;
            } else if (x < y) {
                return -1;
            } else {
                return 1;
            }
        }
        // mismatch was not found, but the versions are of different length, e.g. 2.8 and 2.8.0
        return 0;
    }

}
