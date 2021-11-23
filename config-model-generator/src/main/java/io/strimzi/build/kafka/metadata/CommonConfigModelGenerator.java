/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.build.kafka.metadata;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.strimzi.kafka.config.model.ConfigModel;
import io.strimzi.kafka.config.model.ConfigModels;
import io.strimzi.kafka.config.model.Scope;
import io.strimzi.kafka.config.model.Type;
import kafka.Kafka;
import org.apache.kafka.common.config.ConfigDef;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Abstract class for loading a config model from Kafka classes.
 */
public abstract class CommonConfigModelGenerator {
    public void run(String fileName) throws Exception {
        String version = kafkaVersion();
        Map<String, ConfigModel> configs = configs();
        ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY);
        ConfigModels root = new ConfigModels();
        root.setVersion(version);
        root.setConfigs(configs);
        mapper.writeValue(new File(fileName), root);
    }

    private static String kafkaVersion() throws IOException {
        Properties p = new Properties();
        try (InputStream resourceAsStream = Kafka.class.getClassLoader().getResourceAsStream("kafka/kafka-version.properties")) {
            p.load(resourceAsStream);
        }
        return p.getProperty("version");
    }

    protected abstract Map<String, ConfigModel> configs() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException;

    protected Type parseType(String typeStr) {
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

    protected Scope parseScope(String scopeStr) {
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

    protected List<String> validList(ConfigDef.ConfigKey key) {
        try {
            Field f = ConfigDef.ValidList.class.getDeclaredField("validString");
            PrivilegedAction<Object> pa = () -> {
                f.setAccessible(true);
                return null;
            };
            AccessController.doPrivileged(pa);
            ConfigDef.ValidString itemValidator = (ConfigDef.ValidString) f.get(key.validator);
            List<String> validItems = enumer(itemValidator);
            return validItems;
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    protected List<String> enumer(ConfigDef.Validator validator) {
        try {
            Field f = getField(ConfigDef.ValidString.class, "validStrings");
            return (List<String>) f.get(validator);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    protected Field getOneOfFields(Class<?> cls, String... alternativeFields) {
        Field ret = null;
        for (String field : alternativeFields)  {
            ret = getField(cls, field);
        }
        if (ret == null) {
            throw new RuntimeException("None of the alternative fields were found.");
        } else {
            return ret;
        }
    }

    protected Field getField(Class<?> cls, String fieldName) {
        return AccessController.doPrivileged((PrivilegedAction<Field>) () -> {
            try {
                Field f1 = cls.getDeclaredField(fieldName);
                f1.setAccessible(true);
                return f1;
            } catch (NoSuchFieldException e) {
                return null;
            }
        });
    }

    protected ConfigModel range(ConfigDef.ConfigKey key, ConfigModel descriptor) {
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

    private void closedRange(Matcher matcher, ConfigModel descriptor) {
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

    private void openRange(Matcher matcher, ConfigModel descriptor) {
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
}
