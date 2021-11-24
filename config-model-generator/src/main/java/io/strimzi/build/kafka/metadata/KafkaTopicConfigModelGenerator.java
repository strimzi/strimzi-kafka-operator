/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.build.kafka.metadata;

import io.strimzi.kafka.config.model.ConfigModel;
import io.strimzi.kafka.config.model.Type;
import kafka.log.LogConfig$;
import org.apache.kafka.common.config.ConfigDef;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@SuppressWarnings("unchecked")
public class KafkaTopicConfigModelGenerator extends CommonConfigModelGenerator {

    @Override
    protected Map<String, ConfigModel> configs() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        ConfigDef def = configDefs();
        Method getConfigValueMethod = methodFromReflection(ConfigDef.class, "getConfigValue", ConfigDef.ConfigKey.class, String.class);
        Method sortedConfigs = methodFromReflection(ConfigDef.class, "sortedConfigs");
        List<ConfigDef.ConfigKey> keys = (List) sortedConfigs.invoke(def);
        Map<String, ConfigModel> result = new TreeMap<>();

        for (ConfigDef.ConfigKey key : keys) {
            addConfigModel(getConfigValueMethod, key, def, Collections.emptyMap(), result);
        }
        return result;
    }

    @Override
    protected ConfigDef configDefs() {
        try {
            Field instance = getField(LogConfig$.class, "MODULE$");
            LogConfig$ x = (LogConfig$) instance.get(null);
            Field config = getOneOfFields(LogConfig$.class, "kafka$log$LogConfig$$configDef", "configDef");
            return (ConfigDef) config.get(x);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected ConfigModel createDescriptor(Method getConfigValueMethod, ConfigDef def, ConfigDef.ConfigKey key, Map<String, String> dynamicUpdates) throws InvocationTargetException, IllegalAccessException {
        Type type = parseType(String.valueOf(getConfigValueMethod.invoke(def, key, "Type")));
        ConfigModel descriptor = new ConfigModel();
        descriptor.setType(type);
        return descriptor;
    }

}
