/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.build.kafka.metadata;

import io.strimzi.kafka.config.model.ConfigModel;
import io.strimzi.kafka.config.model.Scope;
import io.strimzi.kafka.config.model.Type;
import kafka.server.DynamicBrokerConfig$;
import kafka.server.KafkaConfig$;
import org.apache.kafka.common.config.ConfigDef;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@SuppressWarnings("unchecked")
public class KafkaConfigModelGenerator extends CommonConfigModelGenerator {

    @Override
    protected Map<String, ConfigModel> configs() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        ConfigDef def = configDefs();
        Method getConfigValueMethod = methodFromReflection(ConfigDef.class, "getConfigValue", ConfigDef.ConfigKey.class, String.class);
        Method sortedConfigs = methodFromReflection(ConfigDef.class, "sortedConfigs");
        List<ConfigDef.ConfigKey> keys = (List) sortedConfigs.invoke(def);
        Map<String, String> dynamicUpdates = brokerDynamicUpdates();
        Map<String, ConfigModel> result = new TreeMap<>();

        for (ConfigDef.ConfigKey key : keys) {
            addConfigModel(getConfigValueMethod, key, def, dynamicUpdates, result);
        }
        return result;
    }

    static Map<String, String> brokerDynamicUpdates() {
        return DynamicBrokerConfig$.MODULE$.dynamicConfigUpdateModes();
    }

    @Override
    protected ConfigDef configDefs() {
        try {
            Field instance = getField(KafkaConfig$.class, "MODULE$");
            KafkaConfig$ x = (KafkaConfig$) instance.get(null);
            Field config = getOneOfFields(KafkaConfig$.class, "kafka$server$KafkaConfig$$configDef", "configDef");
            return (ConfigDef) config.get(x);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected ConfigModel createDescriptor(Method getConfigValueMethod, ConfigDef def, ConfigDef.ConfigKey key, Map<String, String> dynamicUpdates) throws InvocationTargetException, IllegalAccessException {
        Type type = parseType(String.valueOf(getConfigValueMethod.invoke(def, key, "Type")));
        Scope scope = parseScope(dynamicUpdates.getOrDefault(key.name, "read-only"));
        ConfigModel descriptor = new ConfigModel();
        descriptor.setType(type);
        descriptor.setScope(scope);
        return descriptor;
    }
}
