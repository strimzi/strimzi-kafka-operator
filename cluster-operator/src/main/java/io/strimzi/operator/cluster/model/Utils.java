/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.operator.cluster.InvalidConfigMapException;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;

import java.util.Map;

import static io.strimzi.operator.cluster.model.KafkaCluster.KEY_KAFKA_CONFIG;
import static io.strimzi.operator.cluster.model.KafkaConnectCluster.KEY_CONNECT_CONFIG;
import static io.strimzi.operator.cluster.model.ZookeeperCluster.KEY_ZOOKEEPER_CONFIG;

public class Utils {
    public static int getInteger(Map<String, String> data, String key, int defaultValue) {
        try {
            if (data.get(key) == null) { // value is not set in config map -> will be used default value
                return defaultValue;
            }
            return Integer.parseInt(data.get(key));
        } catch (NumberFormatException e)  {
            String msg = " is corrupted";
            if (data.get(key).isEmpty()) {
                msg = " is empty";
            }
            throw new InvalidConfigMapException(key, msg);
        }
    }

    public static boolean getBoolean(Map<String, String> data, String key, boolean defaultValue) {
        if (data.get(key) == null) { // value is not set in config map
            return defaultValue;
        } // java parses "truue" as false. We need to be more strict
        if (data.get(key).equals("true")) {
            return true;
        } else if (data.get(key).equals("false")) {
            return false;
        } else {
            throw new InvalidConfigMapException(key, " is corrupted");
        }
    }

    public static Storage getStorage(Map<String, String> data, String key) {
        try {
            if (data.get(key) == null) { // value is not set in config map
                return new Storage(Storage.StorageType.PERSISTENT_CLAIM);
            }
            return Storage.fromJson(new JsonObject(data.get(key)));
        } catch (Exception e) {
            throw new InvalidConfigMapException(key, " is corrupted");
        }
    }

    public static String getNonemptyString(Map<String, String> data, String key, String defaultValue) {
        if (data.get(key) == null) { // value is not set in config map -> will be used default value
            return defaultValue;
        }
        if (data.get(key).length() == 0) {
            throw new InvalidConfigMapException(key, " is empty");
        }
        return data.get(key);
    }

    public static String getJsonCorruptionBlame(DecodeException de, String config) {
        String token = "";
        token = de.getMessage();
        if (token.contains("Illegal unquoted character")) {
            return "JSON quotation";
        }
        if (token.contains("Failed to decode: Unexpected end-of-input")) {
            return "JSON braces";
        }
        token = token.replace("Failed to decode: Unrecognized token '", "");
        token = token.substring(0, token.indexOf("'"));

        String[] entries = config.replace("{", "").replace("}", "")
               .replace("\"", "").replace(" ", "")
                .replace(",", "").split("\n");
        int i;
        String blame = "";
        for (i = 0; i < entries.length; ++i) {
            if (entries[i].split(":")[1].equals(token)) {
                blame = entries[i].split(":")[0];
                break;
            }
        }
        return blame;
    }

    public static <T extends AbstractConfiguration> T getConfig(Map<String, String> data, String key) {
        JsonObject jo = getJson(data, key);
        if (key.equals(KEY_CONNECT_CONFIG)) {
            return jo == null ? (T) new KafkaConnectConfiguration(new JsonObject()) : (T) new KafkaConnectConfiguration(jo);
        } else if (key.equals(KEY_ZOOKEEPER_CONFIG)) {
            return jo == null ? (T) new ZookeeperConfiguration(new JsonObject()) : (T) new ZookeeperConfiguration(jo);
        } else if (key.equals(KEY_KAFKA_CONFIG)) {
            return jo == null ?  (T) new KafkaConfiguration(new JsonObject()) : (T) new KafkaConfiguration(jo);
        }
        return null;
    }

    public static JsonObject getJson(Map<String, String> data, String key) {
        String config = data.get(key);
        if (config == null) {
            return null;
        }
        try {
            new JsonObject(config);
        } catch (DecodeException de) {
            throw new InvalidConfigMapException(getJsonCorruptionBlame(de, config), " corruptes JSON");
        }
        return new JsonObject(config);
    }
}
