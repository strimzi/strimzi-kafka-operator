/*
 * Copyright 2017-2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.fabric8.kubernetes.api.model.Affinity;
import io.strimzi.operator.cluster.InvalidConfigMapException;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;

import java.util.Map;

public final class Utils {


    private Utils() {
        // no-op
    }
    public static int getInteger(Map<String, String> data, String key, int defaultValue) {
        try {
            if (data.get(key) == null) { // value is not set in ConfigMap -> will be used default value
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
        if (data.get(key) == null) { // value is not set in ConfigMap
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
            if (data.get(key) == null) { // value is not set in ConfigMap
                return new Storage(Storage.StorageType.PERSISTENT_CLAIM);
            }
            return Storage.fromJson(new JsonObject(data.get(key)));
        } catch (Exception e) {
            throw new InvalidConfigMapException(key, " is corrupted");
        }
    }

    public static String getNonEmptyString(Map<String, String> data, String key, String defaultValue) {
        if (data.get(key) == null) { // value is not set in ConfigMap -> will be used default value
            return defaultValue;
        }
        if (data.get(key).length() == 0) {
            throw new InvalidConfigMapException(key, " is empty");
        }
        return data.get(key);
    }

    public static String getJsonCorruptionBlame(DecodeException de, String config) {
        try {
            String token = "";
            token = de.getMessage();
            if (token.contains("Failed to decode: No content to map due to end-of-input")) {
                return "JSON - empty value";
            }
            if (token.contains("Illegal unquoted character")) {
                return "JSON quotation";
            }
            if (token.contains("Failed to decode: Unexpected character")) {
                token = token.replace("Failed to decode: Unexpected character ('", "");
                token = token.substring(0, token.indexOf("'"));
                return "Unexpected character - " + token;
            }
            if (token.contains("Failed to decode: Unexpected end-of-input")) {
                return "JSON bracing";
            }
            token = token.replace("Failed to decode: Unrecognized token '", "");
            token = token.substring(0, token.indexOf("'"));

            String[] entries = config.replace("{", "").replace("}", "")
                    .replace("\"", "").replace(" ", "")
                    .replace(",", "").split("\n");
            int i;
            String blame = "";
            for (i = 0; i < entries.length; ++i) {
                if (entries[i].length() < 1)
                    continue;
                if (entries[i].split(":")[1].equals(token)) {
                    blame = entries[i].split(":")[0];
                    break;
                }
            }
            return blame;
        } catch (ArrayIndexOutOfBoundsException e) {
            return "unknown flaw";
        }
    }

    public static KafkaConnectConfiguration getKafkaConnectConfiguration(Map<String, String> data, String key) {
        JsonObject jo = getJson(data, key);
        return jo == null ? new KafkaConnectConfiguration(new JsonObject()) : new KafkaConnectConfiguration(jo);
    }

    public static ZookeeperConfiguration getZookeeperConfiguration(Map<String, String> data, String key) {
        JsonObject jo = getJson(data, key);
        return jo == null ? new ZookeeperConfiguration(new JsonObject()) : new ZookeeperConfiguration(jo);
    }

    public static KafkaConfiguration getKafkaConfiguration(Map<String, String> data, String key) {
        JsonObject jo = getJson(data, key);
        return jo == null ? new KafkaConfiguration(new JsonObject()) : new KafkaConfiguration(jo);
    }

    public static JsonObject getJson(Map<String, String> data, String key) {
        String config = data.get(key);
        if (config == null) {
            return null;
        }
        try {
            new JsonObject(config);
        } catch (DecodeException de) {
            throw new InvalidConfigMapException(getJsonCorruptionBlame(de, config), " corrupts JSON");
        }
        return new JsonObject(config);
    }

    public static Affinity getAffinity(String json) {
        return JsonUtils.fromYaml(json, Affinity.class);
    }
}
