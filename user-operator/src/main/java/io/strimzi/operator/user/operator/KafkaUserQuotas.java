/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.vertx.core.json.JsonObject;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.Charset;
import java.util.Map;

/**
 * Utility class for managing Quotas for Kafka User
 */
public class KafkaUserQuotas {
    private static final Logger log = LogManager.getLogger(KafkaUserQuotas.class.getName());

    private final static int CONNECTION_TIMEOUT = 30_000;

    private ZkClient zkClient;

    public KafkaUserQuotas(String zookeeperUrl, int zookeeperSessionTimeout) {
        zkClient = new ZkClient(zookeeperUrl, zookeeperSessionTimeout, CONNECTION_TIMEOUT, new BytesPushThroughSerializer());
    }

    /**
     * Create or update the quotas for the given user.
     *
     * @param username The name of the user which should be created or updated
     * @param quotas The desired user quotas
     */
    public void createOrUpdate(String username, JsonObject quotas) {
        byte[] data = zkClient.readData("/config/users/" + username, true);

        if (data != null)   {
            log.debug("Updating quotas for user {}", username);
            zkClient.writeData("/config/users/" + username, updateUserJson(data, quotas));
        } else {
            log.debug("Creating quotas for user {}", username);
            ensurePath("/config/users");
            zkClient.createPersistent("/config/users/" + username, createUserJson(quotas));
        }

        notifyChanges(username);
    }

    /**
     * Generates the JSON with the credentials
     *
     * @param quotas  quotas
     * @return  Returns the generated JSON as byte array
     */
    protected byte[] createUserJson(JsonObject quotas)   {
        JsonObject json = new JsonObject()
                .put("version", 1)
                .put("config", new JsonObject());

        for (Map.Entry<String, Object> quota: quotas.getMap().entrySet()) {
            json.getJsonObject("config").put(quota.getKey(), quota.getValue().toString());
        }

        return json.encode().getBytes(Charset.defaultCharset());
    }

    /**
     * Updates the quotas in existing JSON
     *
     * @param user JSON string with existing user configuration as byte[]
     * @param quotas  quotas
     *
     * @return  Returns the updated JSON as byte array
     */
    protected byte[] updateUserJson(byte[] user, JsonObject quotas)   {
        JsonObject json = new JsonObject(new String(user, Charset.defaultCharset()));

        validateJsonVersion(json);

        if (json.getJsonObject("config") == null)   {
            json.put("config", new JsonObject());
        }

        for (Map.Entry<String, Object> quota: quotas.getMap().entrySet()) {
            json.getJsonObject("config").put(quota.getKey(), quota.getValue().toString());
        }
        return json.encode().getBytes(Charset.defaultCharset());

    }

    /**
     * This notifies Kafka about the changes we have made
     *
     * @param username  Name of the user whose configuration changed
     */
    private void notifyChanges(String username) {
        log.debug("Notifying changes for user {}", username);

        ensurePath("/config/changes");

        JsonObject json = new JsonObject().put("version", 2).put("entity_path", "users/" + username);
        zkClient.createPersistentSequential("/config/changes/config_change_", json.encode().getBytes(Charset.defaultCharset()));
    }

    /**
     * Ensures that the path in Zookeeper exists.
     * It checks whether it already exists and in case it doesn't, it will create the path.
     *
     * @param path The Zookeeper path which should exist
     */
    private void ensurePath(String path)    {
        if (!zkClient.exists(path))   {
            zkClient.createPersistent(path, true);
        }
    }

    /**
     * Determine whether the given user has quotas.
     *
     * @param username Name of the user
     *
     * @return True if the user exists
     */
    public boolean exists(String username) {
        byte[] data = zkClient.readData("/config/users/" + username, true);

        if (data != null)   {
            String jsonString = new String(data, Charset.defaultCharset());
            JsonObject json = new JsonObject(jsonString);
            validateJsonVersion(json);
            JsonObject config = json.getJsonObject("config");

            if (config != null) {
                String prod = config.getString("producer_byte_rate");
                String cons = config.getString("consumer_byte_rate");
                String perc = config.getString("request_percentage");

                if (prod != null || cons != null || perc != null) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Delete the quotas for the given user.
     * It is not an error if the user doesn't exist, or doesn't currently have any quotas.
     *
     * @param username Name of the user
     */
    public void delete(String username) {
        byte[] data = zkClient.readData("/config/users/" + username, true);

        if (data != null)   {
            log.debug("Deleting quotas for user {}", username);
            zkClient.writeData("/config/users/" + username, deleteUserJson(data));
            notifyChanges(username);
        } else {
            log.warn("Quotas for user {} already don't exist", username);
        }
    }

    /**
     * Deletes the quotas from existing JSON
     *
     * @param user JSON string with existing user configuration as byte[]
     *
     * @return  Returns the updated JSON without the quotas as byte array
     */
    protected byte[] deleteUserJson(byte[] user)   {
        JsonObject json = new JsonObject(new String(user, Charset.defaultCharset()));

        validateJsonVersion(json);

        if (json.getJsonObject("config") == null)   {
            json.put("config", new JsonObject());
        }

        if (json.getJsonObject("config").getString("producer_byte_rate") != null) {
            json.getJsonObject("config").remove("producer_byte_rate");
        }
        if (json.getJsonObject("config").getString("consumer_byte_rate") != null) {
            json.getJsonObject("config").remove("consumer_byte_rate");
        }
        if (json.getJsonObject("config").getString("request_percentage") != null) {
            json.getJsonObject("config").remove("request_percentage");
        }

        return json.encode().getBytes(Charset.defaultCharset());
    }

    protected void validateJsonVersion(JsonObject json) {
        if (json.getInteger("version") != 1) {
            throw new RuntimeException("Failed to validate the user JSON. The version is missing or has an invalid value.");
        }
    }
}

