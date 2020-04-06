/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.vertx.core.json.JsonObject;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.kafka.common.security.scram.ScramCredential;
import org.apache.kafka.common.security.scram.internals.ScramCredentialUtils;
import org.apache.kafka.common.security.scram.internals.ScramFormatter;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.Charset;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for managing Scram credentials
 */
public class ScramShaCredentials {
    private static final Logger log = LogManager.getLogger(ScramShaCredentials.class.getName());

    private final static int ITERATIONS = 4096;
    private final static int CONNECTION_TIMEOUT = 30_000;

    private final ScramMechanism mechanism = ScramMechanism.SCRAM_SHA_512;
    private ZkClient zkClient;

    public ScramShaCredentials(String zookeeperUrl, int zookeeperSessionTimeout) {
        zkClient = new ZkClient(zookeeperUrl, zookeeperSessionTimeout, CONNECTION_TIMEOUT, new BytesPushThroughSerializer());
    }

    /**
     * Create or update the SCRAM-SHA credentials for the given user.
     *
     * @param username The name of the user which should be created or updated
     * @param password The desired user password
     */
    public void createOrUpdate(String username, String password) {
        byte[] data = zkClient.readData("/config/users/" + username, true);

        if (data != null)   {
            log.debug("Updating {} credentials for user {}", mechanism.mechanismName(), username);
            zkClient.writeData("/config/users/" + username, updateUserJson(data, password));
        } else {
            log.debug("Creating {} credentials for user {}", mechanism.mechanismName(), username);
            ensurePath("/config/users");
            zkClient.createPersistent("/config/users/" + username, createUserJson(password));
        }

        notifyChanges(username);
    }

    private boolean configJsonIsEmpty(JsonObject json) {
        validateJsonVersion(json);
        JsonObject config = json.getJsonObject("config");
        return config.isEmpty();
    }

    /**
     * Delete the SCRAM-SHA credentials for the given user.
     * It is not an error if the user doesn't exist, or doesn't currently have any SCRAM-SHA credentials.
     *
     * @param username Name of the user
     */
    public void delete(String username) {
        byte[] data = zkClient.readData("/config/users/" + username, true);

        if (data != null)   {
            log.debug("Deleting {} credentials for user {}", mechanism.mechanismName(), username);
            JsonObject deletedJson = removeScramCredentialsFromUserJson(data);
            if (configJsonIsEmpty(deletedJson)) {
                zkClient.deleteRecursive("/config/users/" + username);
            } else {
                zkClient.writeData("/config/users/" + username, deletedJson.toBuffer().getBytes());
            }
            notifyChanges(username);
        } else {
            log.warn("Credentials for user {} already don't exist", username);
        }
    }

    /**
     * Determine whether the given user has SCRAM-SHA credentials.
     *
     * @param username Name of the user
     *
     * @return True if the user exists and is configured for given mechanism
     */
    public boolean exists(String username) {
        byte[] data = zkClient.readData("/config/users/" + username, true);

        if (data != null)   {
            String jsonString = new String(data, Charset.defaultCharset());
            JsonObject json = new JsonObject(jsonString);
            validateJsonVersion(json);
            JsonObject config = json.getJsonObject("config");

            if (config != null) {
                String scramCredentials = config.getString(mechanism.mechanismName());

                if (scramCredentials != null) {
                    try {
                        ScramCredentialUtils.credentialFromString(scramCredentials);
                        return true;
                    } catch (IllegalArgumentException e) {
                        log.warn("Invalid {} credentials for user {}", mechanism.mechanismName(), username);
                    }
                }
            }
        }

        return false;
    }

    /**
     * List users with SCRAM-SHA credentials
     *
     * @return List of usernames configured for given mechanism
     */
    public List<String> list() {
        List<String> result = new ArrayList<>();

        if (zkClient.exists("/config/users"))   {
            List<String> nodes = zkClient.getChildren("/config/users");

            for (String node : nodes)   {
                if (exists(node))   {
                    result.add(node);
                }
            }
        }

        return result;
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

    /* test */
    boolean isPathExist(String path)    {
        return zkClient.exists(path);
    }

    /**
     * Generates the JSON with the credentials
     *
     * @param password  Password in String format
     * @return  Returns the geenrated JSON as byte array
     */
    protected byte[] createUserJson(String password)   {
        try {
            ScramFormatter formatter = new ScramFormatter(mechanism);
            ScramCredential credentials = formatter.generateCredential(password, ITERATIONS);

            JsonObject json = new JsonObject()
                    .put("version", 1)
                    .put("config", new JsonObject().put(mechanism.mechanismName(), ScramCredentialUtils.credentialToString(credentials)));

            return json.encode().getBytes(Charset.defaultCharset());
        } catch (NoSuchAlgorithmException e)    {
            throw new RuntimeException("Failed to generate credentials", e);
        }
    }

    /**
     * Updates the SCRAM credentials in existing JSON
     *
     * @param user JSON string with existing user configuration as byte[]
     * @param password  Password in String format
     *
     * @return  Returns the updated JSON as byte array
     */
    protected byte[] updateUserJson(byte[] user, String password)   {
        JsonObject json = new JsonObject(new String(user, Charset.defaultCharset()));

        validateJsonVersion(json);

        if (json.getJsonObject("config") == null)   {
            json.put("config", new JsonObject());
        }

        try {
            ScramFormatter formatter = new ScramFormatter(mechanism);
            ScramCredential credentials = formatter.generateCredential(password, ITERATIONS);

            json.getJsonObject("config").put(mechanism.mechanismName(), ScramCredentialUtils.credentialToString(credentials));

            return json.encode().getBytes(Charset.defaultCharset());
        } catch (NoSuchAlgorithmException e)    {
            throw new RuntimeException("Failed to generate credentials", e);
        }
    }

    /**
     * Deletes the SCRAM credentials from existing JSON
     *
     * @param user JSON string with existing user configuration as byte[]
     *
     * @return  Returns the updated JSON without the SCRAM credentials
     */
    protected JsonObject removeScramCredentialsFromUserJson(byte[] user)   {
        JsonObject json = new JsonObject(new String(user, Charset.defaultCharset()));

        validateJsonVersion(json);

        if (json.getJsonObject("config") == null)   {
            json.put("config", new JsonObject());
        }

        if (json.getJsonObject("config").getString(mechanism.mechanismName()) != null) {
            json.getJsonObject("config").remove(mechanism.mechanismName());
        }

        return json;
    }

    protected void validateJsonVersion(JsonObject json)   {
        if (json.getInteger("version") != 1)    {
            throw new RuntimeException("Failed to validate the user JSON. The version is missing or has an invalid value.");
        }
    }
}

