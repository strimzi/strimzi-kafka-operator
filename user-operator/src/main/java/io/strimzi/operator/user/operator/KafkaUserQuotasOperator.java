/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.zjsonpatch.JsonDiff;
import io.strimzi.api.kafka.model.KafkaUserQuotas;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class KafkaUserQuotasOperator {
    private static final Logger log = LogManager.getLogger(KafkaUserQuotasOperator.class.getName());

    private final static int CONNECTION_TIMEOUT = 30_000;

    private ZkClient zkClient;
    private Vertx vertx;

    public KafkaUserQuotasOperator(Vertx vertx, String zookeeperUrl, int zookeeperSessionTimeout) {
        this.zkClient = new ZkClient(zookeeperUrl, zookeeperSessionTimeout, CONNECTION_TIMEOUT, new BytesPushThroughSerializer());
        this.vertx = vertx;
    }

    Future<ReconcileResult<KafkaUserQuotas>> reconcile(String username, KafkaUserQuotas quotas) {
        Promise<ReconcileResult<KafkaUserQuotas>> prom = Promise.promise();
        
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                try {
                    boolean exists = exists(username);
                    if (quotas != null) {
                        createOrUpdate(username, quotas);
                        future.complete(exists ? ReconcileResult.created(quotas) : ReconcileResult.patched(quotas));
                    } else {
                        if (exists) {
                            delete(username);
                            future.complete(ReconcileResult.deleted());
                        } else {
                            future.complete(ReconcileResult.noop(null));
                        }
                    }
                } catch (Throwable t) {
                    prom.fail(t);
                }
            },
            false,
            prom);

        return prom.future();
    }

    /**
     * Create or update the quotas for the given user.
     *
     * @param username The name of the user which should be created or updated
     * @param quotas The desired user quotas
     */
    public void createOrUpdate(String username, KafkaUserQuotas quotas) {
        String encodedUsername = encodeUsername(username);

        byte[] data = zkClient.readData("/config/users/" + encodedUsername, true);

        if (data != null)   {
            log.debug("Checking quota updates for user {}", username);
            JsonNode diff = null;

            try {
                ObjectMapper objectMapper = new ObjectMapper();
                diff = JsonDiff.asJson(objectMapper.readTree(data), objectMapper.readTree(createOrUpdateUserJson(data, quotas)));
            } catch (IOException e) {
                log.error("Failed to diff user configuration for user {}", username, e);
            }

            if (diff != null && diff.size() > 0) {
                log.debug("Updating quotas for user {}", username);
                zkClient.writeData("/config/users/" + encodedUsername, createOrUpdateUserJson(data, quotas));
                notifyChanges(username);
            } else {
                log.debug("Nothing to update in quotas for user {}", username);
            }
        } else {
            log.debug("Creating quotas for user {}", username);
            ensurePath("/config/users");
            zkClient.createPersistent("/config/users/" + encodedUsername, createUserJson(quotas));
            notifyChanges(username);
        }
    }

    /**
     * Generates the JSON with the credentials
     *
     * @param quotas  quotas
     * @return  Returns the generated JSON as byte array
     */
    protected byte[] createUserJson(KafkaUserQuotas quotas)   {
        return createOrUpdateUserJson(null, quotas);
    }

    /**
     * Updates the quotas in existing JSON. If the existing JSON is null, new JSON will be created.
     *
     * @param user user configuration
     * @param quotas  quotas
     *
     * @return  Returns the updated JSON as byte array
     */
    protected byte[] createOrUpdateUserJson(byte[] user, KafkaUserQuotas quotas)   {
        JsonObject json;

        if (user != null) {
            json = new JsonObject(new String(user, StandardCharsets.UTF_8));
            validateJsonVersion(json);
        } else {
            json = new JsonObject()
                    .put("version", 1)
                    .put("config", new JsonObject());
        }

        JsonObject config = json.getJsonObject("config", new JsonObject());

        if (quotas != null && quotas.getProducerByteRate() != null) {
            config.put("producer_byte_rate", quotas.getProducerByteRate().toString());
        } else {
            if (config.getString("producer_byte_rate") != null) {
                config.remove("producer_byte_rate");
            }
        }

        if (quotas != null && quotas.getConsumerByteRate() != null) {
            config.put("consumer_byte_rate", quotas.getConsumerByteRate().toString());
        } else {
            if (config.getString("consumer_byte_rate") != null) {
                config.remove("consumer_byte_rate");
            }
        }

        if (quotas != null && quotas.getRequestPercentage() != null) {
            config.put("request_percentage", quotas.getRequestPercentage().toString());
        } else {
            if (config.getString("request_percentage") != null) {
                config.remove("request_percentage");
            }
        }

        json.put("config", config);

        return json.encode().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * This notifies Kafka about the changes we have made
     *
     * @param username  Name of the user whose configuration changed
     */
    private void notifyChanges(String username) {
        String encodedUsername = encodeUsername(username);

        log.debug("Notifying changes for user {}", username);

        ensurePath("/config/changes");

        JsonObject json = new JsonObject().put("version", 2).put("entity_path", "users/" + encodedUsername);
        zkClient.createPersistentSequential("/config/changes/config_change_", json.encode().getBytes(StandardCharsets.UTF_8));
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
     * Determine whether the given user has quotas.
     *
     * @param username Name of the user
     *
     * @return True if the user exists
     */
    boolean exists(String username) {
        String encodedUsername = encodeUsername(username);

        byte[] data = zkClient.readData("/config/users/" + encodedUsername, true);

        if (data != null)   {
            String jsonString = new String(data, StandardCharsets.UTF_8);
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

    private boolean configJsonIsEmpty(JsonObject json) {
        validateJsonVersion(json);
        JsonObject config = json.getJsonObject("config");
        return config.isEmpty();
    }

    /**
     * Delete the quotas for the given user.
     * It is not an error if the user doesn't exist, or doesn't currently have any quotas.
     *
     * @param username Name of the user
     */
    public void delete(String username) {
        String encodedUsername = encodeUsername(username);

        byte[] data = zkClient.readData("/config/users/" + encodedUsername, true);

        if (data != null)   {
            log.debug("Deleting quotas for user {}", username);
            JsonObject deleteJson = removeQuotasFromJsonUser(data);
            if (configJsonIsEmpty(deleteJson)) {
                zkClient.deleteRecursive("/config/users/" + encodedUsername);
                log.debug("User {} deleted from ZK store", username);
            } else {
                zkClient.writeData("/config/users/" + encodedUsername, deleteJson.toBuffer().getBytes());
            }
            notifyChanges(username);
        } else {
            log.warn("Quotas for user {} already don't exist", username);
        }
    }

    /**
     * Deletes the quotas from existing JSON
     *
     * @param userConfig JSON string with existing userConfig configuration as byte[]
     *
     * @return  Returns the updated JSON without the quotas
     */
    protected JsonObject removeQuotasFromJsonUser(byte[] userConfig)   {
        JsonObject json = new JsonObject(new String(userConfig, StandardCharsets.UTF_8));

        validateJsonVersion(json);
        JsonObject config = json.getJsonObject("config");
        if (config == null) {
            json.put("config", new JsonObject());
        } else {
            if (config.getString("producer_byte_rate") != null) {
                config.remove("producer_byte_rate");
            }
            if (config.getString("consumer_byte_rate") != null) {
                config.remove("consumer_byte_rate");
            }
            if (config.getString("request_percentage") != null) {
                config.remove("request_percentage");
            }
        }
        return json;
    }

    protected void validateJsonVersion(JsonObject json) {
        if (json.getInteger("version") != 1) {
            throw new RuntimeException("Failed to validate the user JSON. The version is missing or has an invalid value.");
        }
    }

    protected JsonObject getQuotas(String username) {
        String encodedUsername = encodeUsername(username);

        byte[] data = zkClient.readData("/config/users/" + encodedUsername, true);

        if (data != null) {
            String jsonString = new String(data, StandardCharsets.UTF_8);
            JsonObject json = new JsonObject(jsonString);
            return json;
        } else return null;
    }

    /**
     * Encodes the username with URL Encoder
     *
     * @param username  Username which should be encoded
     * @return          Encoded username
     */
    protected static String encodeUsername(String username) {
        try {
            return URLEncoder.encode(username, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Failed to encode username", e);
        }
    }
}
