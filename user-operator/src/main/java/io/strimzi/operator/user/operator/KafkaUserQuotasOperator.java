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
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class KafkaUserQuotasOperator {
    private static final Logger log = LogManager.getLogger(KafkaUserQuotasOperator.class.getName());

    private final static int CONNECTION_TIMEOUT = 30_000;

    private ZkClient zkClient;
    private Vertx vertx;

    public KafkaUserQuotasOperator(Vertx vertx, String zookeeperUrl, int zookeeperSessionTimeout) {
        this.zkClient = new ZkClient(zookeeperUrl, zookeeperSessionTimeout, CONNECTION_TIMEOUT, new BytesPushThroughSerializer());
        this.vertx = vertx;
    }

    Future<ReconcileResult<Void>> reconcile(String username, KafkaUserQuotas quotas) {
        Promise<ReconcileResult<Void>> prom = Promise.promise();
        
        vertx.createSharedWorkerExecutor("kubernetes-ops-pool").executeBlocking(
            future -> {
                try {
                    boolean exists = exists(username);
                    if (quotas != null) {
                        createOrUpdate(username, quotas);
                        future.complete(exists ? ReconcileResult.created(null) : ReconcileResult.patched(null));
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
        byte[] data = zkClient.readData("/config/users/" + username, true);

        if (data != null)   {
            log.debug("Updating quotas for user {}", username);
            JsonNode diff = null;
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                diff = JsonDiff.asJson(objectMapper.readTree(data), objectMapper.readTree(createUserJson(quotas)));
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (diff != null && diff.size() > 0) {
                zkClient.writeData("/config/users/" + username, updateUserJson(data, quotas));
            }
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
    protected byte[] createUserJson(KafkaUserQuotas quotas)   {
        JsonObject json = new JsonObject()
                .put("version", 1)
                .put("config", new JsonObject());

        for (Map.Entry<String, Object> quota: quotasToJson(quotas).getMap().entrySet()) {
            json.getJsonObject("config").put(quota.getKey(), quota.getValue().toString());
        }

        return json.encode().getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Updates the quotas in existing JSON
     *
     * @param user user configuration
     * @param quotas  quotas
     *
     * @return  Returns the updated JSON as byte array
     */
    protected byte[] updateUserJson(byte[] user, KafkaUserQuotas quotas)   {
        JsonObject json = new JsonObject(new String(user, StandardCharsets.UTF_8));

        validateJsonVersion(json);

        if (json.getJsonObject("config") == null)   {
            json.put("config", new JsonObject());
        }

        for (Map.Entry<String, Object> quota: quotasToJson(quotas).getMap().entrySet()) {
            json.getJsonObject("config").put(quota.getKey(), quota.getValue().toString());
        }
        return json.encode().getBytes(StandardCharsets.UTF_8);

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
        byte[] data = zkClient.readData("/config/users/" + username, true);

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
            zkClient.writeData("/config/users/" + username, removeQuotasFromJsonUser(data));
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
    protected byte[] removeQuotasFromJsonUser(byte[] user)   {
        JsonObject json = new JsonObject(new String(user, StandardCharsets.UTF_8));

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

        return json.encode().getBytes(StandardCharsets.UTF_8);
    }

    protected void validateJsonVersion(JsonObject json) {
        if (json.getInteger("version") != 1) {
            throw new RuntimeException("Failed to validate the user JSON. The version is missing or has an invalid value.");
        }
    }

    protected JsonObject getQuotas(String username) {
        byte[] data = zkClient.readData("/config/users/" + username, true);
        if (data != null) {
            String jsonString = new String(data, StandardCharsets.UTF_8);
            JsonObject json = new JsonObject(jsonString);
            return json;
        } else return null;
    }

    protected JsonObject quotasToJson(KafkaUserQuotas quotas) {
        JsonObject quotasJson = new JsonObject();
        if (quotas == null) {
            return quotasJson;
        }
        if (quotas.getProducerByteRate() != null) {
            quotasJson.put("producer_byte_rate", quotas.getProducerByteRate().toString());
        }
        if (quotas.getConsumerByteRate() != null) {
            quotasJson.put("consumer_byte_rate", quotas.getConsumerByteRate().toString());
        }
        if (quotas.getRequestPercentage() != null) {
            quotasJson.put("request_percentage", quotas.getRequestPercentage().toString());
        }
        return quotasJson;
    }
}
