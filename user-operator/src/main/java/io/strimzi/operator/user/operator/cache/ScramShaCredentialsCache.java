/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator.cache;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.common.KafkaFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A periodically updated SCRAM-SHA Cache for keeping a track of which users have SCRAM-SHA credentials locally and
 * avoid querying Kafka. Since the credentials cannot be listed, it only keeps a boolean to indicate if the credentials
 * exist or not.
 */
public class ScramShaCredentialsCache extends AbstractCache<Boolean> {
    private final static Logger LOGGER = LogManager.getLogger(ScramShaCredentialsCache.class);

    private final Admin adminClient;

    /**
     * Constructs the Scram-SHA credentials cache
     *
     * @param adminClient           Kafka Admin client
     * @param refreshIntervalMs     Interval in which the cache should be refreshed
     */
    public ScramShaCredentialsCache(Admin adminClient, long refreshIntervalMs) {
        super("ScramShaCredentials", refreshIntervalMs);
        this.adminClient = adminClient;
    }

    /**
     * Loads the SCRAM-SHA credentials from Kafka for all users
     *
     * @return  ConcurrentHashMap with Boolean values indicating if the user has SCRAM-SHA credentials set.
     */
    @Override
    protected ConcurrentHashMap<String, Boolean> loadCache() {
        KafkaFuture<List<String>> futureUsers = adminClient.describeUserScramCredentials().users();

        try {
            List<String> users = futureUsers.get(1, TimeUnit.MINUTES);
            ConcurrentHashMap<String, Boolean> map = new ConcurrentHashMap<>((int) (users.size() / 0.75f));
            users.forEach(u -> map.put(u, true));
            return map;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOGGER.warn("Failed to load SCRAM-SHA credentials", e);
            throw new RuntimeException("Failed to load SCRAM-SHA credentials", e);
        }
    }
}
