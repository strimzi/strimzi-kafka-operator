/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.Util;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterUserScramCredentialsResult;
import org.apache.kafka.clients.admin.DescribeUserScramCredentialsResult;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.UserScramCredentialDeletion;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.apache.kafka.common.errors.ResourceNotFoundException;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.List;

public class ScramCredentialsOperator extends AbstractAdminApiOperator<String, List<String>> {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ScramCredentialsOperator.class.getName());
    private final static int ITERATIONS = 4096;
    private final static ScramMechanism SCRAM_MECHANISM = ScramMechanism.SCRAM_SHA_512;
    // Not generating new salt in every reconcile loop reduce the amount of changes (otherwise everything changes every loop)
    // This salt uses the same algorithm as Kafka
    private final static byte[] SALT =  (new BigInteger(130, new SecureRandom())).toString(36).getBytes(StandardCharsets.UTF_8);

    /**
     * Constructor
     *
     * @param vertx Vertx instance
     * @param adminClient Kafka Admin client instance
     */
    public ScramCredentialsOperator(Vertx vertx, Admin adminClient) {
        super(vertx, adminClient);
    }

    @Override
    public Future<ReconcileResult<String>> reconcile(Reconciliation reconciliation, String username, String desired) {
        if (desired != null)    {
            UserScramCredentialUpsertion upsertion = new UserScramCredentialUpsertion(username, new ScramCredentialInfo(SCRAM_MECHANISM, ITERATIONS), desired.getBytes(StandardCharsets.UTF_8), SALT);
            LOGGER.debugCr(reconciliation, "Upserting SCRAM credentials for user {}", username);
            AlterUserScramCredentialsResult result = adminClient.alterUserScramCredentials(List.of(upsertion));

            return Util.kafkaFutureToVertxFuture(reconciliation, vertx, result.all()).map(ReconcileResult.patched(desired));
        } else {
            Promise<ReconcileResult<String>> deletePromise = Promise.promise();

            UserScramCredentialDeletion deletion = new UserScramCredentialDeletion(username, SCRAM_MECHANISM);
            LOGGER.debugCr(reconciliation, "Deleting SCRAM credentials for user {}", username);
            AlterUserScramCredentialsResult result = adminClient.alterUserScramCredentials(List.of(deletion));

            result.all().whenComplete((ignore, error) -> {
                vertx.runOnContext(ignore2 -> {
                    if (error != null) {
                        if (error instanceof ResourceNotFoundException) {
                            // Resource was not found => return success
                            LOGGER.debugCr(reconciliation, "Previously deleted SCRAM credentials for user {}", username);
                            deletePromise.complete(ReconcileResult.noop(null));
                        } else {
                            LOGGER.warnCr(reconciliation, "Failed to delete SCRAM credentials for user {}", username);
                            deletePromise.fail(error);
                        }
                    } else {
                        LOGGER.debugCr(reconciliation, "Deleted SCRAM credentials for user {}", username);
                        deletePromise.complete(ReconcileResult.deleted());
                    }
                });
            });

            return deletePromise.future();
        }
    }

    /**
     * @return List with all usernames which have some scram credentials set
     */
    @Override
    public Future<List<String>> getAllUsers() {
        LOGGER.debugOp("Listing all users with SCRAM credentials");

        DescribeUserScramCredentialsResult creds = adminClient.describeUserScramCredentials();
        return Util.kafkaFutureToVertxFuture(vertx, creds.users());
    }
}
