/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.kafka.clients.admin.UserScramCredentialsDescription;
import org.apache.kafka.common.errors.ResourceNotFoundException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.concurrent.ExecutionException;

@ExtendWith(VertxExtension.class)
public class ScramCredentialsOperatorIT extends AbstractAdminApiOperatorIT<String, List<String>> {
    @Override
    AbstractAdminApiOperator<String, List<String>> operator() {
        return new ScramCredentialsOperator(vertx, adminClient);
    }

    @Override
    String getOriginal() {
        return "dummyPassword";
    }

    @Override
    String getModified() {
        return "modifiedDummyPassword";
    }

    @Override
    String get(String username) {
        try {
            UserScramCredentialsDescription result = adminClient.describeUserScramCredentials(List.of(username)).description(username).get();
            // The SCRAM-SHA credentials never return back an password. So we return a dummy empty String
            return result != null ? "" : null;
        } catch (ResourceNotFoundException e) {
            // Admin API throws an exception when the resource is not found. We transform it into null
            return null;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof ResourceNotFoundException)  {
                // Admin API throws an exception when the resource is not found. We transform it into null
                return null;
            } else {
                throw new RuntimeException("Failed to get Scram credentials", e);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed to get Scram credentials", e);
        }
    }

    @Override
    void assertResources(VertxTestContext context, String expected, String actual) {
        // The password can be never obtained again from Kafka. So there is nothing to do here
    }

    /**
     * SCRAM-SHA credentials are valid only for SCRAM users and not for TLS users. So this inherited test is disabled here.
     *
     * @param context   Test context
     */
    @Test
    @Override
    public void testCreateModifyDeleteTlsUsers(VertxTestContext context)    {
        context.completeNow();
    }
}
