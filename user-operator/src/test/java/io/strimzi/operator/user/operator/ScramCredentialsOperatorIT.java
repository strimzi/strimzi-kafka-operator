/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.user.operator;

import io.strimzi.operator.user.ResourceUtils;
import org.apache.kafka.clients.admin.UserScramCredentialsDescription;
import org.apache.kafka.common.errors.ResourceNotFoundException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

public class ScramCredentialsOperatorIT extends AdminApiOperatorIT<String, List<String>> {
    protected boolean createPatches = true;

    @Override
    AdminApiOperator<String, List<String>> operator() {
        return new ScramCredentialsOperator(adminClient, ResourceUtils.createUserOperatorConfig(), Executors.newSingleThreadExecutor());
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
            // The SCRAM-SHA credentials never return a password. So we return a dummy empty String
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
    void assertResources(String expected, String actual) {
        // The password can be never obtained again from Kafka. So there is nothing to do here
    }

    /**
     * SCRAM-SHA credentials are valid only for SCRAM users and not for TLS users. So this inherited test is disabled here.
     */
    @Test
    @Override
    public void testCreateModifyDeleteTlsUsers()    {
        // Do nothing => we just override it to have the test skipped for SCRAM-SHA credentials
    }

    // With SCRAM-SHA users, we always patch the credentials regardless whether they exist or not
    // So we override this and return true
    @Override
    public boolean createPatches()    {
        return true;
    }
}
