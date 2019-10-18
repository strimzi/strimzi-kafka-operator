/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.matchers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>A LogHasNoUnexpectedErrors is custom matcher to check log form kubernetes client
 * doesn't have any unexpected errors. </p>
 */
public class LogHasNoUnexpectedErrors extends BaseMatcher<String> {

    private static final Logger LOGGER = LogManager.getLogger(LogHasNoUnexpectedErrors.class);

    @Override
    public boolean matches(Object actualValue) {
        if (!"".equals(actualValue)) {
            if (actualValue.toString().contains("NullPointer") || actualValue.toString().contains("Unhandled Exception")) {
                return false;
            }
            for (String line : ((String) actualValue).split("\n")) {
                if (line.contains("DEBUG")) {
                    continue;
                }
                String lineLowerCase = line.toLowerCase(Locale.ENGLISH);
                if (lineLowerCase.contains("error") || lineLowerCase.contains("exception")) {
                    boolean whiteListResult = false;
                    for (LogWhiteList value : LogWhiteList.values()) {
                        Matcher m = Pattern.compile(value.name).matcher(line);
                        if (m.find()) {
                            whiteListResult = true;
                        }
                    }
                    if (!whiteListResult) {
                        return false;
                    }
                }
            }
            return true;
        }
        return true;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("The log should not contain unexpected errors.");
    }

    enum LogWhiteList {
        CO_TIMEOUT_EXCEPTION("io.strimzi.operator.common.operator.resource.TimeoutException"),
        // "NO_ERROR" is necessary because DnsNameResolver prints debug information `QUERY(0), NoError(0), RD RA` after `recived` operation
        NO_ERROR("NoError\\(0\\)"),
        // This is necessary for OCP 3.10 or less because of having exception handling during the patching of NetworkPolicy
        CAUGHT_EXCEPTION_FOR_NETWORK_POLICY("Caught exception while patching NetworkPolicy"
                + "(?s)(.*?)"
                + "io.fabric8.kubernetes.client.KubernetesClientException: Failure executing: PATCH"),
        // fabric8 now throws exceptions, which doesn't influence kafka scaleup/scaledown
        FABRIC_EIGHT_SCALEUP_ERROR("ERROR StatefulSetOperationsImpl:[0-9]+ - Error while waiting for resource to be scaled."),
        FABRIC_EIGHT_STATEFUL_SET_SCALEUP_ERROR("ERROR StatefulSetOperationsImpl:[0-9]+ - 0/.* pod(s).*after waiting for 0 seconds so giving up"),
        // This happen from time to time during CO startup, it doesn't influence CO behavior
        EXIT_ON_OUT_OF_MEMORY("ExitOnOutOfMemoryError"),
        OPERATION_TIMEOUT("Util:[0-9]+ - Exceeded timeout of.*while waiting for.*"),
        // This is whitelisted cause it's no real problem when this error appears, components are being created even after timeout
        RECONCILIATION_TIMEOUT("ERROR Abstract.*Operator:[0-9]+ - Reconciliation.*");

        final String name;

        LogWhiteList(String name) {
            this.name = name;
        }
    }
}
