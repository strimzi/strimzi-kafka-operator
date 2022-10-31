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
            if (actualValue.toString().contains("Unhandled Exception")) {
                return false;
            }
            // This pattern is used for split each log ine with stack trace if it's there from some reasons
            // It's match start of the line which contains date in format yyyy-mm-dd hh:mm:ss
            String logLineSplitPattern = "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}";
            for (String line : ((String) actualValue).split(logLineSplitPattern)) {
                if (line.contains("DEBUG") || line.contains("WARN") || line.contains("INFO")) {
                    continue;
                }
                if (line.startsWith("java.lang.NullPointerException")) {
                    return false;
                }
                String lineLowerCase = line.toLowerCase(Locale.ENGLISH);
                if (lineLowerCase.contains("error") || lineLowerCase.contains("exception")) {
                    boolean ignoreListResult = false;
                    for (LogIgnoreList value : LogIgnoreList.values()) {
                        Matcher m = Pattern.compile(value.name).matcher(line);
                        if (m.find()) {
                            ignoreListResult = true;
                            break;
                        }
                    }
                    if (!ignoreListResult) {
                        LOGGER.error(line);
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

    enum LogIgnoreList {
        CO_TIMEOUT_EXCEPTION("io.strimzi.operator.common.operator.resource.TimeoutException"),
        // "NO_ERROR" is necessary because DnsNameResolver prints debug information `QUERY(0), NoError(0), RD RA` after `received` operation
        NO_ERROR("NoError\\(0\\)"),
        // This is necessary for OCP 3.10 or less because of having exception handling during the patching of NetworkPolicy
        CAUGHT_EXCEPTION_FOR_NETWORK_POLICY("Caught exception while patching NetworkPolicy"
                + "(?s)(.*?)"
                + "io.fabric8.kubernetes.client.KubernetesClientException: Failure executing: PATCH"),
        // This happen from time to time during CO startup, it doesn't influence CO behavior
        EXIT_ON_OUT_OF_MEMORY("ExitOnOutOfMemoryError"),
        OPERATION_TIMEOUT("Util:[0-9]+ - Reconciliation #[0-9]+.*Exceeded timeout of.*while waiting for.*"),
        // This is ignored cause it's no real problem when this error appears, components are being created even after timeout
        RECONCILIATION_TIMEOUT("ERROR Abstract.*Operator:[0-9]+ - Reconciliation.*"),
        ASSEMBLY_OPERATOR_RECONCILIATION_TIMEOUT("ERROR .*AssemblyOperator:[0-9]+ - Reconciliation.*[fF]ailed.*"),
        WATCHER_CLOSED_EXCEPTION("ERROR AbstractOperator:.+ - Watcher closed with exception in namespace .*"),
        CONCURRENT_RESOURCE_DELETION("io.strimzi.operator.cluster.operator.resource.ConcurrentDeletionException"),
        RECOVERY_STS_DELETION("java\\.lang\\.IllegalStateException: Can't wait for StatefulSet: recovery-cluster-(kafka|zookeeper) in namespace: recovery-cluster-test to scale. Resource is no longer available."),

        // error that pods already exist when doing reconciliation of StrimziPodSets
        // connected to https://github.com/strimzi/strimzi-kafka-operator/issues/7529 - remove this once the issue will be fixed
        RECONCILIATION_PODSET_ALREADY_EXISTS("ERROR StrimziPodSetController:[0-9]+ - Reconciliation.*[fF]ailed"
            + "(?s)(.*?)"
            + "io.fabric8.kubernetes.client.KubernetesClientException.*already exists");

        final String name;

        LogIgnoreList(String name) {
            this.name = name;
        }
    }
}
