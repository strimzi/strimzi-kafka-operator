/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.systemtest.parallel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;

public class NamespaceWatcher {

    private static final Logger LOGGER = LogManager.getLogger(NamespaceWatcher.class);
    private static final Random RNG = new Random();

    private static final int NUMBER_OF_PARALLEL_TEST_NAMESPACES_TO_WATCH = 10;
    private static final Function<Integer, Map<String, Boolean>> CONSTRUCT_MAP_OF_NAMESPACES = numberOfNamespaces -> {
        // pre-allocate array to size `numberOfNamespaces` and avoid re-allocation...
        Map<String, Boolean> parallelNamespaceTestsMap = new HashMap<>(numberOfNamespaces);
        for (int i = 0; i < numberOfNamespaces; i++) {
            // false -> namespace is by default ready
            parallelNamespaceTestsMap.put("namespace-" + i, false);
        }
        return parallelNamespaceTestsMap;
    };

    private static final Function<Integer, List<String>> CONSTRUCT_LIST_OF_NAMESPACES = numberOfNamespaces -> {
        // pre-allocate array to size `numberOfNamespaces` and avoid re-allocation...
        List<String> listOfParallelNamespaceTests = new ArrayList<>(numberOfNamespaces);
        for (int i = 0; i < numberOfNamespaces; i++) {
            listOfParallelNamespaceTests.add("namespace-" + i);
        }
        return listOfParallelNamespaceTests;
    };

    // here we will have namespace for @ParallelNamespaceTest prepared and we will pick if one is label as ready
    public final static List<String> PARALLEL_NAMESPACE_TEST_NAMES = CONSTRUCT_LIST_OF_NAMESPACES.apply(NUMBER_OF_PARALLEL_TEST_NAMESPACES_TO_WATCH);

    private Map<String, Boolean> parallelNamespaceTestsMap;

    public NamespaceWatcher() {
        this.parallelNamespaceTestsMap = CONSTRUCT_MAP_OF_NAMESPACES.apply(NUMBER_OF_PARALLEL_TEST_NAMESPACES_TO_WATCH);
    }

    /**
     * Stochastic algorithm that tries to pick free-namespace (this means that his flag is set to `false`). It will
     * be faster than classic iterative approach because we basically have 5-10 tests in parallel (where capacity is 20), which is high probability
     * to pick that free-one in best scenario resulting O(1) time complexity.
     * @return picked namespace
     */
    public synchronized String pickAndLockNamespace() throws InterruptedException {
        String pickedNamespace;

        while (true) {
            final int randomNumber = RNG.nextInt(this.parallelNamespaceTestsMap.keySet().size());
            final List<String> keyList = new ArrayList<>(this.parallelNamespaceTestsMap.keySet());
            pickedNamespace = keyList.get(randomNumber);

            if (this.parallelNamespaceTestsMap.get(pickedNamespace).equals(Boolean.FALSE)) {
                // free namespace -> will be now locked
                break;
            }

            LOGGER.info("Namespace {} is already taken by another thread. Trying again in 5s...", pickedNamespace);
            Thread.sleep(5000);
        }

        this.parallelNamespaceTestsMap.put(pickedNamespace, true);
        LOGGER.info("Current map of namespaces:\n{}", this.parallelNamespaceTestsMap::toString);
        return pickedNamespace;
    }

    public synchronized void unlockNamespace(final String namespaceName) {
        this.parallelNamespaceTestsMap.put(namespaceName, false);
    }
}
