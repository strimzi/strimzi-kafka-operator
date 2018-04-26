/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test;

import org.junit.Assume;
import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

public class JUnitGroupRule implements MethodRule {

    @Override
    public Statement apply(Statement base, FrameworkMethod method, Object target) {
        JUnitGroup testGroup = method.getAnnotation(JUnitGroup.class);

        if (testGroup == null) {
            return base;
        }

        Collection<String> enabledGroups = getEnabledGroups(testGroup.key());
        Collection<String> declaredGroups = getDeclaredGroups(testGroup);

        if (isGroupEnabled(enabledGroups, declaredGroups)) {
            return base;
        }

        return new IgnoreStatement(enabledGroups, declaredGroups);
    }

    static class IgnoreStatement extends Statement {

        Collection<String> enabledGroups;
        Collection<String> testGroups;

        public IgnoreStatement(Collection<String> enabledGroups, Collection<String> testGroups) {
            this.enabledGroups = enabledGroups;
            this.testGroups = testGroups;
        }

        @Override
        public void evaluate() throws Throwable {
            Assume.assumeTrue("None of the test groups " + this.testGroups + " are enabled. Enabled test groups: " + this.enabledGroups, false);
        }
    }

    private static Collection<String> getEnabledGroups(String key) {
        Collection<String> enabledGroups = splitProperties(System.getProperty(key));

        return enabledGroups;
    }

    private static Collection<String> getDeclaredGroups(JUnitGroup testGroup) {
        String[] declaredGroups = testGroup.value();
        if (declaredGroups.length != 0) {
            return new HashSet<>(Arrays.asList(declaredGroups));
        }

        return Collections.emptyList();
    }

    /**
     * A test group is enabled if {@link JUnitGroup#ALL_GROUPS} is defined or
     * the declared test groups contain at least one defined test group
     * @param enabledGroups Test groups that are enabled for execution.
     * @param declaredGroups Test groups that are declared in the {@link JUnitGroup} annotation.
     * @return boolean value with actual status
     */
    private static boolean isGroupEnabled(Collection<String> enabledGroups, Collection<String> declaredGroups) {
        if (enabledGroups.contains(JUnitGroup.ALL_GROUPS) || (enabledGroups.isEmpty() && declaredGroups.isEmpty())) {
            return true;
        }

        for (String enabledGroup : enabledGroups) {
            if (declaredGroups.contains(enabledGroup)) {
                return true;
            }
        }

        return false;
    }

    private static Collection<String> splitProperties(String commaSeparated) {
        if (commaSeparated == null || commaSeparated.trim().isEmpty()) {
            return Collections.emptySet();
        }
        return new HashSet<>(Arrays.asList(commaSeparated.split(",+")));
    }
}
