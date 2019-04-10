/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.extensions;

import io.strimzi.test.annotations.OpenShiftOnly;
import io.strimzi.test.k8s.KubeClusterResource;
import io.strimzi.test.k8s.Minishift;
import io.strimzi.test.k8s.OpenShift;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

/**
 * A test extension which checks if tests should be ignored or not by annotation {@link OpenShiftOnly}.
 * JUnit5 annotation {@link Tag} can be used for execute/skip specific
 * test classes and/or test methods.
 */
public class StrimziExtension implements ExecutionCondition {
    private static final Logger LOGGER = LogManager.getLogger(StrimziExtension.class);

    /**
     * If env var NOTEARDOWN is set to any value then teardown for resources supported by annotations
     * won't happen. This can be useful in debugging a single test, because it leaves the cluster
     * in the state it was in when the test failed.
     */
    public static final String NOTEARDOWN = "NOTEARDOWN";
    public static final String TOPIC_CM = "../examples/topic/kafka-topic.yaml";
    private static final String DEFAULT_TAG = "";
    private static final String TAG_LIST_NAME = "junitTags";

    /**
     * Tag for acceptance tests, which are triggered for each push/pr/merge on travis-ci
     */
    public static final String ACCEPTANCE = "acceptance";
    /**
     * Tag for regression tests which are stable.
     */
    public static final String REGRESSION = "regression";
    /**
     * Tag for tests, which results are not 100% reliable on all testing environments.
     */
    public static final String FLAKY = "flaky";
    /**
     * Tag for tests, which are failing only on CCI VMs
     */
    public static final String CCI_FLAKY = "cci_flaky";

    private KubeClusterResource clusterResource;
    private Class testClass;
    private Collection<String> declaredTags;
    private Collection<String> enabledTags;

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        if (context.getElement().get() instanceof Class) {
            saveTestingClassInfo(context);
            if (!isWrongClusterType(context) && !areAllChildrenIgnored(context)) {
                return ConditionEvaluationResult.enabled("Test class is enabled");
            }
            return ConditionEvaluationResult.disabled("Test class is disabled");
        } else {
            if (!isIgnoredByTag(context) && !isWrongClusterType(context)) {
                return ConditionEvaluationResult.enabled("Test method is enabled");
            }
            return ConditionEvaluationResult.disabled("Test method is disabled");
        }
    }

    /**
     * Check if currently executed test has @OpenShiftOnly annotation
     * @param context extension context
     * @return true or false
     */
    private boolean isWrongClusterType(ExtensionContext context) {
        AnnotatedElement element = context.getElement().get();
        return isWrongClusterType(element);
    }

    /**
     * Check if current element has @OpenShiftOnly annotation
     * @param element test method/test class
     * @return true or false
     */
    private boolean isWrongClusterType(AnnotatedElement element) {
        boolean result = element.getAnnotation(OpenShiftOnly.class) != null
                && !(clusterResource().cluster() instanceof OpenShift
                || clusterResource().cluster() instanceof Minishift);
        if (result) {
            LOGGER.info("{} is @OpenShiftOnly, but the running cluster is not OpenShift: Ignoring {}",
                    name(testClass),
                    name(element)
            );
        }
        return result;
    }

    private void saveTestingClassInfo(ExtensionContext context) {
        testClass = context.getTestClass().orElse(null);
        declaredTags = context.getTags();
        enabledTags = getEnabledTags();
    }

    /**
     * Checks if some method of current test class is enabled.
     * @param context test context
     * @return true or false
     */
    private boolean areAllChildrenIgnored(ExtensionContext context) {
        if (enabledTags.isEmpty()) {
            LOGGER.info("Test class {} with tags {} does not have any tag restrictions by tags: {}",
                    context.getDisplayName(), declaredTags, enabledTags);
            return false;
        } else {
            if (containsAny(enabledTags, declaredTags) || declaredTags.isEmpty()) {
                LOGGER.info("Test class {} with tags {} does not have any tag restrictions by tags: {}. Checking method tags ...",
                        context.getDisplayName(), declaredTags, enabledTags);
                for (Method method : testClass.getDeclaredMethods()) {
                    if (method.getAnnotation(Test.class) == null && method.getAnnotation(ParameterizedTest.class) == null) {
                        continue;
                    }
                    if (!isWrongClusterType(method) && !isIgnoredByTag(method)) {
                        LOGGER.info("One of the test group {} is enabled for test: {} with tags {} in class: {}",
                                enabledTags, method.getName(), declaredTags, context.getDisplayName());
                        return false;
                    }
                }
            }
        }
        LOGGER.info("None test from class {} is enabled for tags {}", context.getDisplayName(), enabledTags);
        return true;
    }

    /**
     * Check if passed element is ignored by set tags.
     * @param context extension context
     * @return true or false
     */
    private boolean isIgnoredByTag(ExtensionContext context) {
        AnnotatedElement element = context.getElement().get();
        return isIgnoredByTag(element);
    }

    /**
     * Check if passed element is ignored by set tags.
     * @param element test method or class
     * @return true or false
     */
    private boolean isIgnoredByTag(AnnotatedElement element) {
        Tag[] annotations = element.getDeclaredAnnotationsByType(Tag.class);

        if (annotations.length == 0 || enabledTags.isEmpty()) {
            LOGGER.info("Test method {} is not ignored by tag", ((Method) element).getName());
            return false;
        }

        for (Tag annotation : annotations) {
            if (enabledTags.contains(annotation.value())) {
                LOGGER.info("Test method {} is not ignored by tag: {}", ((Method) element).getName(), annotation.value());
                return false;
            }
        }
        LOGGER.info("Test method {} is ignored by tag", ((Method) element).getName());
        return true;
    }

    /**
     * Get the value of the KubeClusterResource field with name 'cluster'
     */
    private KubeClusterResource clusterResource() {
        if (clusterResource == null) {
            try {
                Field field = testClass.getField("CLUSTER");
                clusterResource = (KubeClusterResource) field.get(KubeClusterResource.class);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                e.printStackTrace();
            }

            if (clusterResource == null) {
                clusterResource = new KubeClusterResource();
                clusterResource.before();
            }
        }
        return clusterResource;
    }

    private static Collection<String> getEnabledTags() {
        return splitProperties((String) System.getProperties().getOrDefault(TAG_LIST_NAME, DEFAULT_TAG));
    }

    private static Collection<String> splitProperties(String commaSeparated) {
        if (commaSeparated == null || commaSeparated.trim().isEmpty()) {
            return Collections.emptySet();
        }
        return new HashSet<>(Arrays.asList(commaSeparated.split(",+")));
    }

    private String name(AnnotatedElement a) {
        if (a instanceof Class) {
            return "class " + ((Class) a).getSimpleName();
        } else if (a instanceof Method) {
            Method method = (Method) a;
            return "method " + method.getDeclaringClass().getSimpleName() + "." + method.getName() + "()";
        } else if (a instanceof Field) {
            Field field = (Field) a;
            return "field " + field.getDeclaringClass().getSimpleName() + "." + field.getName();
        } else {
            return a.toString();
        }
    }

    private boolean containsAny(Collection<String> enabledTags, Collection<String> declaredTags) {
        for (String declaredTag : declaredTags) {
            if (enabledTags.contains(declaredTag)) {
                return true;
            }
        }
        return false;
    }
}
