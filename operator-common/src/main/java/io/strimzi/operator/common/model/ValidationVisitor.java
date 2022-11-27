/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.api.annotations.DeprecatedProperty;
import io.strimzi.api.annotations.DeprecatedType;
import io.strimzi.api.kafka.model.UnknownPropertyPreserving;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.strimzi.operator.common.operator.resource.StatusUtils;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Member;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Validation visitor
 */
public class ValidationVisitor implements ResourceVisitor.Visitor {
    private final ReconciliationLogger logger;
    private final HasMetadata resource;
    private final Set<Condition> warningConditions;
    private final String transitionTime = StatusUtils.iso8601Now();

    /**
     * Constructs the validation visitor
     *
     * @param resource              Kubernetes resource
     * @param logger                Reconciliation logger
     * @param warningConditions     Warning conditions with validation warnings
     */
    public ValidationVisitor(HasMetadata resource, ReconciliationLogger logger, Set<Condition> warningConditions) {
        this.resource = resource;
        this.logger = logger;
        this.warningConditions = warningConditions;
    }

    <M extends AnnotatedElement & Member> boolean isPresent(M member,
                      Object propertyValue) {
        JsonInclude annotation = member.getAnnotation(JsonInclude.class);
        if (annotation != null) {
            if (propertyValue == null) {
                return false;
            }
            switch (annotation.value()) {
                case NON_ABSENT:
                    // Technically we should handle Optional and AtomicReference
                    // but we're not using these types in the api module, so just fall through
                case NON_EMPTY:
                    if (propertyValue instanceof Collection) {
                        return !((Collection) propertyValue).isEmpty();
                    } else if (propertyValue instanceof Map) {
                        return !((Map) propertyValue).isEmpty();
                    } else if (propertyValue instanceof String) {
                        return !((String) propertyValue).isEmpty();
                    } else if (propertyValue instanceof Object[]) {
                        return ((Object[]) propertyValue).length != 0;
                    } else if (propertyValue.getClass().isArray()) {
                        // primitive arrays
                        try {
                            return ((int) propertyValue.getClass().getField("length").get(propertyValue)) != 0;
                        } catch (ReflectiveOperationException e) {
                            return false;
                        }
                    }
            }
        }
        return propertyValue != null;
    }

    private <M extends AnnotatedElement & Member> void checkForDeprecated(Reconciliation reconciliation,
                                                                          List<String> path,
                                                                          M member,
                                                                          Object propertyValue,
                                                                          String propertyName) {
        // Look for deprecated field
        DeprecatedProperty deprecated = member.getAnnotation(DeprecatedProperty.class);
        if (deprecated != null
            && isPresent(member, propertyValue)) {
            String msg = String.format("In API version %s the %s property at path %s has been deprecated",
                    resource.getApiVersion(),
                    propertyName,
                    path(path, propertyName));
            if (!deprecated.movedToPath().isEmpty()) {
                msg += ", and should now be configured using " + deprecated.movedToPath() + "";
            }
            if (!deprecated.removalVersion().isEmpty()) {
                msg += ". This property is removed in API version " + deprecated.removalVersion();
            }
            msg += ".";
            if (!deprecated.description().isEmpty()) {
                msg += " " + deprecated.description();
            }

            warningConditions.add(StatusUtils.buildWarningCondition("DeprecatedFields", msg, transitionTime));
            logger.warnCr(reconciliation, msg);
        }

        // Look for deprecated objects. With OneOf, the field might not be deprecated, but the used value might be
        // replaced with something new
        if (propertyValue != null) {
            DeprecatedType deprecatedType = propertyValue.getClass().getAnnotation(DeprecatedType.class);
            if (deprecatedType != null
                    && isPresent(member, propertyValue)) {
                String msg = String.format("In API version %s the object %s at path %s has been deprecated. ",
                        resource.getApiVersion(),
                        propertyName,
                        path(path, propertyName));
                if (deprecatedType.replacedWithType() != null) {
                    msg += "This object has been replaced with " + deprecatedType.replacedWithType().getSimpleName();
                }
                if (!deprecatedType.removalVersion().isEmpty()) {
                    msg += " and is removed in API version " + deprecatedType.removalVersion();
                }
                msg += ".";

                warningConditions.add(StatusUtils.buildWarningCondition("DeprecatedObjects", msg, transitionTime));
                logger.warnCr(reconciliation, msg);
            }
        }
    }

    private String path(List<String> path, String propertyName) {
        return String.join(".", path) + "." + propertyName;
    }

    @Override
    public <M extends AnnotatedElement & Member> void visitProperty(Reconciliation reconciliation, List<String> path, Object resource,
                                    M method, ResourceVisitor.Property<M> property, Object propertyValue) {
        checkForDeprecated(reconciliation, path, method, propertyValue, property.propertyName(method));
    }

    @Override
    public void visitObject(Reconciliation reconciliation, List<String> path, Object object) {
        if (object instanceof UnknownPropertyPreserving) {
            Map<String, Object> properties = ((UnknownPropertyPreserving) object).getAdditionalProperties();
            if (properties != null && !properties.isEmpty()) {
                String msg = String.format("Contains object at path %s with %s: %s",
                        String.join(".", path),
                        properties.size() == 1 ? "an unknown property" : "unknown properties",
                        String.join(", ", properties.keySet()));

                logger.warnCr(reconciliation, msg);
                warningConditions.add(StatusUtils.buildWarningCondition("UnknownFields", msg, transitionTime));
            }
        }
    }
}
