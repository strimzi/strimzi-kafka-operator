/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import io.strimzi.api.kafka.model.UnknownPropertyPreserving;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Member;
import java.util.List;
import java.util.Map;

public class ValidationVisitor implements ResourceVisitor.Visitor {

    private final Logger logger;

    private final String context;

    public ValidationVisitor(String context, Logger logger) {
        this.context = context;
        this.logger = logger;
    }

    private <M extends AnnotatedElement & Member> void checkForDeprecated(List<String> path,
                                                                          M member,
                                                                          Object propertyValue,
                                                                          String propertyName) {
        if (propertyValue != null
                && member.isAnnotationPresent(Deprecated.class)) {
            logger.warn("{} contains an object at path {} but the property {} is deprecated " +
                            "and will be removed in a future release",
                    context,
                    String.join(".", path) + "." + propertyName,
                    propertyName);
        }
    }

    @Override
    public <M extends AnnotatedElement & Member> void visitProperty(List<String> path, Object resource,
                                    M method, ResourceVisitor.Property<M> property, Object propertyValue) {
        checkForDeprecated(path, method, propertyValue, property.propertyName(method));
    }

    @Override
    public void visitObject(List<String> path, Object object) {
        if (object instanceof UnknownPropertyPreserving) {
            Map<String, Object> properties = ((UnknownPropertyPreserving) object).getAdditionalProperties();
            if (properties != null && !properties.isEmpty()) {
                logger.warn("{} contains object at path {} with {}: {}",
                        context,
                        String.join(".", path),
                        properties.size() == 1 ? "an unknown property" : "unknown properties",
                        String.join(", ", properties.keySet()));
            }
        }
    }
}
