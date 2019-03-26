/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.api.annotations.DeprecatedProperty;
import io.strimzi.api.kafka.model.UnknownPropertyPreserving;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Member;
import java.util.List;
import java.util.Map;

public class ValidationVisitor implements ResourceVisitor.Visitor {

    private final Logger logger;
    private final HasMetadata resource;

    public ValidationVisitor(HasMetadata resource, Logger logger) {
        this.resource = resource;
        this.logger = logger;
    }

    String context() {
        return resource.getKind() + " resource " + resource.getMetadata().getName()
                + " in namespace " + resource.getMetadata().getNamespace();
    }

    private <M extends AnnotatedElement & Member> void checkForDeprecated(List<String> path,
                                                                          M member,
                                                                          Object propertyValue,
                                                                          String propertyName) {
        if (propertyValue != null) {
            DeprecatedProperty deprecated = member.getAnnotation(DeprecatedProperty.class);
            if (deprecated != null) {
                String msg = String.format("In API version %s the property %s at path %s has been deprecated. ",
                        resource.getApiVersion(),
                        propertyName,
                        path(path, propertyName));
                if (!deprecated.movedToPath().isEmpty()) {
                    msg += "This feature should now be configured at path " + deprecated.movedToPath() + ".";
                }
                if (!deprecated.description().isEmpty()) {
                    msg += " " + deprecated.description();
                }
                if (!deprecated.removalVersion().isEmpty()) {
                    msg += " This property is scheduled for removal in version " + deprecated.removalVersion() + ".";
                }
                logger.warn("{}: {}", context(), msg);
            }
        }
    }

    private String path(List<String> path, String propertyName) {
        return String.join(".", path) + "." + propertyName;
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
                logger.warn("{}: Contains object at path {} with {}: {}",
                        context(),
                        String.join(".", path),
                        properties.size() == 1 ? "an unknown property" : "unknown properties",
                        String.join(", ", properties.keySet()));
            }
        }
    }
}
