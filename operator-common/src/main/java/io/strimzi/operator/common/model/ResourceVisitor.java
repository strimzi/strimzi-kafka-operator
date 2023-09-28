/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.common.model;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Resource visitor used to validate resources
 */
public class ResourceVisitor {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(ResourceVisitor.class);

    /**
     * Interface defining the visitor
     */
    public interface Visitor {
        /**
         * Called when a property is visited.
         * @param reconciliation The reconciliation
         * @param path The property path for reaching this property
         * @param owner The object with the property
         * @param method The getter method for the property.
         * @param property abstraction for using the method.
         * @param propertyValue The value of the property.
         */
        default void visitMethodProperty(Reconciliation reconciliation, List<String> path, Object owner,
                                 Method method, Property<Method> property, Object propertyValue) {
            visitProperty(reconciliation, path, owner, method, property, propertyValue);
        }

        /**
         * Called when a field property is visited.
         * @param reconciliation The reconciliation
         * @param path The property path for reaching this property
         * @param owner The object with the property
         * @param field The field for the property.
         * @param property abstraction for using the method.
         * @param propertyValue The value of the property.
         */
        default void visitFieldProperty(Reconciliation reconciliation, List<String> path, Object owner,
                                Field field, Property<Field> property, Object propertyValue) {
            visitProperty(reconciliation, path, owner, field, property, propertyValue);
        }

        /**
         * Called when a property is visited.
         * @param reconciliation The reconciliation
         * @param path The property path for reaching this property
         * @param owner The object with the property
         * @param member The getter method or field for the property.
         * @param property abstraction for using the method.
         * @param propertyValue The value of the property.
         * @param <M> The type of member ({@code Field} or {@code Method}).
         */
        <M extends AnnotatedElement & Member> void visitProperty(Reconciliation reconciliation, List<String> path, Object owner,
                                        M member, Property<M> property, Object propertyValue);

        /**
         * Called when an object is visited.
         * @param reconciliation The reconciliation
         * @param path The property path to this object.
         * @param object The object
         */
        void visitObject(Reconciliation reconciliation, List<String> path, Object object);
    }

    /**
     * Visits a field in the resource
     *
     * @param reconciliation    Reconciliation marker
     * @param resource          Resource which should be visited
     * @param visitor           Visitor class
     *
     * @param <T>               Type of the resource which is being validated
     */
    public static <T extends HasMetadata> void visit(Reconciliation reconciliation, T resource, Visitor visitor) {
        ArrayList<String> path = new ArrayList<>();
        try {
            visit(reconciliation, path, resource, visitor);
        } catch (RuntimeException | ReflectiveOperationException | StackOverflowError e) {
            LOGGER.errorCr(reconciliation, "Error while visiting {}", path, e);
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private static void visit(Reconciliation reconciliation, List<String> path, Object resource, Visitor visitor) throws ReflectiveOperationException {
        Class<?> cls = resource.getClass();
        visitor.visitObject(reconciliation, path, resource);
        for (Field field : cls.getFields()) {
            Object propertyValue = field.get(resource);
            visitor.visitFieldProperty(reconciliation, path, resource, field, FIELD_PROPERTY, propertyValue);
            visitProperty(reconciliation, path, field, FIELD_PROPERTY, propertyValue, visitor);
        }
        for (Method method : cls.getMethods()) {
            String name = method.getName();
            if (!"getClass".equals(name)) {
                Property<Method> property = null;
                if (name.length() > 3
                        && name.startsWith("get")
                        && !method.getReturnType().equals(Void.class)
                        && method.getParameterCount() == 0) {
                    property = GET_METHOD_PROPERTY;
                } else if (name.length() > 2
                        && name.startsWith("is")
                        && method.getReturnType().equals(boolean.class)
                        && method.getParameterCount() == 0) {
                    property = IS_METHOD_PROPERTY;
                }
                if (property != null) {
                    Object propertyValue = method.invoke(resource);
                    visitor.visitMethodProperty(reconciliation, path, resource, method, property, propertyValue);
                    visitProperty(reconciliation, path, method, property, propertyValue, visitor);
                }
            }
        }
    }

    private static boolean isScalar(Class<?> returnType) {
        boolean isInteger = Short.class.equals(returnType)
                || Integer.class.equals(returnType)
                || Long.class.equals(returnType);
        boolean isFloat = Float.class.equals(returnType)
                || Double.class.equals(returnType);
        return returnType.isPrimitive()
                || String.class.equals(returnType)
                || Boolean.class.equals(returnType)
                || isInteger
                || Byte.class.equals(returnType)
                || isFloat;
    }

    static <M extends AnnotatedElement & Member> void visitProperty(Reconciliation reconciliation, List<String> path, M member,
                                                                    Property<M> property, Object propertyValue,
                                                                    Visitor visitor)
            throws ReflectiveOperationException {
        String propertyName = property.propertyName(member);
        Class<?> returnType = property.type(member);
        if (propertyValue != null) {
            if (returnType.isArray()) {
                path.add(propertyName);
                if (propertyValue instanceof Object[]) {
                    for (Object element : (Object[]) propertyValue) {
                        visit(reconciliation, path, element, visitor);
                    }
                }
                // otherwise it's an array of primitives, in which case there are not further objects to visit
                path.remove(path.size() - 1);
            } else if (Collection.class.isAssignableFrom(returnType)) {
                path.add(propertyName);
                for (Object element : (Collection<?>) propertyValue) {
                    if (element != null
                            && !element.getClass().isEnum()
                            && !isScalar(element.getClass())) {
                        visit(reconciliation, path, element, visitor);
                    }
                }
                path.remove(path.size() - 1);
            } else if (!isScalar(returnType)
                    && !Map.class.isAssignableFrom(returnType)
                    && !returnType.isEnum()) {
                path.add(propertyName);
                visit(reconciliation, path, propertyValue, visitor);
                path.remove(path.size() - 1);
            }
        }
    }

    /**
     * Property interface
     *
     * @param <M>   Type of the member
     */
    public interface Property<M extends Member> {
        /**
         * Name of the property
         *
         * @param i     Identifier of the property
         *
         * @return  Name of the property
         */
        String propertyName(M i);

        /**
         * Type of the property
         *
         * @param i     Identifier of the property
         *
         * @return  Type of the property
         */
        Class<?> type(M i);
    }

    static class MethodProperty implements Property<Method> {
        private final boolean getPrefix;
        public MethodProperty(boolean getPrefix) {
            this.getPrefix = getPrefix;
        }

        @Override
        public String propertyName(Method i) {
            String name = i.getName();
            String propertyName = name.substring(getPrefix ? 3 : 2);
            return propertyName.substring(0, 1).toLowerCase(Locale.ENGLISH) + propertyName.substring(1);
        }

        @Override
        public Class<?> type(Method i) {
            return i.getReturnType();
        }
    }

    private static final MethodProperty GET_METHOD_PROPERTY = new MethodProperty(true);
    private static final MethodProperty IS_METHOD_PROPERTY = new MethodProperty(false);

    private static final Property<Field> FIELD_PROPERTY = new Property<>() {
        @Override
        public String propertyName(Field i) {
            return i.getName();
        }

        @Override
        public Class<?> type(Field i) {
            return i.getType();
        }
    };

}
