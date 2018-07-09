/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.CustomResource;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

class Property implements AnnotatedElement {
    private final String name;
    private final Class<?> owner;

    private AnnotatedElement a;
    private Member m;
    private PropertyType type;
    public Property(Method method) {
        name = propertyName(method);
        owner = method.getDeclaringClass();
        a = method;
        m = method;
        this.type = new PropertyType(method.getReturnType(), method.getGenericReturnType());
    }

    public Property(Field field) {
        name = field.getName();
        owner = field.getDeclaringClass();
        a = field;
        m = field;
        this.type = new PropertyType(field.getType(), field.getGenericType());
    }

    public String getName() {
        return name;
    }

    private static boolean isGetterName(Method method) {
        String name = method.getName();
        return name.startsWith("get")
                && name.length() > 3
                && !"getClass".equals(name)
                || name.startsWith("is")
                && name.length() > 2
                && method.getReturnType().equals(boolean.class);
    }

    private static String propertyName(Method getterMethod) {
        JsonProperty jsonProperty = getterMethod.getAnnotation(JsonProperty.class);
        if (jsonProperty != null
                && !jsonProperty.value().isEmpty()) {
            return jsonProperty.value();
        } else {
            String name = getterMethod.getName();
            if (name.startsWith("get")
                    && name.length() > 3) {
                return name.substring(3, 4).toLowerCase(Locale.ENGLISH) + name.substring(4);
            } else if (name.startsWith("is")
                    && name.length() > 2
                    && getterMethod.getReturnType().equals(boolean.class)) {
                return name.substring(2, 3).toLowerCase(Locale.ENGLISH) + name.substring(3);
            } else {
                return null;
            }
        }
    }

    static Map<String, Property> properties(Class<?> crdClass) {
        TreeMap<String, Property> unordered = new TreeMap<>();
        for (Method method : crdClass.getMethods()) {
            Class<?> returnType = method.getReturnType();
            boolean isGetter = isGetterName(method)
                    && method.getParameterCount() == 0
                    && !returnType.equals(void.class);
            boolean isNotInherited = !hasMethod(CustomResource.class, method)
                    && !hasMethod(HasMetadata.class, method);
            boolean isNotIgnored = !method.isAnnotationPresent(JsonIgnore.class);
            if (isGetter
                    && isNotInherited
                    && isNotIgnored) {
                Property property = new Property(method);
                unordered.put(property.getName(), property);
            }
        }
        for (Field field : crdClass.getFields()) {
            boolean isProperty = !Modifier.isStatic(field.getModifiers());
            boolean isNotIgnored = !field.isAnnotationPresent(JsonIgnore.class);
            if (isProperty && isNotIgnored) {
                Property property = new Property(field);
                unordered.put(property.getName(), property);
            }
        }
        JsonPropertyOrder order = crdClass.getAnnotation(JsonPropertyOrder.class);
        return sortedProperties(order != null ? order.value() : null, unordered);
    }

    static Map<String, Property> sortedProperties(String[] order, TreeMap<String, Property> unordered) {
        Map<String, Property> result;
        if (order != null) {
            LinkedHashMap<String, Property> ordered = new LinkedHashMap<>(unordered.size());
            if (order.length > 0) {
                for (String propertyName : order) {
                    Property property = unordered.remove(propertyName);
                    if (property != null) {
                        ordered.put(propertyName, property);
                    }
                }
            }
            // The rest in alphabetic order, irrespective of unordered.alphabetic
            ordered.putAll(unordered);
            result = ordered;
        } else {
            result = unordered;
        }
        return unmodifiableMap(result);
    }

    private static boolean hasMethod(Class<?> c, Method m) {
        try {
            c.getDeclaredMethod(m.getName(), m.getParameterTypes());
            return true;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    static boolean isPolymorphic(Class<?> cls) {
        return cls.isAnnotationPresent(JsonSubTypes.class);
    }

    static Map<Class<?>, String> subtypeMap(Class<?> crdClass) {
        JsonSubTypes subtypes = crdClass.getAnnotation(JsonSubTypes.class);
        if (subtypes != null) {
            LinkedHashMap<Class<?>, String> result = new LinkedHashMap<>(subtypes.value().length);
            for (JsonSubTypes.Type type : subtypes.value()) {
                result.put(type.value(), type.name());
            }
            return result;
        } else {
            return emptyMap();
        }
    }

    static List<Class<?>> subtypes(Class<?> crdClass) {
        JsonSubTypes subtypes = crdClass.getAnnotation(JsonSubTypes.class);
        if (subtypes != null) {
            ArrayList<Class<?>> result = new ArrayList<>(subtypes.value().length);
            for (JsonSubTypes.Type type : subtypes.value()) {
                result.add(type.value());
            }
            return result;
        } else {
            return emptyList();
        }
    }

    static List<String> subtypeNames(Class<?> crdClass) {
        JsonSubTypes subtypes = crdClass.getAnnotation(JsonSubTypes.class);
        if (subtypes != null) {
            ArrayList<String> result = new ArrayList<>(subtypes.value().length);
            for (JsonSubTypes.Type type : subtypes.value()) {
                result.add(type.name());
            }
            return result;
        } else {
            return emptyList();
        }
    }

    static String discriminator(Class<?> returnType) {
        if (returnType != null) {
            JsonTypeInfo annotation = returnType.getAnnotation(JsonTypeInfo.class);
            if (annotation != null) {
                return annotation.property();
            }
        }
        return null;
    }

    boolean isDiscriminator() {
        return getName().equals(discriminator(m.getDeclaringClass()));
    }

    @Override
    public <T extends Annotation> T getAnnotation(Class<T> annotationClass) {
        return a.getAnnotation(annotationClass);
    }

    @Override
    public Annotation[] getAnnotations() {
        return a.getAnnotations();
    }

    @Override
    public Annotation[] getDeclaredAnnotations() {
        return a.getDeclaredAnnotations();
    }

    public Class<?> getDeclaringClass() {
        return m.getDeclaringClass();
    }

    public PropertyType getType() {
        return type;
    }

    public String toString() {
        return owner.getName() + "." + name;
    }
}
