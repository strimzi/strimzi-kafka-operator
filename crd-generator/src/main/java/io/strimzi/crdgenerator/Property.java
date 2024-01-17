/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.crdgenerator.annotations.PresentInVersions;

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

    private final AnnotatedElement a;
    private final Member m;
    private final PropertyType type;

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
                && isReallyGetterName(method, name)
                || name.startsWith("is")
                && name.length() > 2
                && method.getReturnType().equals(boolean.class);
    }

    private static boolean isReallyGetterName(Method method, String name) {
        return !"getClass".equals(name)
                && !("getDeclaringClass".equals(name) && Enum.class.equals(method.getDeclaringClass()));
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

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    static Map<String, Property> properties(ApiVersion crApiVersion, Class<?> crdClass) {
        TreeMap<String, Property> unordered = new TreeMap<>();
        for (Method method : crdClass.getMethods()) {
            Class<?> returnType = method.getReturnType();
            boolean isGetter = isGetterName(method)
                    && method.getParameterCount() == 0
                    && !returnType.equals(void.class);
            boolean isNotInherited = !(hasMethod(CustomResource.class, method) && !method.getName().equals("getSpec") && !method.getName().equals("getStatus"))
                    && !hasMethod(HasMetadata.class, method)
                    && !method.isBridge();
            boolean isNotIgnored = !hasJsonIgnore(method)
                    && !hasAnyGetter(method)
                    && isPresentInVersion(crApiVersion, method);
            if (isGetter
                    && isNotInherited
                    && isNotIgnored) {
                Property property = new Property(method);
                Property existing = unordered.put(property.getName(), property);
                if (existing != null) {
                    throw new RuntimeException("Duplicate property " + method.getName());
                }
            }
        }
        for (Field field : crdClass.getFields()) {
            boolean isProperty = !Modifier.isStatic(field.getModifiers());
            boolean isNotIgnored = !field.isAnnotationPresent(JsonIgnore.class)
                    && isPresentInVersion(crApiVersion, field);
            if (isProperty && isNotIgnored) {
                Property property = new Property(field);
                Property existing = unordered.put(property.getName(), property);
                if (existing != null) {
                    throw new RuntimeException("Duplicate property " + field.getName());
                }
            }
        }
        JsonPropertyOrder order = crdClass.getAnnotation(JsonPropertyOrder.class);
        return sortedProperties(order != null ? order.value() : null, unordered);
    }

    private static boolean isPresentInVersion(ApiVersion crApiVersion, AnnotatedElement method) {
        PresentInVersions annotation = method.getAnnotation(PresentInVersions.class);
        if (annotation == null) {
            return true;
        } else {
            return ApiVersion.parseRange(annotation.value()).contains(crApiVersion);
        }
    }

    private static boolean hasAnyGetter(Method method) {
        JsonAnyGetter annotation = findAnnotation(JsonAnyGetter.class, method, method.getDeclaringClass());
        return annotation != null && annotation.enabled();
    }

    private static boolean hasAnySetter(Method method) {
        JsonAnySetter annotation = findAnnotation(JsonAnySetter.class, method, method.getDeclaringClass());
        return annotation != null && annotation.enabled();
    }

    private static boolean hasJsonIgnore(Method method) {
        JsonIgnore annotation = findAnnotation(JsonIgnore.class, method, method.getDeclaringClass());
        return annotation != null && annotation.value();
    }

    private static <A extends Annotation> A findAnnotation(Class<A> annotationClass, Method method, Class<?> c) {
        do {
            A a = methodAnnotation(annotationClass, method, c);
            if (a != null) {
                return a;
            }
            for (Class<?> iface : c.getInterfaces()) {
                a = findAnnotation(annotationClass, method, iface);
                if (a != null) {
                    return a;
                }
            }
            c = c.getSuperclass();
        } while (c != null);
        return null;
    }

    private static <A extends Annotation> A methodAnnotation(Class<A> annotationClass, Method method, Class<?> lookupClass) {
        try {
            Method m = lookupClass.getMethod(method.getName(), method.getParameterTypes());
            A a = m.getAnnotation(annotationClass);
            if (a != null) {
                return a;
            }
        } catch (NoSuchMethodException e) {
            // ignore
        }
        return null;
    }

    private static boolean isAnySetter(Method method) {
        return method.getParameterCount() == 2
                && method.getReturnType().equals(void.class)
                && method.getParameterTypes()[0].equals(String.class)
                && method.getParameterTypes()[1].equals(Object.class)
                && !Modifier.isStatic(method.getModifiers());
    }

    private static boolean isAnyGetter(Method method) {
        return method.getParameterCount() == 0
                && !method.getReturnType().equals(void.class)
                && !Modifier.isStatic(method.getModifiers());
    }

    static Map<String, Property> sortedProperties(String[] order, TreeMap<String, Property> unordered) {
        Map<String, Property> result;

        if (order != null) {
            LinkedHashMap<String, Property> ordered = new LinkedHashMap<>(unordered.size());
            for (String propertyName : order) {
                Property property = unordered.remove(propertyName);
                if (property != null) {
                    ordered.put(propertyName, property);
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

    static boolean hasAnyGetterAndAnySetter(Class<?> crdClass) {
        boolean anyGetter = false;
        boolean anySetter = false;
        for (Method method : crdClass.getMethods()) {
            if (isAnySetter(method) &&
                    hasAnySetter(method)) {
                anySetter = true;
            }
            if (isAnyGetter(method) &&
                    hasAnyGetter(method)) {
                anyGetter = true;
            }
            if (anyGetter && anySetter) {
                break;
            }
        }
        return anyGetter && anySetter;
    }

    private static boolean hasMethod(Class<?> c, Method m) {
        try {
            if (!c.isAssignableFrom(m.getDeclaringClass()))
                return false;

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
