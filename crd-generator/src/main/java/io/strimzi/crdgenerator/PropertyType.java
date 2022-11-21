/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.crdgenerator;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.List;

class PropertyType {

    private final Class<?> type;
    private final Type gType;

    PropertyType(Class<?> type, Type gType) {
        this.type = type;
        this.gType = gType;
    }

    public Class<?> getType() {
        return type;
    }

    public Type getGenericType() {
        return gType;
    }

    public boolean isArray() {
        //Class<?> propertyType = getType();
        //return propertyType.isArray() || List.class.equals(propertyType);
        boolean b = gType instanceof Class<?> && ((Class<?>) gType).equals(List.class)
                || gType instanceof ParameterizedType && ((ParameterizedType) gType).getRawType().equals(List.class);
        return gType instanceof GenericArrayType
                || gType instanceof Class<?> && ((Class<?>) gType).isArray()
                || b;
    }

    public int arrayDimension() {

        int result = 0;
        Type t = gType;
        while (true) {
            if (t instanceof Class<?>) {
                Class<?> c = (Class<?>) t;
                if (c.isArray()) {
                    t = c.getComponentType();
                    result++;
                } else if (List.class.equals(c)) {
                    // Raw list type!
                    result++;
                    break;
                } else {
                    break;
                }
            } else if (t instanceof GenericArrayType) {
                GenericArrayType g = (GenericArrayType) t;
                t = g.getGenericComponentType();
                result++;
            } else if (t instanceof ParameterizedType) {
                ParameterizedType pt = (ParameterizedType) t;
                if (List.class.equals(pt.getRawType())) {
                    t = pt.getActualTypeArguments()[0];
                    result++;
                } else {
                    break;
                }
            } else if (t instanceof WildcardType) {
                WildcardType wt = (WildcardType) t;
                t = wt.getUpperBounds()[0];
            } else {
                break;
            }
        }
        return result;
    }

    public Class<?> arrayBase() {
        if (!isArray()) {
            return null;
        }
        Class<?> result;
        Type t = gType;
        while (true) {
            if (t instanceof Class<?>) {
                Class<?> c = (Class<?>) t;
                if (c.isArray()) {
                    t = c.getComponentType();
                } else if (List.class.equals(c)) {
                    // Raw list type!
                    result = Object.class;
                    break;
                } else {
                    result = c;
                    break;
                }
            } else if (t instanceof GenericArrayType) {
                GenericArrayType g = (GenericArrayType) t;
                t = g.getGenericComponentType();
            } else if (t instanceof ParameterizedType) {
                ParameterizedType pt = (ParameterizedType) t;
                if (List.class.equals(pt.getRawType())) {
                    t = pt.getActualTypeArguments()[0];
                } else {
                    Type rt = pt.getRawType();
                    if (rt instanceof Class<?>) {
                        result = (Class<?>) rt;
                        break;
                    } else if (rt instanceof TypeVariable) {
                        t = ((TypeVariable) rt).getBounds()[0];
                    } else if (rt instanceof WildcardType) {
                        t = ((WildcardType) rt).getUpperBounds()[0];
                    } else {
                        result = null;
                        break;
                    }
                }
            } else if (t instanceof TypeVariable) {
                Type type = ((TypeVariable) t).getBounds()[0];
                if (type instanceof Class<?>) {
                    result = (Class<?>) type;
                    break;
                } else if (type instanceof TypeVariable<?>) {
                    t = type;
                }
            } else if (t instanceof WildcardType) {
                t = ((WildcardType) t).getUpperBounds()[0];
            } else {
                result = null;
                break;
            }
        }
        return result;
    }

    public boolean isEnum() {
        return this.type.isEnum();
    }

    public Enum[] getEnumElements() {
        if (isEnum()) {
            try {
                Method valuesMethod = this.getType().getMethod("values");
                return (Enum[]) valuesMethod.invoke(null);
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        } else {
            return new Enum[0];
        }
    }
}
