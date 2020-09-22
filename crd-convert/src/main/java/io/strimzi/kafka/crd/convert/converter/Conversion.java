/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.converter;

import java.lang.reflect.Method;
import java.util.Locale;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.api.annotations.ApiVersion;
import org.apache.commons.jxpath.AbstractFactory;
import org.apache.commons.jxpath.JXPathContext;
import org.apache.commons.jxpath.JXPathNotFoundException;
import org.apache.commons.jxpath.Pointer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A {@linkplain #reverse() "reversible"} change to an object.
 * If the change encompasses all the changes to convert between one version of an object and another
 * it would be a {@link VersionConversion}.
 * @param <T> The converted type
 */
public interface Conversion<T> {

    static final Conversion<Object> NOOP = new Conversion<Object>() {
        @Override
        public void convert(Object instance) {
        }

        @Override
        public Conversion<Object> reverse() {
            return this;
        }
    };

    /**
     * @param <T> The type of the converted object.
     * @return The no-op conversion.
     */
    @SuppressWarnings("unchecked")
    public static <T> Conversion<T> noop() {
        return (Conversion) NOOP;
    }

    /**
     * @param apiVersion The group/version
     * @param newVersion The new version
     * @return a new "group/version" with the given version
     */
    private static String replaceVersion(String apiVersion, ApiVersion newVersion) {
        if (apiVersion == null || !apiVersion.matches("[a-z.]+/v[0-9]+((alpha|beta)[0-9]+)?")) {
            throw new RuntimeException("Bad api version " + apiVersion);
        }
        return apiVersion.substring(0, apiVersion.indexOf("/") + 1) + newVersion;
    }

    class ReplaceApiVersion<T extends HasMetadata> implements Conversion<T> {
        private final ApiVersion fromVersion;
        private final ApiVersion toVersion;

        public ReplaceApiVersion(ApiVersion fromVersion, ApiVersion toVersion) {
            this.fromVersion = fromVersion;
            this.toVersion = toVersion;
        }

        public ApiVersion getFromVersion() {
            return fromVersion;
        }

        public ApiVersion getToVersion() {
            return toVersion;
        }

        @Override
        public void convert(T from) {
            from.setApiVersion(Conversion.replaceVersion(from.getApiVersion(), toVersion));
        }

        @Override
        public Conversion<T> reverse() {
            return replaceApiVersion(toVersion, fromVersion);
        }
    }


    /**
     * @param fromVersion The old API version.
     * @param toVersion The new API version.
     * @param <T> The type of resource.
     * @return A conversion which replaces the API version with the given value
     */
    public static <T extends HasMetadata> ReplaceApiVersion<T> replaceApiVersion(ApiVersion fromVersion, ApiVersion toVersion) {
        return new ReplaceApiVersion<>(fromVersion, toVersion);
    }

    /**
     * @param fromPath The path of the object to move
     * @param toPath The path where the object should be moved to
     * @param <T> The type of resource
     * @return A conversion which moves any object to from the given path to the given path
     * (if it is not already set).
     */
    public static <T> Conversion<T> move(String fromPath, String toPath) {
        return move(fromPath, toPath, null);
    }

    public static <T> Conversion<T> move(String fromPath, String toPath, Conversion<T> inverse) {
        Logger logger = LogManager.getLogger(Conversion.class);
        return new Conversion<T>() {
            @Override
            public void convert(T from) {
                JXPathContext context = JXPathContext.newContext(from);
                context.setFactory(new AbstractFactory() {
                    @Override
                    public boolean createObject(JXPathContext context, Pointer pointer, Object parent, String name, int index) {
                        try {
                            Method declaredMethod = parent.getClass().getDeclaredMethod("get" + name.substring(0, 1).toUpperCase(Locale.ENGLISH) + name.substring(1));
                            Class<?> returnType = declaredMethod.getReturnType();
                            Object newInstance = returnType.getConstructor().newInstance();
                            pointer.setValue(newInstance);
                            return true;
                        } catch (ReflectiveOperationException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
                Pointer fromPointer;
                try {
                    fromPointer = context.getPointer(fromPath);
                } catch (JXPathNotFoundException e) {
                    return;
                }
                Pointer toPointer;
                try {
                    toPointer = context.getPointer(toPath);
                } catch (JXPathNotFoundException e) {
                    toPointer = null;
                }

                if (toPointer == null || toPointer.getValue() == null) {
                    Object value = fromPointer.getValue();
                    context.createPathAndSetValue(toPath, value);
                    fromPointer.setValue(null);
                } else {
                    // TODO More context to the logging, so the user know which resource was effected
                    logger.warn("Path {} already has a value, {}, so cannot move the value at path {} there",
                            toPath, toPointer.getValue(), fromPath);
                }

            }

            @Override
            public Conversion<T> reverse() {
                return inverse == null ? move(toPath, fromPath) : Conversion.noop();
            }
        };
    }

    /**
     * @param path The path to delete
     * @param <T> The type of resource
     * @return A conversion which deletes anything at the given path
     */
    public static <T> Conversion<T> delete(String path) {
        // TODO delete is just replace with a function that returns null
        class Delete implements Conversion<T> {
            @Override
            public void convert(T from) {
                JXPathContext context = JXPathContext.newContext(from);
                try {
                    Pointer pointer = context.getPointer(path);
                    pointer.setValue(null);
                } catch (JXPathNotFoundException e) {

                }
            }

            @Override
            public Conversion<T> reverse() {
                return new Conversion<T>() {
                    @Override
                    public void convert(T instance) {
                    }
                    @Override
                    public Conversion<T> reverse() {
                        return Delete.this;
                    }
                };
            }
        }
        return new Delete();
    }

    static interface InvertibleFunction<T> {
        T apply(T t);
        InvertibleFunction<T> inverse();
    }

    /**
     * @param path The path to replace.
     * @param replacement A function for computing the replacement for the object at the given path.
     * @param <T> The type of resource.
     * @param <V> The type of the replacement function parameter and result.
     * @return A conversion which replaces an object at a given path.
     */
    @SuppressWarnings("unchecked")
    public static <T, V> Conversion<T> replace(String path, InvertibleFunction<V> replacement) {
        return new Conversion<T>() {
            @Override
            public void convert(T from) {
                JXPathContext context = JXPathContext.newContext(from);
                try {
                    Pointer pointer = context.getPointer(path);
                    V value = replacement.apply((V) pointer.getValue());
                    pointer.setValue(value);
                } catch (JXPathNotFoundException e) {

                }
            }

            @Override
            public Conversion<T> reverse() {
                return replace(path, replacement.inverse());
            }
        };
    }

    /**
     * Perform some arbitrary conversion of the given object, in place.
     * @param instance The instance to convert.
     */
    void convert(T instance);

    /**
     * Returns a conversion which performs the "reverse" of this conversion.
     * This might not be a strict mathematical bijection.
     * For example in a property is deleted between versions X and Y because it's no longer used
     * (i.e. setting it has become a no op wrt operator) then the reverse of the delete would be {@link #noop()}.
     * @return The inverse of this conversion.
     */
    Conversion<T> reverse();
}

