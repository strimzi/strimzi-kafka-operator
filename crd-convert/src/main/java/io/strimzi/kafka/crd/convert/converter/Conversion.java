/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.crd.convert.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeCreator;
import io.fabric8.kubernetes.api.model.ConfigMapKeySelector;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.strimzi.api.annotations.ApiVersion;
import io.strimzi.api.kafka.model.ExternalConfigurationReference;
import io.strimzi.api.kafka.model.ExternalLogging;
import io.strimzi.api.kafka.model.Logging;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.introspector.BeanAccess;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static io.strimzi.kafka.crd.convert.converter.ConversionUtil.pathTokens;

/**
 * A {@linkplain #reverse() "reversible"} change to an object.
 * If the change encompasses all the changes to convert between one version of an object and another
 * it would be a {@link VersionConversion}.
 *
 * @param <T> The converted type
 */
public interface Conversion<T> {
    Logger log = LogManager.getLogger(Conversion.class);

    Conversion<Object> NOOP = new Conversion<>() {
        @Override
        public void convert(JsonNode node) {
        }

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
    static <T> Conversion<T> noop() {
        //noinspection rawtypes
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
        public void convert(JsonNode node) {
            String oldVersion = Converter.getApiVersion(node);
            String newVersion = Conversion.replaceVersion(oldVersion, toVersion);
            ConversionUtil.replace(node, "/apiVersion", (n, c) -> c.textNode(newVersion));
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
     * @param toVersion   The new API version.
     * @param <T>         The type of resource.
     * @return A conversion which replaces the API version with the given value
     */
    static <T extends HasMetadata> ReplaceApiVersion<T> replaceApiVersion(ApiVersion fromVersion, ApiVersion toVersion) {
        return new ReplaceApiVersion<>(fromVersion, toVersion);
    }

    /**
     * @param fromPath The path of the object to move
     * @param toPath   The path where the object should be moved to
     * @param <T>      The type of resource
     * @return A conversion which moves any object to from the given path to the given path
     * (if it is not already set).
     */
    static <T> Conversion<T> move(String fromPath, String toPath) {
        return move(fromPath, toPath, null);
    }

    /**
     * @param path the logging path
     * @param key the default key
     * @param <T>  The type of resource
     * @return replace logging conversion
     */
    static <T> Conversion<T> replaceLogging(String path, String key) {
        return replace(path, new InvertibleFunction<Logging>() {
            @Override
            public JsonNode apply(JsonNode node, JsonNodeCreator creator) {
                if (node != null) {
                    JsonNode typeNode = node.get("type");
                    if (typeNode != null && !typeNode.isNull()) {
                        String type = typeNode.asText();
                        if (ExternalLogging.TYPE_EXTERNAL.equals(type)) {
                            return ConversionUtil.replace(
                                node,
                                ExternalLogging.class,
                                this::convert
                            );
                        }
                    }
                }
                return node;
            }

            @Override
            public InvertibleFunction<Logging> inverse() {
                return this;
            }

            @Override
            public Logging apply(Logging logging) {
                if (logging instanceof ExternalLogging) {
                    ExternalLogging el = (ExternalLogging) logging;
                    convert(el);
                }
                return logging;
            }

            @SuppressWarnings("deprecation")
            private ExternalLogging convert(ExternalLogging el) {
                String name = el.getName();
                if (name != null && el.getValueFrom() == null) {
                    el.setName(null);
                    ExternalConfigurationReference ecr = new ExternalConfigurationReference();
                    ecr.setConfigMapKeyRef(new ConfigMapKeySelector(key, name, null));
                    el.setValueFrom(ecr);
                }
                return el;
            }
        });
    }

    private static <T, V> void replace(String xpath, T from, PropertyUtils pu, Function<V, V> fn, boolean instantiate) {
        try {
            List<String> toNames = pathTokens(xpath);
            Object to = from;
            for (int j = 0; j < toNames.size() - 1; j++) {
                Class<?> toClass = to.getClass();
                String toName = toNames.get(j);
                Property toProperty = pu.getProperty(toClass, toName);
                if (toProperty == null || !toProperty.isReadable()) {
                    throw new IllegalArgumentException("No such property: " + toName);
                }
                Object tmp = toProperty.get(to);
                if (tmp == null) {
                    if (instantiate) {
                        tmp = toProperty.getType().getConstructor().newInstance();
                        toProperty.set(to, tmp);
                    } else {
                        return;
                    }
                }
                to = tmp;
            }
            Property lastProperty = pu.getProperty(to.getClass(), toNames.get(toNames.size() - 1));
            @SuppressWarnings("unchecked")
            V value = (V) lastProperty.get(to);
            V newValue = fn.apply(value);
            lastProperty.set(to, newValue);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static <T> Conversion<T> move(String fromPath, String toPath, Conversion<T> inverse) {
        return new Conversion<>() {
            @Override
            public void convert(JsonNode node) {
                ConversionUtil.move(node, fromPath, toPath);
            }

            @Override
            public void convert(T from) {
                try {
                    PropertyUtils pu = new PropertyUtils();
                    pu.setBeanAccess(BeanAccess.PROPERTY);
                    List<String> fromNames = pathTokens(fromPath);
                    Object target = from;
                    for (int i = 0; i < fromNames.size(); i++) {
                        Class<?> targetClass = target.getClass();
                        String name = fromNames.get(i);
                        Property property = pu.getProperty(targetClass, name);
                        if (property == null || !property.isReadable()) {
                            break;
                        }
                        Object value = property.get(target);
                        if (value == null) {
                            break;
                        }
                        if (i == fromNames.size() - 1) {
                            property.set(target, null);
                            replace(toPath, from, pu, v -> value, true);
                            break;
                        }
                        target = value;
                    }
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new IllegalStateException(e);
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
     * @param <T>  The type of resource
     * @return A conversion which deletes anything at the given path
     */
    static <T> Conversion<T> delete(String path) {
        return replace(path, new InvertibleFunction<T>() {
            @Override
            public T apply(T t) {
                return null;
            }

            @Override
            public JsonNode apply(JsonNode node, JsonNodeCreator creator) {
                return null;
            }

            @Override
            public InvertibleFunction<T> inverse() {
                return this;
            }
        });
    }

    interface InvertibleFunction<T> extends Function<T, T> {
        JsonNode apply(JsonNode node, JsonNodeCreator creator);
        InvertibleFunction<T> inverse();
    }

    abstract class DefaultInvertibleFunction<T> implements InvertibleFunction<T> {
        abstract Class<T> convertedType();

        @Override
        public JsonNode apply(JsonNode node, JsonNodeCreator creator) {
            if (node != null && !node.isNull()) {
                return ConversionUtil.replace(
                    node,
                    convertedType(),
                    this
                );
            } else {
                return null;
            }
        }
    }

    class NoopInvertibleFunction<T> implements InvertibleFunction<T> {
        @Override
        public JsonNode apply(JsonNode node, JsonNodeCreator creator) {
            return node;
        }

        @Override
        public InvertibleFunction<T> inverse() {
            return this;
        }

        @Override
        public T apply(T t) {
            return t;
        }
    }

    /**
     * @param path        The path to replace.
     * @param replacement A function for computing the replacement for the object at the given path.
     * @param <T>         The type of resource.
     * @param <V>         The type of the replacement function parameter and result.
     * @return A conversion which replaces an object at a given path.
     */
    static <T, V> Conversion<T> replace(String path, InvertibleFunction<V> replacement) {
        return new Conversion<>() {
            @Override
            public void convert(JsonNode root) {
                Objects.requireNonNull(root);
                ConversionUtil.replace(root, path, replacement::apply);
            }

            @Override
            public void convert(T from) {
                PropertyUtils pu = new PropertyUtils();
                pu.setBeanAccess(BeanAccess.PROPERTY);
                replace(path, from, pu, replacement, false);
            }

            @Override
            public Conversion<T> reverse() {
                return replace(path, replacement.inverse());
            }
        };
    }

    /**
     * Perform some arbitrary conversion of the given node, in place.
     *
     * @param node The node to convert.
     */
    void convert(JsonNode node);

    /**
     * Perform some arbitrary conversion of the given object, in place.
     *
     * @param instance The instance to convert.
     */
    void convert(T instance);

    /**
     * Returns a conversion which performs the "reverse" of this conversion.
     * This might not be a strict mathematical bijection.
     * For example in a property is deleted between versions X and Y because it's no longer used
     * (i.e. setting it has become a no op wrt operator) then the reverse of the delete would be {@link #noop()}.
     *
     * @return The inverse of this conversion.
     */
    Conversion<T> reverse();
}

