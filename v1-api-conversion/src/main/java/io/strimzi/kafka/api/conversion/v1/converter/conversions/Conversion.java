/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter.conversions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeCreator;
import io.strimzi.kafka.api.conversion.v1.converter.ApiConversionFailedException;
import org.yaml.snakeyaml.introspector.BeanAccess;
import org.yaml.snakeyaml.introspector.Property;
import org.yaml.snakeyaml.introspector.PropertyUtils;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static io.strimzi.kafka.api.conversion.v1.converter.conversions.ConversionUtil.pathTokens;

/**
 * Interface for conversion between two versions of a Strimzi resource
 *
 * @param <T> The converted type
 */
public interface Conversion<T> {
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
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Moves fields from one path to another. Currently unused, but might be useful in the future.
     *
     * @param fromPath The path of the object to move
     * @param toPath   The path where the object should be moved to
     * @param <T>      The type of resource
     *
     * @return  A conversion which moves any object to from the given path to the given path (if it is not already set).
     */
    @SuppressWarnings("unused") // Might be useful one day
    static <T> Conversion<T> move(String fromPath, String toPath) {
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
                            Object originalValue = getValueFromPath(from, toPath, pu);

                            if (originalValue != null)  {
                                throw new ApiConversionFailedException("Cannot move " + fromPath + " to " + toPath + ". The target path already exists. Please resolve the issue manually and run the API conversion tool again.");
                            }

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

            private Object getValueFromPath(T from, String path, PropertyUtils pu)   {
                List<String> pathNames = pathTokens(path);
                Object target = from;

                for (int i = 0; i < pathNames.size(); i++) {
                    Class<?> targetClass = target.getClass();
                    String name = pathNames.get(i);
                    Property property = pu.getProperty(targetClass, name);

                    if (property == null || !property.isReadable()) {
                        return null;
                    }

                    Object value = property.get(target);

                    if (value == null) {
                        return null;
                    }

                    if (i == pathNames.size() - 1) {
                        return value;
                    }

                    target = value;
                }

                return null;
            }
        };
    }

    /**
     * Deletes the object at a given path
     *
     * @param path The path to delete
     * @param <T>  The type of resource
     *
     * @return  A conversion which deletes anything at the given path
     */
    static <T> Conversion<T> delete(String path) {
        return replace(path, new ConversionFunction<T>() {
            @Override
            public T apply(T t) {
                return null;
            }

            @Override
            public JsonNode apply(JsonNode node, JsonNodeCreator creator) {
                return null;
            }
        });
    }

    /**
     * Conversion function for converting a typed or JSON based resource
     *
     * @param <T>   Type that this function can convert
     */
    interface ConversionFunction<T> extends Function<T, T> {
        /**
         * Applies the conversion to a typed object
         *
         * @param t     Object to apply the conversion on
         *
         * @return  Converted instance of the object
         */
        T apply(T t);

        /**
         * Applies the conversion to a JSON object
         *
         * @param node      JSON Object to apply the conversion on
         * @param creator   JSOn Creator instance
         *
         * @return  Converted JSON Object
         */
        JsonNode apply(JsonNode node, JsonNodeCreator creator);
    }

    /**
     * The default conversion function is used to map between the JSON object and the Strimzi API Object. That is useful
     * so that we can write a conversion only once for the Strimzi API objects, and it is applied to JSON (YAML)
     * resources as well.
     *
     * @param <T>   Type that this conversion function converts
     */
    abstract class DefaultConversionFunction<T> implements ConversionFunction<T> {
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

    /**
     * @param path        The path to replace.
     * @param replacement A function for computing the replacement for the object at the given path.
     * @param <T>         The type of resource.
     * @param <V>         The type of the replacement function parameter and result.
     * @return A conversion which replaces an object at a given path.
     */
    static <T, V> Conversion<T> replace(String path, ConversionFunction<V> replacement) {
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
}