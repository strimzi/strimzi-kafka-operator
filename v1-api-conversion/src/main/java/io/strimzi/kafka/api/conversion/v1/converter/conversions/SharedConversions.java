/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter.conversions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeCreator;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fabric8.kubernetes.client.CustomResource;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthentication;
import io.strimzi.api.kafka.model.common.authentication.KafkaClientAuthenticationOAuth;
import io.strimzi.api.kafka.model.common.tracing.Tracing;
import io.strimzi.api.kafka.model.connect.KafkaConnectSpec;
import io.strimzi.kafka.api.conversion.v1.converter.ApiConversionFailedException;

/**
 * Class for holding the various conversions that are not specific to a single custom resource and apply to more of them.
 */
@SuppressWarnings("deprecation")
public class SharedConversions {
    private SharedConversions() { }

    /**
     * Conversion for deleting the `type: jaeger` tracing
     *
     * @return  The conversion
     *
     * @param <T> Type of the resource where the conversion should be applied
     */
    public static <T> Conversion<T> deleteJaegerTracing() {
        return Conversion.replace("/spec/tracing", new Conversion.ConversionFunction<T>() {
            @Override
            public T apply(T t) {
                if (t == null) {
                    return null;
                } else if (t instanceof Tracing tracing) {
                    if (tracing.getType() != null && "jaeger".equals(tracing.getType())) {
                        return null;
                    } else {
                        return t;
                    }
                } else {
                    throw new ApiConversionFailedException("Cannot delete Jaeger Tracing from " + t.getClass().getName());
                }
            }

            @Override
            public JsonNode apply(JsonNode node, JsonNodeCreator creator) {
                if (node == null || node.isNull()) {
                    return null;
                }

                JsonNode typeNode = node.get("type");
                if (typeNode != null && !typeNode.isNull() && "jaeger".equals(typeNode.asText())) {
                    return null;
                } else {
                    return node;
                }
            }
        });
    }

    /**
     * Sets the default replicas
     *
     * @param <T>             Type of the resource where the conversion should be applied
     * @param defaultReplicas The default number of replicas (differs per resource type)
     * @return The conversion
     */
    public static <T> Conversion<T> defaultReplicas(int defaultReplicas) {
        return Conversion.replace("/spec", new Conversion.ConversionFunction<T>() {
            @Override
            public T apply(T t) {
                return t;
            }

            @Override
            public JsonNode apply(JsonNode node, JsonNodeCreator creator) {
                if (node == null || node.isNull()) {
                    return null;
                }

                JsonNode replicasNode = node.get("replicas");
                if (replicasNode == null || replicasNode.isNull()) {
                    if (node instanceof ObjectNode) {
                        ((ObjectNode) node).put("replicas", defaultReplicas);
                    } else {
                        throw new ApiConversionFailedException("Cannot cast JsonNode to ObjectNode when fixing default replicas.");
                    }
                }

                return node;
            }
        });
    }

    /**
     * Enforces that OAuth authentication type in client authentication is removed. This has to be done manually by the
     * user. So if `type: oauth` authentication is found, it will raise an error and fail.
     *
     * @return  The conversion
     *
     * @param <T> Type of the resource where the conversion should be applied
     */
    public static <T> Conversion<T> enforceOAuthClientAuthentication() {
        return Conversion.replace("/spec/authentication", new Conversion.ConversionFunction<T>() {
            @Override
            public T apply(T t) {
                if (t == null) {
                    return null;
                } else if (t instanceof KafkaClientAuthentication auth) {
                    if (KafkaClientAuthenticationOAuth.TYPE_OAUTH.equals(auth.getType())) {
                        throw new ApiConversionFailedException("The OAuth authentication is removed in the v1 API version. Use the Custom authentication instead. Please fix the resource manually and re-run the conversion tool.");
                    }
                } else {
                    throw new ApiConversionFailedException("Cannot enforce OAuth authentication in " + t.getClass().getName());
                }

                return t;
            }

            @Override
            public JsonNode apply(JsonNode node, JsonNodeCreator creator) {
                if (node == null || node.isNull()) {
                    return null;
                }

                JsonNode type = node.get("type");
                if (type != null && !type.isNull() && KafkaClientAuthenticationOAuth.TYPE_OAUTH.equals(type.asText())) {
                    throw new ApiConversionFailedException("The OAuth authentication is removed in the v1 API version. Use the Custom authentication instead. Please fix the resource manually and re-run the conversion tool.");
                }

                return node;
            }
        });
    }

    /**
     * Enforces that external configuration in Kafka Connect .spec is removed. This has to be done manually by the
     * user. So if `.spec.externalConfiguration` is found, it will raise an error and fail.
     *
     * @return  The conversion
     *
     * @param <T>   Type of the resource where the conversion should be applied
     */
    public static <T> Conversion<T> enforceExternalConfiguration() {
        return Conversion.replace("/spec", new Conversion.ConversionFunction<T>() {
            @Override
            public T apply(T t) {
                if (t == null) {
                    return null;
                } else if (t instanceof KafkaConnectSpec spec) {
                    if (spec.getExternalConfiguration() != null) {
                        throw new ApiConversionFailedException("The External Configuration is removed in the v1 API version. Use the .spec.template section instead. Please fix the resource manually and re-run the conversion tool.");
                    }
                } else {
                    throw new ApiConversionFailedException("Cannot enforce External Configuration in " + t.getClass().getName());
                }

                return t;
            }

            @Override
            public JsonNode apply(JsonNode node, JsonNodeCreator creator) {
                if (node == null || node.isNull()) {
                    return null;
                }

                JsonNode type = node.get("externalConfiguration");
                if (type != null && !type.isNull()) {
                    throw new ApiConversionFailedException("The External Configuration is removed in the v1 API version. Use the .spec.template section instead. Please fix the resource manually and re-run the conversion tool.");
                }

                return node;
            }
        });
    }

    /**
     * Enforces that the .spec section is present. It fails if it is missing.
     *
     * @return  The conversion
     *
     * @param <T>   Type of the resource where the conversion should be applied
     */
    public static <T extends CustomResource<?, ?>> Conversion<T> enforceSpec() {
        return new Conversion<>() {
            @Override
            public void convert(JsonNode node) {
                if (node == null || node.isNull()) {
                    throw new ApiConversionFailedException("Cannot check .spec section presence in a null resource");
                } else {
                    JsonNode spec = node.get("spec");
                    if (spec == null || spec.isNull()) {
                        throw new ApiConversionFailedException("The .spec section is required and has to be added manually to " + node.get("kind").asText() + " kind resources. Please fix the resource manually and re-run the conversion tool.");
                    }
                }
            }

            @Override
            public void convert(T resource) {
                if (resource == null) {
                    throw new ApiConversionFailedException("Cannot check .spec section presence in a null resource");
                } else {
                    if (resource.getSpec() == null) {
                        throw new ApiConversionFailedException("The .spec section is required and has to be added manually to " + resource.getClass().getSimpleName() + " kind resources. Please fix the resource manually and re-run the conversion tool.");
                    }
                }
            }
        };
    }
}
