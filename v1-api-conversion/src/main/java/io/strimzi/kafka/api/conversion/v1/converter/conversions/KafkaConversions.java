/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter.conversions;

import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorization;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorizationKeycloak;
import io.strimzi.api.kafka.model.kafka.KafkaAuthorizationOpa;
import io.strimzi.api.kafka.model.kafka.KafkaClusterSpec;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityTopicOperatorSpec;
import io.strimzi.api.kafka.model.kafka.entityoperator.EntityUserOperatorSpec;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationCustom;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerAuthenticationOAuth;
import io.strimzi.api.kafka.model.kafka.listener.ListenerStatus;
import io.strimzi.kafka.api.conversion.v1.converter.ApiConversionFailedException;

/**
 * Class for holding the various conversions specific for the Kafka API conversion
 */
@SuppressWarnings("deprecation")
public class KafkaConversions {
    /**
     * Checks for the presence of Keycloak authorization and raises an exception if it is used.
     *
     * @return  The conversion
     */
    public static Conversion<Kafka> enforceKeycloakAuthorization() {
        return Conversion.replace("/spec/kafka/authorization", new Conversion.DefaultConversionFunction<KafkaAuthorization>() {
            @Override
            Class<KafkaAuthorization> convertedType() {
                return KafkaAuthorization.class;
            }

            @Override
            public KafkaAuthorization apply(KafkaAuthorization authorization) {
                if (authorization == null) {
                    return null;
                }

                if (authorization instanceof KafkaAuthorizationKeycloak) {
                    throw new ApiConversionFailedException("The Keycloak authorization is removed in the v1 API version. Use the Custom authorization instead. Please fix the resource manually and re-run the conversion tool.");
                }

                return authorization;
            }
        });
    }

    /**
     * Checks for the presence of OPA authorization and raises an exception if it is used.
     *
     * @return  The conversion
     */
    public static Conversion<Kafka> enforceOpaAuthorization() {
        return Conversion.replace("/spec/kafka/authorization", new Conversion.DefaultConversionFunction<KafkaAuthorization>() {
            @Override
            Class<KafkaAuthorization> convertedType() {
                return KafkaAuthorization.class;
            }

            @Override
            public KafkaAuthorization apply(KafkaAuthorization authorization) {
                if (authorization == null) {
                    return null;
                }

                if (authorization instanceof KafkaAuthorizationOpa) {
                    throw new ApiConversionFailedException("The Open Policy Agent authorization is removed in the v1 API version. Use the Custom authorization instead. Please fix the resource manually and re-run the conversion tool.");
                }

                return authorization;
            }
        });
    }

    /**
     * Checks for the presence of OAuth authentication in the Kafka resource. If OAuth authentication is found, an error
     * is raised and the user has to convert it manually.
     *
     * @return  The conversion
     */
    public static Conversion<Kafka> enforceOauthServerAuthentication() {
        return Conversion.replace("/spec/kafka", new Conversion.DefaultConversionFunction<KafkaClusterSpec>() {
            @Override
            Class<KafkaClusterSpec> convertedType() {
                return KafkaClusterSpec.class;
            }

            @Override
            public KafkaClusterSpec apply(KafkaClusterSpec spec) {
                if (spec == null) {
                    return null;
                }

                if (spec.getListeners() != null)    {
                    for (GenericKafkaListener listener : spec.getListeners()) {
                        if (listener.getAuth() != null && listener.getAuth() instanceof KafkaListenerAuthenticationOAuth)   {
                            throw new ApiConversionFailedException("The OAuth authentication is removed in the v1 API version. Use the Custom authentication instead. Please fix the resource manually and re-run the conversion tool.");
                        }
                    }
                }

                return spec;
            }
        });
    }

    /**
     * Checks for the presence of custom Secrets in custom listener authentication. If any custom Secrets are found,
     * an error is raised and the user has to convert it manually.
     *
     * @return  The conversion
     */
    public static Conversion<Kafka> enforceSecretsInCustomServerAuthentication() {
        return Conversion.replace("/spec/kafka", new Conversion.DefaultConversionFunction<KafkaClusterSpec>() {
            @Override
            Class<KafkaClusterSpec> convertedType() {
                return KafkaClusterSpec.class;
            }

            @Override
            public KafkaClusterSpec apply(KafkaClusterSpec spec) {
                if (spec == null) {
                    return null;
                }

                if (spec.getListeners() != null)    {
                    for (GenericKafkaListener listener : spec.getListeners()) {
                        if (listener.getAuth() != null && listener.getAuth() instanceof KafkaListenerAuthenticationCustom customAuth)   {
                            if (customAuth.getSecrets() != null) {
                                throw new ApiConversionFailedException("Adding secrets in custom authentication is removed in the v1 API version. Use the additional volumes feature in the template section instead. Please fix the resource manually and re-run the conversion tool.");
                            }
                        }
                    }
                }

                return spec;
            }
        });
    }

    /**
     * Checks for the presence of resources in .spec.kafka in the Kafka CR. If found, an error is raised and the user
     * has to convert it manually.
     *
     * @return  The conversion
     */
    public static Conversion<Kafka> enforceKafkaResources() {
        return Conversion.replace("/spec/kafka", new Conversion.DefaultConversionFunction<KafkaClusterSpec>() {
            @Override
            Class<KafkaClusterSpec> convertedType() {
                return KafkaClusterSpec.class;
            }

            @Override
            public KafkaClusterSpec apply(KafkaClusterSpec spec) {
                if (spec == null) {
                    return null;
                }

                if (spec.getResources() != null)    {
                    throw new ApiConversionFailedException("The resources configuration in .spec.kafka.resources is removed in the v1 API version. Use the KafkaNodePool resource instead to configure resources. Please fix the resource manually and re-run the conversion tool.");
                }

                return spec;
            }
        });
    }

    /**
     * Converts the reconciliation interval for Topic Operator
     *
     * @return  The conversion
     */
    public static Conversion<Kafka> convertReconciliationIntervalInTO() {
        return Conversion.replace("/spec/entityOperator/topicOperator", new Conversion.DefaultConversionFunction<EntityTopicOperatorSpec>() {
            @Override
            Class<EntityTopicOperatorSpec> convertedType() {
                return EntityTopicOperatorSpec.class;
            }

            @Override
            public EntityTopicOperatorSpec apply(EntityTopicOperatorSpec spec) {
                if (spec == null) {
                    return null;
                }

                if (spec.getReconciliationIntervalSeconds() != null) {
                    if (spec.getReconciliationIntervalMs() == null) {
                        spec.setReconciliationIntervalMs(spec.getReconciliationIntervalSeconds().longValue() * 1_000);
                    }

                    spec.setReconciliationIntervalSeconds(null);
                }

                return spec;
            }
        });
    }

    /**
     * Converts the reconciliation interval for User Operator
     *
     * @return  The conversion
     */
    public static Conversion<Kafka> convertReconciliationIntervalInUO() {
        return Conversion.replace("/spec/entityOperator/userOperator", new Conversion.DefaultConversionFunction<EntityUserOperatorSpec>() {
            @Override
            Class<EntityUserOperatorSpec> convertedType() {
                return EntityUserOperatorSpec.class;
            }

            @Override
            public EntityUserOperatorSpec apply(EntityUserOperatorSpec spec) {
                if (spec == null) {
                    return null;
                }

                if (spec.getReconciliationIntervalSeconds() != null) {
                    if (spec.getReconciliationIntervalMs() == null) {
                        spec.setReconciliationIntervalMs(spec.getReconciliationIntervalSeconds() * 1_000);
                    }

                    spec.setReconciliationIntervalSeconds(null);
                }

                return spec;
            }
        });
    }

    /**
     * Removes the listener type in the status section
     *
     * @return  The conversion
     */
    public static Conversion<Kafka> removeListenerTypeInStatus()   {
        return Conversion.replace("/status", new Conversion.DefaultConversionFunction<KafkaStatus>() {
            @Override
            Class<KafkaStatus> convertedType() {
                return KafkaStatus.class;
            }

            @Override
            public KafkaStatus apply(KafkaStatus status) {
                if (status == null) {
                    return null;
                }

                if (status.getListeners() != null)    {
                    for (ListenerStatus listener : status.getListeners()) {
                        if (listener.getType() != null) {
                            listener.setType(null);
                        }
                    }
                }

                return status;
            }
        });
    }
}
