/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.v1.converter.conversions;

import io.strimzi.api.kafka.model.user.KafkaUser;
import io.strimzi.api.kafka.model.user.KafkaUserAuthorization;
import io.strimzi.api.kafka.model.user.KafkaUserAuthorizationSimple;
import io.strimzi.api.kafka.model.user.acl.AclRule;

import java.util.List;

/**
 * Class for holding the various conversions specific for the KafkaUser API conversion
 */
@SuppressWarnings("deprecation")
public class UserConversions {
    /**
     * Converts the operation field to operations in KafkaUser resources
     *
     * @return  The conversion
     */
    public static Conversion<KafkaUser> operationToOperations() {
        return Conversion.replace("/spec/authorization", new Conversion.DefaultConversionFunction<KafkaUserAuthorization>() {
            @Override
            Class<KafkaUserAuthorization> convertedType() {
                return KafkaUserAuthorization.class;
            }

            @Override
            public KafkaUserAuthorization apply(KafkaUserAuthorization authorization) {
                if (authorization == null) {
                    return null;
                } else if (authorization instanceof KafkaUserAuthorizationSimple simpleAuthorization && simpleAuthorization.getAcls() != null) {
                    for (AclRule aclRule : simpleAuthorization.getAcls()) {
                        // We convert this only if `operations` is not set. Otherwise, we just remove it.
                        if (aclRule.getOperation() != null && (aclRule.getOperations() == null || aclRule.getOperations().isEmpty())) {
                            aclRule.setOperations(List.of(aclRule.getOperation()));
                        }

                        aclRule.setOperation(null);
                    }
                }

                return authorization;
            }
        });
    }
}
