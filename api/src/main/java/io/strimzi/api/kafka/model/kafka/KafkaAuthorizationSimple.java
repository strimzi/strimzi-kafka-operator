/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.strimzi.crdgenerator.annotations.Example;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;

/**
 * Configures the broker authorization
 */
@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "superUsers"})
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class KafkaAuthorizationSimple extends KafkaAuthorization {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_SIMPLE = "simple";

    public static final String AUTHORIZER_CLASS_NAME = "kafka.security.authorizer.AclAuthorizer";
    public static final String KRAFT_AUTHORIZER_CLASS_NAME = "org.apache.kafka.metadata.authorizer.StandardAuthorizer";

    private List<String> superUsers;

    @Description("Must be `" + TYPE_SIMPLE + "`")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Override
    public String getType() {
        return TYPE_SIMPLE;
    }

    /**
     * Simple authorizer is the native Kafka authorizer. This method returns always true for it.
     *
     * @return Returns always true for Simple authorizer
     */
    public boolean supportsAdminApi()   {
        return true;
    }

    @Description("List of super users. Should contain list of user principals which should get unlimited access rights.")
    @Example("- CN=my-user\n" +
             "- CN=my-other-user")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public List<String> getSuperUsers() {
        return superUsers;
    }

    public void setSuperUsers(List<String> superUsers) {
        this.superUsers = superUsers;
    }
}
