/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.Example;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * Configures the broker authorization
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"type", "url", "allowOnError", "initialCacheCapacity", "maximumCacheSize", "expireAfterMs", "token", "superUsers"})
@EqualsAndHashCode
public class KafkaAuthorizationOpa extends KafkaAuthorization {
    private static final long serialVersionUID = 1L;

    public static final String TYPE_OPA = "opa";

    public static final String AUTHORIZER_CLASS_NAME = "com.lbg.kafka.opa.OpaAuthorizer";

    private List<String> superUsers;
    private String url;
    private boolean allowOnError = false;
    private int initialCacheCapacity = 100;
    private int maximumCacheSize = 100;
    private long expireAfterMs = 600000;
    private String token;

    @Description("Must be `" + TYPE_OPA + "`")
    @Override
    public String getType() {
        return TYPE_OPA;
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

    @Description("URL of the OPA server with the policy name which should be queried. " +
            "This option is required.")
    @Example("http://opa:8181/v1/data/kafka/authz/allow")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Description("Defines the authorizer behaviour when the OPA calls fail. " +
            "Defaults to `false`.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public boolean isAllowOnError() {
        return allowOnError;
    }

    public void setAllowOnError(boolean allowOnError) {
        this.allowOnError = allowOnError;
    }

    @Description("Initial capacity of the decision cache. " +
            "Defaults to `100`.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public int getInitialCacheCapacity() {
        return initialCacheCapacity;
    }

    public void setInitialCacheCapacity(int initialCacheCapacity) {
        this.initialCacheCapacity = initialCacheCapacity;
    }

    @Description("Maximum capacity of the decision cache. " +
            "Defaults to `100`.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public int getMaximumCacheSize() {
        return maximumCacheSize;
    }

    public void setMaximumCacheSize(int maximumCacheSize) {
        this.maximumCacheSize = maximumCacheSize;
    }

    @Description("Decision cache expiry in milliseconds. " +
            "Defaults to `600000`.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public long getExpireAfterMs() {
        return expireAfterMs;
    }

    public void setExpireAfterMs(long expireAfterMs) {
        this.expireAfterMs = expireAfterMs;
    }

    @Description("Token for authentication with OPA.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }
}
