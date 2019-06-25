/*
 * Copyright 2019, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.listener;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;

@Buildable(
        editableEnabled = false,
        generateBuilderPackage = false,
        builderPackage = "io.fabric8.kubernetes.api.builder"
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
public class KafkaListenerAuthenticationSslPlaintextVault extends KafkaListenerAuthentication {

    public static final String SASL_PLAINTEXT = "sasl_plaintext";
    private String vaultAddr;
    private String vaultToken;
    private String adminPath = "secret/kafka/private/admin";
    private String usersPath = "secret/kafka/private/users";
    private boolean cacheEnabled;
    private int cacheTTL;

    @Description("Must be `" + SASL_PLAINTEXT + "`")
    @Override
    public String getType() {
        return SASL_PLAINTEXT;
    }

    @Description("Vault Address Server")
    public String getVaultAddr() {
        return vaultAddr;
    }

    public void setVaultAddr(String vaultAddr) {
        this.vaultAddr = vaultAddr;
    }

    @Description("Vault auth token")
    public String getVaultToken() {
        return vaultToken;
    }

    public void setVaultToken(String vaultToken) {
        this.vaultToken = vaultToken;
    }

    @Description("Optional admin path, defaults to `secret/kafka/private/admin`")
    public String getAdminPath() {
        return adminPath;
    }

    public void setAdminPath(String adminPath) {
        this.adminPath = adminPath;
    }

    @Description("Optional users path, defaults to `secret/kafka/private/users`")
    public String getUsersPath() {
        return usersPath;
    }

    public void setUsersPath(String usersPath) {
        this.usersPath = usersPath;
    }

    @Description("Whether vault cache is enable or not, defaults to false")
    public boolean isCacheEnabled() {
        return cacheEnabled;
    }

    public void setCacheEnabled(boolean cacheEnabled) {
        this.cacheEnabled = cacheEnabled;
    }

    @Description("Optional TTL in minutes when cache is enabled")
    public int getCacheTTL() {
        return cacheTTL;
    }

    public void setCacheTTL(int cacheTTL) {
        this.cacheTTL = cacheTTL;
    }
}
