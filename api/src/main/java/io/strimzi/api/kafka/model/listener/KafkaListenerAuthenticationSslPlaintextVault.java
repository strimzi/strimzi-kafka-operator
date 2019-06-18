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
}
