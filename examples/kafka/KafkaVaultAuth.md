# SASL_PLAIN Authentication with Vault (Hashicorp)

## Overview
The default implementation of SASL/PLAIN in Kafka specifies user names and passwords in the JAAS configuration file. 
In order to avoid storing these plain in disk you need to create your own implementation of common JCA `javax.security.auth.spi.LoginModule` and `javax.security.auth.callback.CallbackHandler` aka in Kafka as `org.apache.kafka.common.security.auth.AuthenticateCallbackHandler` that obtain
the username and password from an external source; in this case Vault. More info [here](https://docs.confluent.io/current/kafka/authentication_sasl/authentication_sasl_plain.html#sasl-plain-overview).

Once you have these implementations you will need to make your classes visible to the cluster; this is achieved adding a jar to `$KAFKA_HOME/libs` and
also instrument Kafka to now use these as part of the AuthN process.

## Instrumenting Kafka to use custom Vault AuthN

### JAAS
1. Create a JAAS configuration file on each of the Kafka brokers e.g. `kafka_server_vault_jaas.conf` and on the `KafkaServer` section add as
login module the `VaultLoginModule`
2. You also need to add two properties `admin_path` and `users_path` that describes the path in Vault where credentials are stored

```
KafkaServer {
    com.company.dataplatform.vaultjca.VaultLoginModule required
    admin_path="secret/kafka/admin"
    users_path="secret/kafka/users";
};
```

## Configuration
Enable SASL/PLAIN and configure the custom `CallbackHandler`
- Plain SASL config. See [this](https://docs.confluent.io/current/kafka/authentication_sasl/authentication_sasl_plain.html#configuration) for more options
```
security.inter.broker.protocol= SASL_PLAINTEXT
sasl.mechanism.inter.broker.protocol=PLAIN
sasl.enabled.mechanisms=PLAIN
```
- Add custom `CallbackHandler`s
```
listener.name.sasl_plaintext.plain.sasl.server.callback.handler.class=com.company.dataplatform.vaultjca.VaultAuthenticationLoginCallbackHandler
listener.name.sasl_plaintext.plain.sasl.login.callback.handler.class=com.company.dataplatform.vaultjca.VaultAuthenticationLoginCallbackHandler
```
- Add ENV_VARS needed to communicate with Vault
These depends on the current authentication method you want to use with vault [see for reference](https://bettercloud.github.io/vault-java-driver/)
```
VAULT_ADDR=https://vault-server:8200
VAULT_TOKEN=vault-secret-token
``` 
- Secrets organization in vault
Credentials for admin and users are separated on different paths and have different layouts; for
admin create a key value entry at `admin_path` as
```
username=admin
password=secretpassword
```

For users create entries under `users_path/{user}` with an entry `passoword=secret-client-password`

- Start the service passing the jaas file as `-Djava.security.auth.login.config=PATH_TO_JAAS_FILE`

## Client Configuration
- Each client needs a JAAS file with a `KafkaClient` section similar to this and two properties `username` and `password`
```
KafkaClient {
  com.company.dataplatform.vaultjca.VaultLoginModule required
  username="alice"
  password="alicepwd";
};
``` 



