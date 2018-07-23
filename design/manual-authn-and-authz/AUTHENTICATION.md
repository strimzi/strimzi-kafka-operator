# Manual Authentication

**This document describes a temporary workaround for handling Authentication manually.** 
**The long term plan for Strimzi is to have custom resources to define User for Authentication.**
**Until this is available, this temporary workaround will be available.**
**There is no long term commitment to support this workaround.**
**After the final implementation of the operator for Authentication this workaround will be disabled and it will not be possible to use it any more.**

## Requiring authentication in Kafka

To make sure the clients are always authenticated the Kafka brokers need to be configured to enforce TLS client authentication on the `clienttls` listener.
To do so, the option `listener.name.clienttls.ssl.client.auth` needs to be set to `required`.
It can be set in the `Kafka` resource:

```
apiVersion: kafka.strimzi.io/v1alpha1
kind: Kafka
metadata:
  name: my-cluster
spec:
  kafka:
    ...
    config:
      listener.name.clienttls.ssl.client.auth: required
    ...
  zookeeper:
    ...
```

## Downloading CA private and public keys

The public and private keys for the clients Certification authority (CA) can be downloaded from the `Secret` created by the Cluster Operator (CO).
The `Secret` is named `<cluster-name>-clients-ca` where the `<cluster-name>` has to be replaced by the name of your cluster (for example `my-cluster-clients-ca`).
The following commands can be used to download both keys _(adapt the secret name according to your actual configuration)_:

```
oc get secret my-cluster-clients-ca -o jsonpath='{.data.clients-ca\.crt}' | base64 -d > clients-ca.crt
oc get secret my-cluster-clients-ca -o jsonpath='{.data.clients-ca\.key}' | base64 -d > clients-ca.key
```

These keys will be used to sign the user certificates.
The private key should be kept secret as it can be used to generate other user certificates.

## Signing user certificates

For the manual authentication process, the end-user has to download the client CA certificate and use it to sign user certificates.
That can be done using many different tools.
This document shows an example of how to do it using [CFSSL](https://github.com/cloudflare/cfssl).
Using CFSSL is not mandatory and other tools can be used as well (OpenSSL, ...).

To generate a new user certificate, follow these steps:

* Create JSON file describing the details of the user certificate:
```json
{
   "CN": "User1",
   "key": {
        "algo": "rsa",
        "size": 2048
    }
}
```
* Generate new signed key usign `cfssl` utility (the example command assumes the JSON files was names `user1.json`):
```
cfssl gencert -ca clients-ca.crt -ca-key clients-ca.key user1.json | cfssljson -bare user1
```
* The command above generated 3 new files:
  * `user1.csr` with the Certificate Signing Request
  * `user1.pem` with the `user1` public key
  * `user1-key.pem` wit the `user1` private key

## Creating `Secret` using the new user certificate

This step assumes that they new User certificate is in the files named `user1.pem` and `user1-key.pem`.
Should the files be named differently, you can either rename them or change the file names in the `oc` command.
To create a `Secret` containing the new certificate, use the following command:

```
oc create secret generic user1 --from-file=./user1.pem --from-file=./user1-key.pem
```

## Using `Secret` in Kafka client

Any application which wants to connect using the TLS client authentication has to mount the new `user1` `Secret`.
It also has to mount the `<cluster-name>-cluster-ca-cert` `Secret` which contains the public key of the CA which was used to sign the broker server certificates.
This is required to verify the identity of the Kafka brokers.
The `Secrets` can be mounted into the `Pods` either as environment variables or as volumes.
For more details how to mount it visit [Kubernetes documentation](https://kubernetes.io/docs/tasks/inject-data-application/distribute-credentials-secure/).

Once you have the certificates inside the pod, you have to convert tham from the PEM format into PKCS12 or Java Keystore formats so that they can be used inside Java applications.
That can be done using the following two commands:

```
// Truststore
keytool -keystore /tmp/truststore -storepass secretpass -noprompt -alias cluster-ca -import -file ca.crt -storetype PKCS12

// Keystore
RANDFILE=/tmp/.rnd openssl pkcs12 -export -in user1.pem -inkey user1-key.pem -name user1 -password pass:secretpass -out /tmp/keystore
```  

Once the truststore and keystore files are prepared, you can start your Java application.
You will have to configure you Kafka Consumer or Producer APIs to use the certificates:

```
Map<String, String> config = new HashMap<>();
config.put("bootstrap.servers", "my-cluster-kafka-bootstrap:9093");
config.put("security.protocol", "SSL");
config.put("ssl.truststore.type", "PKCS12");
config.put("ssl.truststore.password", "secretpass");
config.put("ssl.truststore.location", "/tmp/truststore");
config.put("ssl.keystore.type", "PKCS12");
config.put("ssl.keystore.password", "secretpass");
config.put("ssl.keystore.location", "/tmp/keystore");

KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
```

## User principal

The principal for the users is created based on the subject of their certificates.
In the example used in this document, the resulting Principal will be `CN=User1`.
In case the subject contains more items they will be all added to the user principal in the following format: `CN=writeuser,OU=Unknown,O=Unknown,L=Unknown,ST=Unknown,C=Unknown`.
