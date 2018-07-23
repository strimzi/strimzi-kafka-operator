# Manual Authentication and Authorization workaround

* Status: **Implemented**
* Discussion: [GitHub PR](https://github.com/strimzi/strimzi-kafka-operator/pull/623)

## Motivation

The long term plan for Strimzi is to have custom resources to define Users for Authentication and Authorization rules.
The custom resources should be read by operator style application which would be responsible for the setup of the users or ACL rules.
Until such an operator (User Operator - UO) is available, it would be useful to have a way to setup Authentication and Authorization at least in a manual / semi-manual way.
This proposal covers such a workaround.
There is no long term commitment to support the mechanisms suggested in this proposal.
After the final implementation of the operator for Authorization and Authentication is implemented, it will not be possible to use this workaround any more.

## Authentication

The Cluster Operator (CO) maintains a _Clients CA_.
This Client CA is set as truststore for the TLS enabled Kafka interface which is designated for client connections (port 9093).
Currently, this _Clients CA_ does not enforce client authentication.
And enabling such configuration through `spec.kafka.config` is not possible because the required options are in the _forbidden options_ list.
This proposal suggests to modify the list of forbidden options to allow end-users to enable or disable the TLS client authentication.
This change will be only temporary and will be removed once the actual UO is in place with the needed configuration.

User who want to use manual setup of authentication will be able to do so in the following steps:

* Modify the Kafka cluster resource and add configuration options to enable TLS client authentication for the clients port (option `listener.name.clienttls.ssl.client.auth=required`)
* Use the Clients CA to manually issue user certificates.
  The CA public and private key can be downloaded from the Kubernetes / OpenShift secret and the user certificates can be signed separately.
  The CA private key should be kept secret.
* The authenticated users will be given the Principal name based on the subject of their certificate (e.g. `CN=writeuser,OU=Unknown,O=Unknown,L=Unknown,ST=Unknown,C=Unknown`)

Alternatively, the end-user can bring own CA for user authentication.
The CO will accept the CA if the secret exists before the cluster is created.

### Required changes to Cluster Operator

To implement this we will need following changes in the Cluster Operator:

* Make sure the Clients CA is used only for client authentication
* Change the forbidden options to allow end-users to enable or disable Authentication

## Authorization

To use Authorization, the Authentication needs to be enabled first.
Authorization can be enabled by setting the ACL Authorizer class in `authorizer.class.name`.
The workaround would support only the built in `kafka.security.auth.SimpleAclAuthorizer`
The default authorization plugin stores the ACL rules in Zookeeper.
Once the Authorization plugin is enabled, ACL rules can be managed with the `kafka-acls.sh` utility.

User who wants to use manual setup of authorization will be able to do so in the following steps:

* Modify the Kafka cluster resource and add configuration options to enable authorization (option `authorizer.class.name=kafka.security.auth.SimpleAclAuthorizer`)
* Since Zookeeper is not expose to the outside of the OpenShift cluster, the `kafka-acls.sh` utility has to be run from inside.
  Ideally, one of the Zookeeper pods can be used for it.
  For example:
  ```
  kubectl exec my-cluster-zookepeer-0 -c zookeeper -i -t -- bin/kafka-acls.sh --authorizer-properties zookeeper.connect=localhost:21810 --add --allow-principal User:CN=writeuser,OU=Unknown,O=Unknown,L=Unknown,ST=Unknown,C=Unknown --operation Write --topic my-topic
  ```

### Required changes to Cluster Operator

To implement this we will need following changes in the Cluster Operator:

* Change the forbidden options to allow end-users to enable or disable Authentication
* Configure the Kafka cluster nodes as _super users_ so that they can do the replication (option `super.users=User:Bob;User:Alice`)
