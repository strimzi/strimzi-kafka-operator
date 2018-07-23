# Manual Authorization

_Note: This guide expects that Authentication was set up as describe in the [AUTHENTICATION.md](AUTHENTICATION.md) guide_

**This document describes a temporary workaround for handling Authorization manually.** 
**The long term plan for Strimzi is to have custom resources to define ACL rules for Authorization.**
**Until this is available, this temporary workaround will be available.**
**There is no long term commitment to support this workaround.**
**After the final implementation of the operator for Authorization this workaround will be disabled and it will not be possible to use it any more.**

## Enabling Authorization in the Kafka

Authorization has to be enabled in Kafka brokers.
To do so, the option `authorizer.class.name` needs to be set to `kafka.security.auth.SimpleAclAuthorizer`.
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
      authorizer.class.name: "kafka.security.auth.SimpleAclAuthorizer"
    ...
  zookeeper:
    ...
```

## Managing ACL rules

The ACL rules used by the default `SimpleAclAuthorizer` plugin are stored in Zookeeper.
Since Zookeeper is not exposed to the outside, the management of the ACL rules has to be done from inside one of the zookeeper pods.
To add or remove any ACL rules, you have to exec into the pod and call the `bin/kafka-acls.sh` utility.
Since Zookeeper encryption is implemented using a sidecar pod, you can internally connect to the unencrypted Zookeeper port.
This port differs in different pods.
It is calculated using following formula: `2181 * 10 + "the number of the pod"`.
For example:
* pod `my-cluster-zookeeper-0` will be listening on port `21810`
* pod `my-cluster-zookeeper-1` will be listening on port `21811`
* etc.

The rules can be manages as described in [Kafka documentation](http://kafka.apache.org/documentation/#security_authz).

For example, to add our User1 rights to write and read from a topic `my-topic` with consumer group `my-group` you could use following examples:

```
oc exec my-cluster-zookeeper-0 -c zookeeper -t -i -- bin/kafka-acls.sh --authorizer-properties zookeeper.connect=localhost:21810 --add --allow-principal User:CN=User1 --producer --topic my-topic
oc exec my-cluster-zookeeper-0 -c zookeeper -t -i -- bin/kafka-acls.sh --authorizer-properties zookeeper.connect=localhost:21810 --add --allow-principal User:CN=User1 --consumer --topic my-topic --group my-group
```
