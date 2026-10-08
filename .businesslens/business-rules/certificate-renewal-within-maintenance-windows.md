---
appliesTo:
  - type: capability-scenario
    id: renew-ca-certificate-in-renewal-period
  - type: capability-scenario
    id: renew-ca-certificate-on-request
  - type: capability-scenario
    id: replace-ca-key-on-request
  - type: capability-scenario
    id: renew-user-certificate-before-expiry
references:
  - kind: code
    role: implementation
    target: cluster-operator/src/main/java/io/strimzi/operator/cluster/operator/assembly/InternalCaProvider.java#InternalCaProvider
  - kind: code
    role: implementation
    target: user-operator/src/main/java/io/strimzi/operator/user/model/KafkaUserModel.java#maybeGenerateCertificates
---

# Expiring and requested certificate renewals happen only inside maintenance time windows

When a Kafka cluster defines maintenance time windows, CA certificates and keys that are renewed or replaced — because they enter their renewal period or on request — and user certificates that enter their renewal period are renewed, and pods rolled for them, only during one of those windows. A user certificate reissued because the clients CA changed does not wait.
