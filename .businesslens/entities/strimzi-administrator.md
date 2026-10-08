---
kind: person
acts: external
references:
  - kind: doc
    role: context
    target: documentation/modules/deploying/proc-deploy-designating-strimzi-administrators.adoc
  - kind: code
    role: context
    target: packaging/install/strimzi-admin/010-ClusterRole-strimzi-admin.yaml
---

# Strimzi administrator

A person whose Kubernetes permissions let them create, change and delete Strimzi custom resources and annotate the pods and Secrets Strimzi creates — a Kubernetes cluster administrator, or someone granted the `strimzi-admin` role.
