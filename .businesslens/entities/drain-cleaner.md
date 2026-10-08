---
kind: system
acts: external
references:
  - kind: doc
    role: context
    target: documentation/assemblies/deploying/assembly-drain-cleaner.adoc
---

# Drain Cleaner

The separately deployed Strimzi Drain Cleaner, which intercepts Kubernetes evictions of Strimzi-managed pods during node drains and asks Strimzi to roll those pods instead.
