# Strimzi Drain Cleaner

Strimzi Drain Cleaner is a utility which helps with moving the Kafka pods deployed by [Strimzi](https://strimzi.io/) from Kubernetes nodes which are being drained.
It is useful if you want the Strimzi operator to move the pods instead of Kubernetes itself.
The advantage of this approach is that the Strimzi operator makes sure that no Kafka topics become under-replicated during the node draining.

For more information and installation guide, see [https://github.com/strimzi/drain-cleaner](https://github.com/strimzi/drain-cleaner).