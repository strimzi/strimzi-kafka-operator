# Supported Apache Kafka versions

It is not feasible to maintain support for all available Apache Kafka versions.
The following rules describe our plans for Kafka versions supported in Strimzi:

* Support at least the last two major/minor versions of Apache Kafka. _For example: Support for Kafka 2.2.x could be removed when support for 2.4.x is added._
* Support at least one common Kafka release in two consecutive major/minor Strimzi releases to allow smooth upgrades between Strimzi and Kafka versions. _For example: when Strimzi 0.13.x supports Kafka 2.2.0 and 2.3.0, Strimzi 0.14.x has to provide support for 2.3.0 as well._
