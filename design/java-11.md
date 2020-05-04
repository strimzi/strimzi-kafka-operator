# Move Strimzi Kafka operators to Java 11

Strimzi Kafka operators are currently developed as a Java 8 project.
For a long time, we have Java 11 in the CI pipelines, but we use java 8 as the language level as well as the runtime in the container images.
Java 11 should be also supported by Apache Kafka, so that should not be a blocker either.
This proposal suggests a roadmap for moving to Java 11 both at the runtime level as well as on the language level.

## Motivation

Java 8 is getting old.
More and more libraries now support only newer versions of Java (lately for example some of the libraries for Open Policy agent integration with Kafka).
Java 11 should also have faster TLS implementation compared to Java 8.
That should be more and more important as we remove the TLS sidecars which were dealing with part of the TLS load and use Java's native TLS support for Zookeeper communication.

We have also increased effort with maintaining the CI builds for both Java 8 and 11.
So moving to Java 11 should allow us to simplify the CI and have less builds as well.

## Proposal

This proposal suggests a phased approach, where we first use Java 11 as the runtime in our container images and only later also change the language level.
If any unexpected problems appear while using Java 11 as runtime, we can adjust the schedule or change the plan.

### Java 11 as runtime

At first, we should move to Java 11 as the runtime environment.
This should include the following tasks:
* Changing the JRE version in our images to OpenJDK 11
* Changing the CI builds so that the Java 11 build acts as the main one and is responsible fur pushing images, documentation and JARs.
* Update all build documentation (HACKING.md, etc.) with regards to the changes described above.

This phase should be done immediately after the Strimzi 0.18.0 release, to give us enough time to observe the behavior during development and tests of Strimzi 0.19.0.
Unless no unexpected issues are discovered, the 0.19.0 release will use images based on Java 11.

### Java 11 as the language level

The move to Java 11 as the language level, should include the following tasks:
* Change the Java language level in the Maven build files
* Remove the Java 8 builds
* Update all build documentation (HACKING.md, etc.) with regards to the changes described above.

After the 0.19.0 release which will use Java 11 as the runtime, we should give users another release cycle to use it and provide feedback.
If no unexpected issues are discovered, this phase should be done immediately after the Strimzi 0.20.0 release.
That would mean that the 0.20.0 release will be still using Java 11 as runtime and Java 8 as a language level.
And that 0.21.0 will use Java 11 as runtime and as language level.

## Not affected projects

This has no impact on the other subprojects such as the [OAuth library](https://github.com/strimzi/strimzi-kafka-oauth) or the [Bridge](https://github.com/strimzi/strimzi-kafka-bridge).

## Rejected alternatives

I considered doing all the changes in one step.
But if any problems arise later, the changes might be hard to revert.
The phased approach allows us to react with more flexibility as we progress and change the approach if needed.
