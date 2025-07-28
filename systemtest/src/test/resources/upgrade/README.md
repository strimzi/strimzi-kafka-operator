# Upgrade & downgrade tests

This short README describes how the upgrade and downgrade tests are configured and what needs to be updated to make the tests work.

We have Strimzi upgrade & downgrade tests (for both YAML and OLM installation types) focused mainly on upgrade/downgrade of Strimzi, 
if the components will be rolled correctly and if there will not be some issue with the Kafka cluster (and KafkaConnect) after Strimzi CO upgrade.
It also verifies that upgrade/downgrade from/to some feature gates is smooth and without issues.
The Strimzi upgrade scenario (covered in `KRaftStrimziUpgradeST`) is configurable in `BundleUpgrade.yaml` for YAML installation type and `OlmUpgrade.yaml` for OLM installation type (covered in `KRaftOlmUpgradeST`).
The downgrade scenario (covered in `KRaftStrimziDowngradeST`) is configurable in `BundleDowngrade.yaml`.

Other than Strimzi related tests, we have Kafka upgrade and downgrade tests.
These tests are not configurable in any way, we are taking the supported versions from `kafka-versions.yaml` through which we are doing the upgrade and downgrade scenarios.
The scenarios are covered inside `KRaftKafkaUpgradeDowngradeST`.

There are three situations when there are changes needed in the configuration YAML files:

## New Strimzi version

Once there is a new Strimzi version, all three YAML files should be updated accordingly.

Let's say that we have new Strimzi version - `0.47.0`:

- `BundleDowngrade.yaml`
  - `toVersion: 0.47.0`
  - `toExamples: strimzi-0.47.0`
  - `toUrl: https://github.com/strimzi/strimzi-kafka-operator/releases/download/0.47.0/strimzi-0.47.0.zip`
  - in the `imagesAfterOperations`:
    - `kafka: strimzi/kafka:0.47.0-kafka-4.0.0` - the version of Kafka should reflect the latest supported version of Kafka from the Strimzi 0.47.0 version - also it should correspond to the version specified in `deployKafkaVersion`.
    - `topicOperator: strimzi/operator:0.47.0`
    - `userOperator: strimzi/operator:0.47.0`
  - based on graduation of feature gates you need to update `fromFeatureGates` and `toFeatureGates`
  - in case that new Strimzi version supports new Kafka version, you should update the `deployKafkaVersion` as well
- `BundleUpgrade.yaml`
  - `fromVersion: 0.47.0`
  - `fromExamples: strimzi-0.47.0`
  - `fromUrl: https://github.com/strimzi/strimzi-kafka-operator/releases/download/0.47.0/strimzi-0.47.0.zip`
  - `fromKafkaVersionsUrl: https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/0.47.0/kafka-versions.yaml`
- `OlmUpgrade.yaml`
  - `fromVersion: 0.47.0`
  - `fromOlmChannelName: strimzi-0.47.x`
  - `fromExamples: strimzi-0.47.0`
  - `fromUrl: https://github.com/strimzi/strimzi-kafka-operator/releases/download/0.47.0/strimzi-0.47.0.zip`
  - `fromKafkaVersionsUrl: https://raw.githubusercontent.com/strimzi/strimzi-kafka-operator/0.47.0/kafka-versions.yaml`

Technically, you can just replace the previous version of Strimzi to the new one - don't forget to change the OLM channel name in `fromOlmChannelName` when doing the search and replace with older version.

## New Kafka version

If you are just adding new Kafka version that is the latest one, you will need to change version of Kafka in `BundleUpgrade.yaml` - inside the `imagesAfterOperations.kafka`.
If it's just a patch release of an already supported minor Kafka version, no changes are needed.
Only once a new version of Strimzi supporting this new version of Kafka, you should change it in the `deployKafkaVersion` field in `BundleDowngrade.yaml`.

## Removal of Kafka version

If there is Kafka version being removed from the support matrix of Strimzi, you have to change the `deployKafkaVersion` field in `BundleUpgrade.yaml`.
