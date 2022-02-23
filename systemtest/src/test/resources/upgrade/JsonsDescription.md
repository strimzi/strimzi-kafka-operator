# Upgrade & Downgrade JSONs description

This file describes how to add new upgrade/downgrade test case into `StrimziUpgradeIsolatedST` or `StrimziDowngradeIsolatedST`.

## Upgrade JSON

For adding new upgrade/updating test case, it's needed to update the `StrimziUpgradeST.json`.

Example record:
```json
  {
    "fromVersion":"0.22.1",
    "fromExamples":"strimzi-0.22.1",
    "urlFrom":"https://github.com/strimzi/strimzi-kafka-operator/releases/download/0.22.1/strimzi-0.22.1.zip",
    "convertCRDs": true,
    "conversionTool": {
      "urlToConversionTool": "https://github.com/strimzi/strimzi-kafka-operator/releases/download/0.22.0/api-conversion-0.22.0.zip",
      "toConversionTool": "api-conversion-0.22.0"
    },
    "generateTopics": true,
    "additionalTopics": 2,
    "oldestKafka": "2.6.0",
    "imagesAfterKafkaUpgrade": {
      "zookeeper": "strimzi/kafka:latest-kafka-3.1.0",
      "kafka": "strimzi/kafka:latest-kafka-3.1.0",
      "topicOperator": "strimzi/operator:latest",
      "userOperator": "strimzi/operator:latest"
    },
    "client": {
      "continuousClientsMessages": 500
    },
    "environmentInfo": {
      "maxK8sVersion": "1.21",
      "status": "stable",
      "reason" : "Test is working on environment, where k8s server version is < 1.22"
    },
    "strimziFeatureGatesFlagsBefore": "",
    "strimziFeatureGatesFlagsAfter": "-ControlPlaneListener"
  }
```

### Fields description

`fromVersion` - String - version of Strimzi, which we want to deploy. From this version we will upgrade the CO to `HEAD` version (latest)
`fromExamples` - String - name of folder, from which we will take all examples - such as `kafka-persistent.yaml` - downloaded from `urlFrom`
`urlFrom` - String - url, from which examples are downloaded
`convertCRDs` - boolean - indicates, if CRDs and CRs will be converted to new API version using conversion tool 
`conversionTool` - JsonObject:
                 - `urlToConversionTool` - String - url, from which conversion tool is downloaded 
                 - `toConversionTool` - String - name of folder, which contains conversion tool
`generateTopics` - boolean - indicates, if topics for upgrade should be generated and should be checked after upgrade
`additionalTopics` - int - number of additional topics, which will be created
`oldestKafka` - String - version of Kafka, used in initial deployment of Kafka CR
`imagesAfterKafkaUpgrade` - JsonObject - images of `zookeeper` (String), `kafka` (String), `topicOperator` (String) and `userOperator` (String) components, checked after the upgrade
`client` - JsonObject - contains informations about clients used during upgrade
         - `continuousClientsMessages` - int - number of messages sent during upgrade
`environmentInfo` - JsonObject - information about environment, on which are upgrade tests running
                               - used for skipping some of the upgrade test cases
                               - `maxK8sVersion` - String - maximal version of K8s, on which the test case can run 
                               - `status` - String - information about stability of the test on environment (information, not propagated to test itself)
                               - `reason` - String - reason of skipping the test case (information, not propagated to test itself)
                               - `flakyEnvVariable` - String - name of ENV variable, which contains information, if the test is runnable on current environment
`strimziFeatureGatesFlagsBefore` - String - FG added to `STRIMZI_FEATURE_GATES` environment variable, on initial deploy of CO
`strimziFeatureGatesFlagsAfter` - String - FG added to `STRIMZI_FEATURE_GATES` environment variable, on upgrade of CO

## Downgrade JSON

For adding new upgrade/updating test case, it's needed to update the `StrimziDowngradeST.json`.

The JSON for downgrade is almost the same as for upgrade.
But there are few fields that are different.

Example record:
```json
  {
    "fromVersion":"HEAD",
    "toVersion":"0.28.0",
    "fromExamples":"HEAD",
    "toExamples":"strimzi-0.28.0",
    "urlFrom":"HEAD",
    "urlTo":"https://github.com/strimzi/strimzi-kafka-operator/releases/download/0.28.0/strimzi-0.28.0.zip",
    "generateTopics": true,
    "additionalTopics": 2,
    "oldestKafka": "3.0.0",
    "imagesAfterOperatorDowngrade": {
      "zookeeper": "strimzi/kafka:0.28.0-kafka-3.1.0",
      "kafka": "strimzi/kafka:0.28.0-kafka-3.1.0",
      "topicOperator": "strimzi/operator:0.28.0",
      "userOperator": "strimzi/operator:0.28.0"
    },
    "deployKafkaVersion": "3.1.0",
    "client": {
      "continuousClientsMessages": 500
    },
    "environmentInfo": {
      "maxK8sVersion": "latest",
      "status": "stable",
      "reason" : "Test is working on all environment used by QE."
    },
    "strimziFeatureGatesFlagsBefore": "+UseStrimziPodSets",
    "strimziFeatureGatesFlagsAfter": "-UseStrimziPodSets"
  }
```

### Field description

List of different fields:

`toVersion` - String - version of Strimzi, to which we will downgrade 
`toExamples` - String - name of folder, from which we will take all examples for downgrade
`urlTo` - String - url, from which examples are downloaded
`imagesAfterOperatorDowngrade` - String - same as for upgrade's `imagesAfterKafkaUpgrade` - images after operator downgrade
`deployKafkaVersion` - String - initial version of Kafka, which will be deployed before downgrade
