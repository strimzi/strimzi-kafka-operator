# Remove deprecated Topic Operator deployment in Kafka CRD

| State    |
|----------|
| Proposal |

When deploying Topic Operator (Topic Operator) through the Kafka custom resource, there are two different options:

* Deploying it as part of the Entity Operator (Entity Operator) together with User Operator using `Kafka.spec.entityOperator`

* Deploying just the Topic Operator using `Kafka.spec.topicOperator`

`Kafka.spec.topicOperator` is already deprecated.
But it also seems to be broken on three different places:

* Network policies don't allow the separate Topic Operator to communicate with Zookeeper and Kafka because they do not list the Topic Operator, they only list Entity Operator in the list of allowed pods.

* Authorization configuration does not list the Topic Operator as super user, so when authorization is enabled - even when it connects to Kafka through the missing network policies - it does not have the rights to list or manage topics in Kafka

* The RBAC rights for the Topic Operator do not allow the updates of `kafkatopics/status`. So it cannot set the status in the KafkaTopic resource to set any of the errors.

These issues are present for several releases:

* Kafka Topic status was added in 0.14.0

* Super users in 0.16.0 IIRC

* Network policies since beginning I guess

And it seems that nobody complained.
That suggests that nobody is using it anymore

## Proposed changes

The feature is already deprecated and seems to not work properly for several releases without anyone noticing.
It can be also easily replaced through the Entity Operator, which offers the same functionality just through a slightly different structure of the Kafka custom resource.

Therefore I propose to remove this functionality completely from the Cluster Operator code and any related tests and system tests.
It will be replaced with a simple check whether `Kafka.spec.topicOperator` is configured and it will set a condition with a warning:

```
    - lastTransitionTime: 2020-05-25T19:17:05+0000
      message: "Kafka.spec.topicOperator is not supported anymore. Topic operator should be configured at path spec.entityOperator.topicOperator."
      status: "True"
      type: Warning
```

It should stay part of the Kafka custom resource to not cause any existing resources which still specify it to be rejected by Kubernetes.
It should be removed from the Kafka custom resource definition when moving to version `v1beta2` or to `v1`.
At this point also the warning condition will be removed.

The separate Topic Operator deployment in the Kafka CR should be also removed from the documentation.

## Rejected alternatives

I also considered fixing the issues.
But the feature is already deprecated for a long time and the problems were not noticed / reported for several releases.

## Next steps

If this proposal is approved, following next steps should be done:

* Remove the code related to `Kafka.spec.topicOperator` from the cluster operator, replace it with the warning condition, remove all related (system) tests, and remove it from documentation

* When upgrading the Kafka CR next time to `v1beta2` or to `v1`, remove the `Kafka.spec.topicOperator` and remove the warning condition.