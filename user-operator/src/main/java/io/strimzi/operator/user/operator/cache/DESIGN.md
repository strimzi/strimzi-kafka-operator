# Cache

The Kafka Admin API does not make it easy to query it to find the current state of things.
It does not allow to batch the describe requests for some of the things the UO cares about, so we cannot use the micro-batching as we do for update requests.
But querying Kafka through the Admin API user by user is slow, puts a lot of load on the broker and leads to timeouts.

To address this, the User Operator is using a simple cache mechanism.
A `ConcurrentHashMap` is used as the cache with usernames as the keys.
The Kafka Admin APIs usually allow to query everything:
* All quotas for all users
* All ACLs for all users
* All users with SCRAM-SHA credentials

The cache utilizes this to get all the data within a single request.
It gets the data periodically and updates the cache by replacing the `ConcurrentHashMap`.
This package contains the abstract class `AbstractCache` which provides the shared methods and mechanisms.
And the 3 different implementations:
* ACL cache
* Quotas cache
* SCRAM-SHA credentials cache (since we cannot query the credentials, it only has a `Boolean` value to indicate if this user has existing credentials)

The `AbstractCache` is using the Java `TimerTask` mechanism to update the cache regularly in configurable intervals.
It also provides the common methods for accessing the data such as `get`, `getOrDefault`, `put`, `remove`, etc.

The different implementations provide a `loadCache` method which loads the data using the Kafka Admin API and fills the cache.
This slightly differs between the different cache implementations where for example the `AclCache` collates the ACL rules for a single user as a single item inside the cache.

While the cache is updated periodically, the different _operator_ classes handling the reconciliations of ACLs, Quotas or credentials also update the cache when reconciling the users.
This helps to reduce any unnecessary operations which would be caused by a stale cache.
For example, when a resource will be reconciled again and again in a loop because the cache says some ACLs are missing while they actually exist inside Kafka already and are only missing from the cache because it hasn't refreshed yet.

## Limitations

Since we are currently using the Kafka Admin API to get all data in a single query, we might run into problems in big clusters where the response would not fit into a single response.
This might be issue with ACLs where a single user might have possibly many ACL rules.
For Quotas or SCRAM-SHA credentials, the amount of data per user is very limited, so it might not be an issue.
Currently, the only solution to this problem is to increase the message size since the Kafka Admin API does not support any paging mechanism.

## Future possibilities

In the future, it might be possible to build a watch / informer from the cache and avoid completely the need for periodical reconciliations.
However, currently this is not possible because with SCRAM-SHA credentials, there is no indication if they changed or not - only whether they exist.
This missing feature is tracked in https://issues.apache.org/jira/browse/KAFKA-14356.
So such mechanism would be feasible only for Quotas and ACLs.
But that would not be helpful for us since we would need to keep the periodical reconciliation anyway for the SCRAM-SHA credentials.
