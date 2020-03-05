# Zookeeper 3.5.x Manual Scaling Procedures

Zookeeper 3.5.x uses a new configuration procedure compare to 3.4.x servers. This [dynamic reconfiguration](https://zookeeper.apache.org/doc/r3.5.7/zookeeperReconfig.html) means that servers need to be added and removed via the Zookeeper command line client (or Admin API) in order to properly maintain a stable Zookeeper cluster. 

The procedures below outline the steps required to scale up (add) and scale down (remove) servers from an existing Strimzi Kafka deployment.

## Scale up procedure

The cluster should be scaled **one server at a time**. 

1) Create an initial Strimzi cluster with n=3 replicas (3 Kafka + 3 Zookeeper servers).

2) Set the replica count to n=4 in the zookeeper section of the Kafka CR.

3) Allow the Zookeeper server (zookeeper-3) to start up normally and establish a link to the existing quorum.

This can be checked with the following command:

```
kubectl exec -n myproject -it my-cluster-zookeeper-3 -c zookeeper -- bash -c "echo 'srvr' | nc 127.0.0.1 21813 | grep 'Mode:'"
```

This and the following commands assume that we are running in the `myproject` namespace and the kafka cluster is called `my-cluster`. Note that the index number in the new zookeeper node's name, `zookeeper-x`, matches the final number of the client port in the `nc 127.0.0.1 2181x` command. This command should output something like:

```
Mode: follower
```

4) Open a zookeeper-shell session on one of the nodes in the original cluster (in this case nodes 0, 1 or 2):

```
kubectl exec -n myproject -it my-cluster-zookeeper-0 -c zookeeper -- ./bin/zookeeper-shell.sh localhost:21810
```

5) Enter the following line to add the new server to the quorum as a voting member:

```
reconfig -add server.4=127.0.0.1:28883:38883:participant;127.0.0.1:21813
```

Note that the within the Zookeeper cluster the nodes are indexed from one not zero like in the node names. So the new `zookeeper-3` node is referred to as `server.4` within the Zookeeper configuration. This command should output the new cluster configuration:

```
server.1=127.0.0.1:28880:38880:participant;127.0.0.1:21810
server.2=127.0.0.1:28881:38881:participant;127.0.0.1:21811
server.3=127.0.0.1:28882:38882:participant;127.0.0.1:21812
server.4=127.0.0.1:28883:38883:participant;127.0.0.1:21813
version=100000054
```
 
This new configuration will then propagate to the other servers in the Zookeeper cluster and the new server should now be a full member of the quorum.
 
6) Increase the replica count by 1 (n=5) in the zookeeper section of the Kafka CR.

7) Allow the Zookeeper server (zookeeper-<n-1>) to start up normally and establish a link to the existing quorum.

This can be checked with the following command:

```
kubectl exec -n <namespace> -it my-cluster-zookeeper-<n-1> -c zookeeper -- bash -c "echo 'srvr' | nc 127.0.0.1 2181<n-1> | grep 'Mode:'"
```

Which should show something like:

```
Mode: follower
```

8) Open a zookeeper-shell session on one of the nodes in the original cluster (in this case nodes 0 >= i <= n-2):

```
kubectl exec -n <namespace> -it my-cluster-zookeeper-<i> -c zookeeper -- ./bin/zookeeper-shell.sh localhost:2181<i>
```

9) Enter the following line to add the new server to the quorum as a voting member:

```
reconfig -add server.<n>=127.0.0.1:2888<n-1>:3888<n-1>:participant;127.0.0.1:2181<n-1>
```

10) Repeat steps 6-9 for as many servers as you wish to add.

11) Once you have a cluster of the desired size you will need to signal to the cluster operator that it is safe to roll the Zookeeper cluster again. To do this set the manual-zk-scaling annotation to false (the CO will set this to true automatically once you change the number of Zookeeper replicas):

```
kubectl -n <namespace> annotate statefulset my-cluster-zookeeper strimzi.io/manual-zk-scaling=false --overwrite
```

## Scale down procedure

The notes given in [the zookeeper documentation](https://zookeeper.apache.org/doc/r3.5.7/zookeeperReconfig.html#sc_reconfig_general) should be understood when scaling down a Zookeeper cluster.

When removing nodes the highest numbered server will be deleted and so on in descending order. Therefore if you have a 5 node cluster and wish to scale down to 3 you will be removing zookeeper-4 and zookeeper-3 and keeping zookeeper-0, zookeeper-1 and zookeeper-2.

1) Log into the zookeeper-shell on one of the nodes which will be retained after scale down.

```
kubectl exec -n myproject -it my-cluster-zookeeper-0 -c zookeeper -- ./bin/zookeeper-shell.sh localhost:21810
```

This command assumes you are running in the `myproject` namespace and the Kafka cluster is called `my-cluster`. Note that the index number in the zookeeper node's name, `zookeeper-x`, matches the final number of the client port in the `zookeeper-shell.sh localhost:2181x` command.

2) Then run the following command to see the current configuration:

```
config
```

Assuming you are scaling down from a cluster that initially has 5 Zookeeper nodes, then this command should output something similar to:

```
server.1=127.0.0.1:28880:38880:participant;127.0.0.1:21810
server.2=127.0.0.1:28881:38881:participant;127.0.0.1:21811
server.3=127.0.0.1:28882:38882:participant;127.0.0.1:21812
server.4=127.0.0.1:28883:38883:participant;127.0.0.1:21813
server.5=127.0.0.1:28884:38884:participant;127.0.0.1:21814
version=100000057
```

3) We need to remove the highest numbered server first, in this case server.5, to do this issue the following command:

```
reconfig -remove 5
```

This will show the new configuration that will be propagated to all other members of the quorum:

```
server.1=127.0.0.1:28880:38880:participant;127.0.0.1:21810
server.2=127.0.0.1:28881:38881:participant;127.0.0.1:21811
server.3=127.0.0.1:28882:38882:participant;127.0.0.1:21812
server.4=127.0.0.1:28883:38883:participant;127.0.0.1:21813
version=200000012
```

4) Once this has completed, the number of replicas for the zookeeper section of the Kafka CR can be reduced by one. This will shutdown zookeeper-4 (server.5).

5) You can now repeat steps 1-4 to reduce the cluster size. Remember to do this in descending order.

6) Once you have a cluster of the desired size you will need to signal to the cluster operator that it is safe to roll the Zookeeper cluster again. To do this set the manual-zk-scaling annotation to false (the CO will set this to true automatically once you change the number of Zookeeper replicas):

```
kubectl -n <namespace> annotate statefulset my-cluster-zookeeper strimzi.io/manual-zk-scaling=false --overwrite
```

Note: It is possible to specify multiple servers to be removed at once, so we could `reconfig -remove 4,5`, to remove the two highest numbered servers at once and scale down from 5 -> 3 in one step. However, doing this can lead to instability and is not recommended.

