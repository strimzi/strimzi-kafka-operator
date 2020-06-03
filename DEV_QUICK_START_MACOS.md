Developer Quick Start for MacOS
===============================


## Preparing your Mac for work

#### Install Homebrew
https://brew.sh/

#### Install git
`brew install git`

#### Install java 11
`brew cask install adoptopenjdk/openjdk/adoptopenjdk11`

#### Install maven
`brew install maven`

#### Install GNU tools
`brew install gnu-sed findutils grep coreutils`

#### Install yq
`brew install yq`

#### Install Minishift (local install of OpenShift)
`brew cask install minishift`

At the moment of this writing you will need to fix xhyve driver for Minishift to work:
https://stackoverflow.com/questions/56358247/error-creating-new-host-json-cannot-unmarshal-bool-into-go-struct-field-driver
```
brew uninstall docker-machine-driver-xhyve
brew edit docker-machine-driver-xhyve
```

Change tag and revision:
`:tag => "v0.3.3", :revision => "7d92f74a8b9825e55ee5088b8bfa93b042badc47"`

```
brew install docker-machine-driver-xhyve
sudo chown root:wheel $(brew --prefix)/opt/docker-machine-driver-xhyve/bin/docker-machine-driver-xhyve
sudo chmod u+s $(brew --prefix)/opt/docker-machine-driver-xhyve/bin/docker-machine-driver-xhyve
```

#### Install latest Bash
`brew install bash`

#### Install Docker
`brew install docker`

#### Install kubectl
`brew install kubernetes-cli`


#### Install Helm
`brew install kubernetes-helm`

You need a running Kubernetes / Minishift in order to initialize Helm. So the following step can not yet be executed at this point (continue reading and execute it later):
`helm init`


## Preparing Minishift

#### Start Minishift
`minishift start --memory=4096 --cpus=4`

Once minishift has been started the memory and cpu settings become part of the persistent configuration, and subsequent invocations of `minishift start` will ignore `--memory` and `--cpus` arguments. In order to change them later you have to edit the VM configuration file located at: `$HOME/.minishift/machines/minishift/config.json`

Another option is to simply delete the existing instance (`minishift delete`), but in that case you discard the whole VM including the disk volumes.


#### Export 'oc' to current shell env
`eval $(minishift oc-env)`

#### Add 'cluster-admin' role to user 'developer' - the default minishift user
`oc adm policy --as system:admin add-cluster-role-to-user cluster-admin developer`

You can achieve the same by doing:
```
oc login -u system:admin
oc adm policy add-cluster-role-to-user cluster-admin developer
oc login -u developer
```

#### Initialise Helm
`helm init`


## Fetching and building Strimzi Kafka Operator

#### Clone Strimzi operator project
```
git clone https://github.com/strimzi/strimzi-kafka-operator.git
cd strimzi-kafka-operator
```

#### Start a new bash session in order to use Bash >= 4

```
bash

# Use minishift's docker daemon so there is no need to push to remote repository
eval $(minishift docker-env)

# check that 'docker ps' works
docker ps

# Use 'oc' version that comes with minishift
eval $(minishift oc-env)

# check that oc client works
oc version
```

#### Build Strimzi
`MVN_ARGS=-DskipTests make docker_build`




## Using Strimzi

Start a single-broker Kafka cluster

#### Install Strimzi custom resource definitions
`kubectl apply -f install/cluster-operator`


#### Create single-broker Kafka cluster
`kubectl apply -f examples/kafka/kafka-persistent-single.yaml`

#### Monitor the cluster starting up (this may take a minute or more)
`kubectl get pod --watch`

#### Test that the cluster works
```
kubectl exec -ti -c kafka my-cluster-kafka-0 /bin/sh

bin/kafka-topics.sh --zookeeper localhost:2181 --list

# create a topic
bin/kafka-topics.sh --zookeeper localhost:2181 --create --replication-factor 1 --partitions 1 --topic topic1

# Use cmd producer to put something in topic1
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic topic1  << EOF
{"id": 1, "message": "Hello"}
EOF

# Use cmd consumer to read content from topic1 (ctrl-c to break out)
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic1 --from-beginning

# Delete topic
bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic topic1

# Confirm that reading from topic1 doesn't work any more
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic1 --from-beginning

exit
```

#### Clean up Kafka cluster deployment and uninstall Strimzi
```
kubectl delete -f examples/kafka/kafka-persistent-single.yaml
kubectl delete -f install/cluster-operator
```
