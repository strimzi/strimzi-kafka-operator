# Debugging Guide for Strimzi

This guide primarily explains how to perform remote debugging of code deployed to Kubernetes with strimzi/operator image, and the strimzi/kafka images.

<!-- TOC depthFrom:2 -->

- [Activating remote debugging using the agent](#activating-remote-debugging-using-the-agent)
- [Remote debugging the Strimzi Cluster Operator](#remote-debugging-the-strimzi-cluster-operator)
- [Remote debugging the Kafka Broker](#remote-debugging-the-kafka-broker)
- [Using the IntelliJ IDEA for debugging](#using-the-intellij-idea-for-debugging)
  - [Setting the breakpoints](#setting-the-breakpoints)
  - [Evaluating expressions when suspended](#evaluating-expressions-when-suspended)
  - [Resolving code version mismatches](#resolving-code-version-mismatches)

<!-- /TOC -->

## Activating remote debugging using the agent

The Java runtime (the JVM) comes with an agent that provides support for remote debugging. Using your Java IDE you can connect to a remote process over a TCP connection, and perform debugging actions like setting breakpoints, suspending threads, gathering stack and heap info, and evaluating arbitrary Java code inside the running JVM.

In order to activate remote debugging the java process needs to be run with an additional argument like the following:

    -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005

This starts a TCP server on localhost using the specified port (5005 in this case). Connection is only possible from the localhost.
Because we used `suspend=y` the start up of the JVM will stall, and wait for the debug client to connect.
All the popular Java IDEs support remote debugging.


## Remote debugging the Strimzi Cluster Operator

Strimzi Cluster Operator is installed through the Deployment definition in `install/cluster-operator/060-Deployment-strimzi-cluster-operator.yaml`.

We can enable remote debugging by adding the `JAVA_OPTS` environment variable to `strimzi-cluster-operator` container in this file:

          env:
            ...
            - name: JAVA_OPTS
              value: "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=*:5005"


Let's also increase the livenessProbe timeout (to avoid Kubernetes restarting the pod if we're slow in attaching the debugger):

          livenessProbe:
            initialDelaySeconds: 3600
            timeoutSeconds: 3600

Apply the modified file:

    kubectl apply -f install/cluster-operator/060-Deployment-strimzi-cluster-operator.yaml

We also need to open the port for localhost access:

    kubectl port-forward $(kubectl get pod | grep strimzi-cluster-operator | awk '{printf $1}') 5005

Start tailing the operator pod (to make sure it's waiting for the debugger to attach, and to see the logging output once you attach with the debugger):

    kubectl logs $(kubectl get pod | grep strimzi-cluster-operator | awk '{printf $1}') -f 

You can now start the remote debug session from your IDE.


## Remote debugging the Kafka Broker

Kafka Broker pods are defined through the 'my-cluster-kafka' StatefulSet which is created by Strimzi Kafka Operator.

In order to activate the remote debugging we need to get the Kafka Broker to run with remote debugging agent activated.

This can be achieved by adding the following to your Kafka CR cluster definition:

```

spec:
  kafka:
    ...

    livenessProbe:
      initialDelaySeconds: 3600
      timeoutSeconds: 3600
    template:
      kafkaContainer:
        env:
        - name: KAFKA_DEBUG
          value: "y"
        - name: DEBUG_SUSPEND_FLAG
          value: "y"
        - name: JAVA_DEBUG_PORT
          value: "5005"

```

We also need to open the specified port (5005 in this case) to be reachable from your host machine:

    kubectl port-forward my-cluster-kafka-0 5005

If you want to map to a different local port use the LOCAL:REMOTE format, for example:

    kubectl port-forward my-cluster-kafka-0 8787:5005

All you now have to do is deploy the new Kafka cluster definition, configure remote debugging in your favourite IDE, telling it to connect to localhost:5005, and start your debug session.

With `DEBUG_SUSPEND_FLAG` set to 'y', the Kafka Broker process will wait during startup for remote debugger (IDE) to connect before continuing with JVM start up.

In Kafka CR we have adjusted the probes to prevent Kubernetes from killing the broker whose execution is suspended due to a breakpoint.

Before remotely connecting with your IDE you'll want to start tailing the `my-cluster-kafka-0` broker pod:

    kubectl logs my-cluster-kafka-0 -f

You should see the following as the last line:

    Listening for transport dt_socket at address: 5005

You can now start a remote debug session from your IDE.

Note: Apache Kafka is written in a way that it detects stalled request processing threads and may exit the JVM that appears to have stalled.
This makes debug stepping and runtime code evaluation trickier as you sometimes need to be quick enough to gather additional data before the process dies and is restarted.


## Using the IntelliJ IDEA for debugging

In order to perform remote debugging you need to configure the remote debugging profile.

When debugging Strimzi Cluster Operator you'll want to start by opening the [Strimzi Kafka Operator project](https://github.com/strimzi/strimzi-kafka-operator).
Then you have to make sure that the Cluster Operator Image version (as defined in install/cluster-operator/060-Deployment-strimzi-cluster-operator.yaml) uses the same versions of libraries that the loaded project declares in pom.xml files.
Or vice-versa - that the Cluster Operator Image that you deploy is the one you want to match the open project.

Similarly, when debugging code deployed as part of the Kafka Broker you'll want to start with the project where such code resides (e.g. the [Strimzi Kafka OAuth project](https://github.com/strimzi/strimzi-kafka-oauth)).

Next, you have to create a run profile. In the menu select `Run` / `Debug ...` and select `Edit Configurations ...`.
In the dialog that opens click the `+` button and choose `Remote`. Change the `Name` to 'Local 5005', leave `Hostname` as 'localhost', and set port to the one used by `kubectl port-forward` - 5005 in our case.

Click `Apply`.

You can now start the remote debugging session by choosing `Run` / `Debug ...` and select `Local 5005`.


There are several useful steps to perform when doing the remote debugging.

### Setting the breakpoints

You can set the break point unconditionally (by default) which means that it will suspend execution of a thread - even all JVM threads - whenever that line of code is reached during execution.
An even more powerful way - which slows down the execution on the server - is to set a condition on the breakpoint.
If you right-click on the red dot that marks the breakpoint a dialog pops up with a `Condition` line where you can type an arbitrary oneliner that evaluates to a boolean.
You can use code-completion, and in addition to local variables, and fields, you can use static methods of any class on the running process's classpath as long as you use the full package name to qualify the class.

### Evaluating expressions when suspended

Whenever the breakpoint is reached and the current thread suspended, you can explore the current stack frame with all the local variables, and fields, and in the stack view on the left you can select stack frames up the stack which gives you access to their local variables, and fields ...
While the current thread is suspended you can also evaluate any kind of java expression which is very useful while debugging.

See [IntelliJ IDEA documentation](https://www.jetbrains.com/help/idea/examining-suspended-program.html#evaluating-expressions) for more information.

### Resolving code version mismatches

For the purpose of debugging you may need to open the `File` / `Project Structure` and under `Libraries` you may need to add the correct version of the missing library.

It depends on the actual Kafka version what specific Kafka libraries and the dependencies are on the running Kafka broker's classpath, and you need to match those in your open IDEA project.

You first need to determine the correct library version by inspecting the insides of the image or inspecting the logging output of the image.
Kafka Broker displays a complete classpath at its startup, and you can figure out the library version from that.
Alternatively you can run something like:

    kubectl exec -ti my-cluster-kafka-0 -- /bin/sh

And explore the file system, and the process environment:

    ps aux | more
    ls -la /opt/kafka/libs
    ...

The easiest way to add the library is by finding its maven artifact - you can use `search.maven.org`.

You can add the library as a maven artifact under `Libraries` by using the '+' button, and selecting 'From Maven', then typing the coordinates to the artifact, for example:

    log4j:log4j:1.2.17

Also check the `Sources` checkbox, and keep the `Transitive dependencies` checked.
When asked to choose the project module to which the library should be added it's enough to choose the top-most parent module. For strimzi-kafka-operator project that is the `strimzi` module.

Sometimes you may have to first manually download the artifact and its sources, for example:

    mvn org.apache.maven.plugins:maven-dependency-plugin:2.8:copy -DrepoUrl=https://repo.maven.apache.org -Dartifact=log4j:log4j:1.2.17:jar
    mvn org.apache.maven.plugins:maven-dependency-plugin:2.8:copy -DrepoUrl=https://repo.maven.apache.org -Dartifact=log4j:log4j:1.2.17:jar:sources
    
You may sometimes need to remove the other versions of the same artifact, or change the ordering position of the artifact under its `Project Settings` / `Modules` `Dependencies` tab.

If code lines keep being mismatched with the server versions it sometimes helps to clean the IDE's caches and restart it (`File` / `Invalidate Caches / Restart ...`).
