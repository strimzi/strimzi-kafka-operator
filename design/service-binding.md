# Service Binding Proposal for Strimzi

This document is intended to progress discussion about how to enable Strimzi to work well with the Service Binding Operator. It includes some suggested enhancements to both Strimzi and the Service Binding Operator. It would be good to settle on an agreement about how these two technologies can best work together so we can begin the technical work to deliver.

The Service Binding Operator is responsible for binding services such as databases and message brokers to runtime applications in Kubernetes. It's still under development, but the intention is that it becomes part of Operator Lifecycle Management.

## Service Binding Operator today

Today, Strimzi does not fit very nicely with service binding. With the current Service Binding Opeerator implementation, you need to include a template in the `ServiceBindingRequest` to extract the address information from the listener status.

``` yaml
apiVersion: apps.openshift.io/v1alpha1
kind: ServiceBindingRequest
metadata:
  name: barista-kafka
spec:
  applicationSelector:
    group: apps
    resource: deployments
    resourceRef: barista-kafka
    version: v1
  backingServiceSelector:
    group: kafka.strimzi.io
    kind: Kafka
    resourceRef: my-cluster
    version: v1beta1
  customEnvVar:
    - name: KAFKA_BOOTSTRAP_SERVERS
      value: |-
        {{- range .status.listeners -}}
          {{- if and (eq .type "plain") (gt (len .addresses) 0) -}}
            {{- with (index .addresses 0) -}}
              {{ .host }}:{{ .port }}
            {{- end -}}
          {{- end -}}
        {{- end -}}
```

While this is quite a clever use of a Go template, it has several problems.

1) The code is included in an annotation for the user's custom resource. This is fragile and ugly.
1) The code includes the name of a listener. If a different listener name is being used, the code is incorrect.
1) The code picks just the first of the array of addresses. It's common Kafka practice to have a list of bootstrap servers.

What would be better is to annotate the Strimzi objects in a way that enabled the Service Binding Operator to populate the environment variables itself.

## Service Binding Specification

As an enhancement to the Service Binding Operator, the [Service Binding Specification](https://github.com/application-stacks/service-binding-specification) is being developed by the community to describe how services can be made bindable, which essentially means providing binding data and a description of where to find it.

Using the service binding specification annotations with Strimzi is still a bit fiddly. Here's an example of a `Kafka` CR annotated to enable service binding.

``` yaml
apiVersion: kafka.strimzi.io/v1beta1
kind: Kafka
metadata:
  name: my-cluster
  annotations:
    servicebinding/secret/host: |-
      {{- range .status.listeners -}}
        {{- if and (eq .type "plain") (gt (len .addresses) 0) -}}
          {{- with (index .addresses 0) -}}
            {{ .host }}
          {{- end -}}
        {{- end -}}
    {{- end -}}
    servicebinding/secret/port: |-
      {{- range .status.listeners -}}
        {{- if and (eq .type "plain") (gt (len .addresses) 0) -}}
          {{- with (index .addresses 0) -}}
            {{ .port }}
          {{- end -}}
        {{- end -}}
    {{- end -}}
spec:
  kafka:
    version: 2.4.0
    replicas: 1
    listeners:
      plain: {}
    storage:
      type: ephemeral
    zookeeper:
      replicas: 1
      storage:
      type: ephemeral
status:
  listeners:
  - type: plain
    addresses:
    - host: myhost.example.com
      port: 9092
```

The Service Binding Operator looks for annotations starting `servicebinding`.

This has the same shortcomings as the first example, with the advantage that the template code is not required in every single `ServiceBindingRequest`.

It would be preferable if Strimzi custom resources were appropriately annotated without the user needing to do it.

## Desired way of consuming binding information

The aim is to make it easy to bind to any service using a `ServiceBindingRequest` CR. Here's an example:

``` yaml
apiVersion: service.binding/v1alpha1
kind: ServiceBindingRequest
metadata:
  name: my-binding
spec:
  services:
  - group: kafka.strimzi.io
    kind: Kafka
    version: v1beta1
    resourceRef: my-cluster
```

Then the binding information is supplied in in a standard way, such as environment variables. Of course, in combination with developer tooling, this can remove the need for the developer to write the code to read credentials and so on. Instead, they're presented to the application as environment variables with well known names.

## Binding data

To connect to Strimzi, a service binding needs the followng:

* **host** - hostname or IP address
* **port** - port number
* **userName** - username, optional
* **password** - password or token, optional
* **certificate** - certificate, optional

These pieces of information can be provided in a variety of ways:

* annotations on custom resources, indicating `spec` and `status` properties
* ConfigMap
* Secret

## Binding to a Kafka cluster with no TLS or authentication

Because Strimzi support multiple listeners and there is also a future plan to enhance the listener configuration capabilities, it seems prudent to design a scheme that works nicely with multiple listeners.

There are two proposals here, with a preference for the first.

### Option 1 - Augment Kafka CR status

This introduces a list of bootstrap addresses into the `status` for a `Kafka` CR like this:

``` yaml
apiVersion: kafka.strimzi.io/v1beta1
kind: Kafka
metadata:
  name: my-cluster
spec:
 ...
status:
  listeners:
  - type: plain
    addresses:
    - host: myhost1.example.com
      port: 9092
    - host: myhost2.example.com
      port: 9092
  - type: tls
    addresses:
    - host: myhost3.example.com
      port: 9093
  bootstrap:
    plain: myhost1.example.com:9092,myhost2.example.com:9092
    tls: myhost3.example.com
```

This has the advantage that the bootstrap servers information is at a known point in the `status` of the `Kafka` custom resource which makes is simple to annotate the CSV so the Service Binding Operator can find it.

Because that structure is a little odd, perhaps this would be preferred.

``` yaml
bootstrap:
- name: plain
  value: myhost1.example.com:9092,myhost2.example.com:9092
- name: tls
  value: myhost3.example.com
```

This is trying of course to find a syntax that is sufficiently general that the Service Binding Operator can be extended to support it.

``` yaml
apiVersion: operators.coreos/com:v1alpha1
kind: ClusterServiceVersion
metadata:
  name: strimzi-cluster-operator.v0.16.2
  namespace: placeholder
  annotations:
    containerImage: docker.io/strimzi/operator:0.16.2
spec:
  customresourcedefinitions:
    owned:
    - description: Represents a Kafka cluster
      displayName: Kafka
      kind: Kafka
      name: kafkas.kafka.strimzi.io
      version: v1beta1
      statusDescriptors:
      - description: The addresses of the bootstrap servers for Kafka clients
        displayName: Bootstrap servers
        path: status.bootstrap
        x-descriptors:
        - 'urn:alm:descriptor:servicebinding:secret:endpoints'
```

### Option 2 - Generate a binding ConfigMap

Rather than adding bootstrap server information to the `status`, Strimzi could create a ConfigMap and use the annotations to point the Service Binding Operator to that.

Here's the content of the ConfigMap, with type and protocol added:

``` yaml
type=kafka
protocol=kafka
endpoints.plain=myhost1.example.com:9092,myhost2.example.com:9092
endpoints.tls=myhost3.example.com
```

And the `status` changed to point to it:

``` yaml
apiVersion:kafka.strimzi.io/v1beta1
kind: Kafka
metadata:
  name: my-cluster
spec:
 ...
status:
  listeners:
  - type: plain
    addresses:
    - host: myhost1.example.com
      port: 9092
    - host: myhost2.example.com
      port: 9092
  - type: tls
    addresses:
    - host: myhost3.example.com
      port: 9093
  bindingConfigMap: binding-cm
```

And the CSV annotations:

``` yaml
apiVersion: operators.coreos/com:v1alpha1
kind: ClusterServiceVersion
metadata:
  name: strimzi-cluster-operator.v0.16.2
  namespace: placeholder
  annotations:
    containerImage: docker.io/strimzi/operator:0.16.2
spec:
  customresourcedefinitions:
    owned:
    - description: Represents a Kafka cluster
      displayName: Kafka
      kind: Kafka
      name: kafkas.kafka.strimzi.io
      version: v1beta1
      statusDescriptors:
      - description: The binding information for Kafka clients
        displayName: ConfigMap containing binding information
        path: status.bindingConfigMap
        x-descriptors:
        - 'urn:alm:descriptor:servicebinding:configMap'
```

### Consuming client's ServiceBindingRequest

All of the required annotations are applied to the `Kafka` CSV, so the binding should only need to refer to the `Kafka` CR.

``` yaml
apiVersion: service.binding/v1alpha1
kind: ServiceBindingRequest
metadata:
  name: my-binding
spec:
  services:
  - group: kafka.strimzi.io
    kind: Kafka
    version: v1beta1
    resourceRef: my-cluster
```

The consuming client needs to know the listener name.

## Binding to a Kafka cluster with TLS but no authentication

The addition with this scenario is that Kafka clients need access to the CA certificate that signed the broker's server certificate. The Service Binding Specification does not currently have support for a separate CA certificate, which seems like a simple enhancement, which it indicated using `caSecret` in the example below.

The CA certificate is most easily obtained from the `<cluster>-cluster-ca-cert` secret using the `ca.p12` and `ca.password` fields in the secret. To enable the Service Binding Operator to obtain this information, the same pattern of enhancing the CR `status` and annotating the CSV can be used.

``` yaml
apiVersion:kafka.strimzi.io/v1beta1
kind: Kafka
metadata:
  name: my-cluster
spec:
 ...
status:
  listeners:
  - type: plain
    addresses:
    - host: myhost1.example.com
      port: 9092
    - host: myhost2.example.com
      port: 9092
  - type: tls
    addresses:
    - host: myhost3.example.com
      port: 9093
  bindingConfigMap: binding-cm
  caCertificateSecret:
    secretName: my-cluster-cluster-ca-cert
---
apiVersion: operators.coreos/com:v1alpha1
kind: ClusterServiceVersion
metadata:
  name: strimzi-cluster-operator.v0.16.2
  namespace: placeholder
  annotations:
    containerImage: docker.io/strimzi/operator:0.16.2
spec:
  customresourcedefinitions:
    owned:
    - description: Represents a Kafka cluster
      displayName: Kafka
      kind: Kafka
      name: kafkas.kafka.strimzi.io
      version: v1beta1
      statusDescriptors:
      - description: The secret containing the CA certificate
        displayName: Secret containing the CA certificate
        path: status.caCertificateSecret.secretName
        x-descriptors:
        - 'urn:alm:descriptor:servicebinding:caSecret'
```

### Consuming client's ServiceBindingRequest

All of the required annotations are applied to the `Kafka` CSV, so the binding should only need to refer to the `Kafka` CR, and the CA certificate secret can be obtained from there.

``` yaml
apiVersion: service.binding/v1alpha1
kind: ServiceBindingRequest
metadata:
  name: my-binding
spec:
  services:
  - group: kafka.strimzi.io
    kind: Kafka
    version: v1beta1
    resourceRef: my-cluster
```

The consuming client needs to know the listener name and also the keys for the CA certificate secret fields for the certificate and password.

## Binding to a Kafka cluster with username/password authentication

Strimzi provides the `KafkaUser` custom resource as a way of managing users and credentials. Using the SASL SCRAM mechanism, the consuming client's credentials are made available in a combination of the CR's status and a secret.

The `KafkaUser` CR should be sufficient to provide the information. For example:

``` yaml
apiVersion: kafka.strimzi.io/v1beta1
kind: KafkaUser
metadata:
  name: my-user
  labels:
    strimzi.io/cluster: my-cluster
spec:
  authentication:
    type: scram-sha-512
  authorization:
    type: simple
    acls:
      - resource:
          type: topic
          name: my-topic
          patternType: literal
        operation: Read
status:
  username: my-user-name
  secret: my-user
```

The secret looks like this:

``` yaml
apiVersion: v1
kind: Secret
metadata:
  name: my-user
  labels:
    strimzi.io/kind: KafkaUser
    strimzi.io/cluster: my-cluster
type: Opaque
data:
  password: Z2VuZXJhdGVkcGFzc3dvcmQ=
```

The CSV can be annotated like this:

``` yaml
apiVersion: operators.coreos/com:v1alpha1
kind: ClusterServiceVersion
metadata:
  name: strimzi-cluster-operator.v0.16.2
  namespace: placeholder
  annotations:
    containerImage: docker.io/strimzi/operator:0.16.2
spec:
  customresourcedefinitions:
    owned:
    - description: Represents a user inside a Kafka cluster
      displayName: Kafka User
      kind: KafkaUser
      name: kafkausers.kafka.strimzi.io
      version: v1beta1
      resources:
      - kind: Secret
        name: ''
        version: v1
      statusDescriptors:
      - description: The username
        displayName: Username
        path: status.username
        x-descriptors:
        - 'urn:alm:descriptor:servicebinding:secret:username'
      - description: The secret containing the credentials
        displayName: Secret
        path: status.secret
        x-descriptors:
        - 'urn:alm:descriptor:servicebinding:secret'
```

### Consuming client's ServiceBindingRequest

The binding needs to refer to the `Kafka` CR for the endpoint information and the `KafkaUser` CR for the username and password.

``` yaml
apiVersion: service.binding/v1alpha1
kind: ServiceBindingRequest
metadata:
  name: my-binding
spec:
  services:
  - group: kafka.strimzi.io
    kind: Kafka
    version: v1beta1
    resourceRef: my-cluster
  - group: kafka.strimzi.io
    kind: KafkaUser
    version: v1beta1
    resourceRef: my-user
```

There are of course two secrets now, the CA certificate secret accessed via the `Kafka` CR and the client's password secret accessed via the `KafkaUser` CR.

The consuming client needs to know the listener name, the keys for the CA certificate secret fields for the certificate and password, and the key for the password field in the `KafkaUser` secret.

## Binding to a Kafka cluster with mutual TLS authentication

The `KafkaUser` CR should be sufficient to provide the information. For example:

``` yaml
apiVersion: kafka.strimzi.io/v1beta1
kind: KafkaUser
metadata:
  name: my-user
  labels:
    strimzi.io/cluster: my-cluster
spec:
  authentication:
    type: tls
  authorization:
    type: simple
    acls:
      - resource:
          type: topic
          name: my-topic
          patternType: literal
        operation: Read
status:
  username: my-user-name
  secret: my-user
```

The secret looks like this:

``` yaml
apiVersion: v1
kind: Secret
metadata:
  name: my-user
  labels:
    strimzi.io/kind: KafkaUser
    strimzi.io/cluster: my-cluster
type: Opaque
data:
  ca.crt: # Public key of the clients' CA
  user.crt: # Public key of the user
  user.key: # Private key of the user
```

The CSV can be annotated like this:

``` yaml
apiVersion: operators.coreos/com:v1alpha1
kind: ClusterServiceVersion
metadata:
  name: strimzi-cluster-operator.v0.16.2
  namespace: placeholder
  annotations:
    containerImage: docker.io/strimzi/operator:0.16.2
spec:
  customresourcedefinitions:
    owned:
    - description: Represents a user inside a Kafka cluster
      displayName: Kafka User
      kind: KafkaUser
      name: kafkausers.kafka.strimzi.io
      version: v1beta1
      resources:
      - kind: Secret
        name: ''
        version: v1
      statusDescriptors:
      - description: The secret containing the credentials
        displayName: Secret
        path: status.secret
        x-descriptors:
        - 'urn:alm:descriptor:servicebinding:secret'
```

### Consuming client's ServiceBindingRequest

The binding needs to refer to the `Kafka` CR for the endpoint information and the `KafkaUser` CR for the client certificate.

``` yaml
apiVersion: service.binding/v1alpha1
kind: ServiceBindingRequest
metadata:
  name: my-binding
spec:
  services:
  - group: kafka.strimzi.io
    kind: Kafka
    version: v1beta1
    resourceRef: my-cluster
  - group: kafka.strimzi.io
    kind: KafkaUser
    version: v1beta1
    resourceRef: my-user
```

There are of course two secrets now, the CA certificate secret accessed via the `Kafka` CR and the client's certificate secret accessed via the `KafkaUser` CR.

The consuming client needs to know the listener name, the keys for the CA certificate secret fields for the certificate and password, and the keys for the client certificate `KafkaUser` secret for the certificate and password.

# Rejected alternatives

## Strimzi enhancement - Concatenation of bootstrap server information for listeners

The main difficulty with using the Service Binding Operator with Strimzi is that there's no convenient way to get a consolidated list of bootstrap servers for the listener that you wish to use.

The way to find the bootstrap information from the `Kafka` CR is to look at the status for the listeners. Here's an example:

``` yaml
apiVersion:kafka.strimzi.io/v1beta1
kind: Kafka
metadata:
  name: my-cluster
spec:
  kafka:
    version: 2.4.0
    replicas: 2
    listeners:
      plain: {}
    storage:
      type: ephemeral
  zookeeper:
    replicas: 1
    storage:
      type: ephemeral
status:
  listeners:
  - type: plain
    addresses:
    - host: myhost1.example.com
      port: 9092
    - host: myhost2.example.com
      port: 9092
```

To bind nicely to this, ideally we'd have a consolidated list `myhost1.example.com:9092,myhost2.example.com:9092` that can directly be used as the Kafka client's bootstrap server list. Most examples dodge this issue by just using the first address in the list, but that's a terrible idea for availability.

One way to do this would be to add a `bootstrap` property to `status.listeners` like this:

``` yaml
apiVersion:kafka.strimzi.io/v1beta1
kind: Kafka
metadata:
  name: my-cluster
spec:
 ...
status:
  listeners:
  - type: plain
    addresses:
    - host: myhost1.example.com
      port: 9092
    - host: myhost2.example.com
      port: 9092
    bootstrap: myhost1.example.com:9092,myhost2.example.com:9092
```

Then the binding can use `status.listeners.plain.bootstrap`. If the listener name could be made into a parameter from the `ServiceBindingRequest`, then it would be possible to annotate the CSV for the `Kafka` CRD so that the service binding request would work without needing the user to annotate their custom resource.

``` yaml
apiVersion: operators.coreos/com:v1alpha1
kind: ClusterServiceVersion
metadata:
  name: strimzi-cluster-operator.v0.16.2
  namespace: placeholder
  annotations:
    containerImage: docker.io/strimzi/operator:0.16.2
spec:
  customresourcedefinitions:
    owned:
    - description: Represents a Kafka cluster
      displayName: Kafka
      kind: Kafka
      name: kafkas.kafka.strimzi.io
      version: v1beta1
      statusDescriptors:
      - description: The addresses of the bootstrap servers for Kafka clients
        displayName: Bootstrap servers
        path: status.listeners.{{servicebinding:listener}}.bootstrap
        x-descriptors:
        - 'urn:alm:descriptor:servicebinding:secret:endpoint'
```

Note the highly irregular `{{servicebinding:listener}}` syntax which I've used to mean that the Service Binding Operator has a variable `listener` provided as part of a `ServiceBindingRequest`, and it can then use this to build the full status path, such as `status.listeners.plain.bootstrap`.

``` yaml
apiVersion: service.binding/v1alpha1
kind: ServiceBindingRequest
metadata:
  name: my-binding
  data:
    listener: plain
spec:
  ...
```

CSV has a limitation here in its descriptor path capability, so **OLM would also need enhancing**. Let's try another way.

## Strimzi enhancement - Specify listener in KafkaUser

When authentication is being used, the user is likely to be used a `KafkaUser` custom resource. This makes quite a good fit for service binding because it is essentially Strimzi's way of expressing a binding.

``` yaml
apiVersion: kafka.strimzi.io/v1beta1
kind: KafkaUser
metadata:
  name: my-user
  labels:
    strimzi.io/cluster: my-cluster
spec:
  authentication:
    type: scram-sha-512
  authorization:
    type: simple
    acls:
      - resource:
          type: topic
          name: my-topic
          patternType: literal
        operation: Read
status:
  username: my-user-name
  secret: my-user
```

The secret looks like this:

``` yaml
apiVersion: v1
kind: Secret
metadata:
  name: my-user
  labels:
    strimzi.io/kind: KafkaUser
    strimzi.io/cluster: my-cluster
type: Opaque
data:
  password: Z2VuZXJhdGVkcGFzc3dvcmQ=
```

One potential way to make this easier, would be to name the target listener and include the bootstrap information in the `KafkaUser` CR like this:

``` yaml
apiVersion: kafka.strimzi.io/v1beta1
kind: KafkaUser
metadata:
  name: my-user
  labels:
    strimzi.io/cluster: my-cluster
spec:
  listener: external
  authentication:
    type: scram-sha-512
  authorization:
    type: simple
    acls:
      - resource:
          type: topic
          name: my-topic
          patternType: literal
        operation: Read
status:
  username: my-user-name
  secret: my-user
  listener:
    bootstrap: myhost1.example.com:9092,myhost2.example.com:9092
```

This makes the required annotations a whole lot simpler:

``` yaml
apiVersion: operators.coreos/com:v1alpha1
kind: ClusterServiceVersion
metadata:
  name: strimzi-cluster-operator.v0.16.2
  namespace: placeholder
  annotations:
    containerImage: docker.io/strimzi/operator:0.16.2
spec:
  customresourcedefinitions:
    owned:
    - description: Represents a user inside a Kafka cluster
      displayName: Kafka User
      kind: KafkaUser
      name: kafkausers.kafka.strimzi.io
      version: v1beta1
      resources:
      - kind: Secret
        name: ''
        version: v1
      statusDescriptors:
      - description: The addresses of the bootstrap servers for Kafka clients
        displayName: Bootstrap servers
        path: status.listener.bootstrap
        x-descriptors:
        - 'urn:alm:descriptor:servicebinding:secret:endpoint'
      - description: The secret containing the credentials
        displayName: Secret
        path: status.secret
        x-descriptors:
        - 'urn:alm:descriptor:servicebinding:secret'
```

Then the service binding looks like this:

``` yaml
apiVersion: service.binding/v1alpha1
kind: ServiceBindingRequest
metadata:
  name: my-binding
spec:
  services:
  - group: kafka.strimzi.io
    kind: KafkaUser
    version: v1beta1
    resourceRef: my-user
  - kind: Secret
    version: v1
    namespace: cluster-namespace
    resourceRef: ca-cert
```

Notice that there are two secrets in use, one associated with the `KafkaUser` for its credentials and one for the cluster's CA certificate.