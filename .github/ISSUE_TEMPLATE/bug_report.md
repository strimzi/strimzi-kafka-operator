---
name: Bug report
about: Create a report to help us improve
title: ''
labels: bug
assignees: ''

---

_Please use this to only for bug reports. For questions or when you need help, you can use the [GitHub Discussions](https://github.com/strimzi/strimzi-kafka-operator/discussions), our [#strimzi Slack channel](https://slack.cncf.io/) or out [user mailing list](https://lists.cncf.io/g/cncf-strimzi-users/topics)._

**Describe the bug**
A clear and concise description of what the bug is.

**To Reproduce**
Steps to reproduce the behavior:
1. Go to '...'
2. Create Custom Resource '....'
3. Run command '....'
4. See error

**Expected behavior**
A clear and concise description of what you expected to happen.

**Environment (please complete the following information):**
 - Strimzi version: [e.g. master, 0.11.1]
 - Installation method: [e.g. YAML files, Helm chart, OperatorHub.io]
 - Kubernetes cluster: [e.g. Kubernetes 1.14, OpenShift 3.9]
 - Infrastructure: [e.g. Amazon EKS, Minikube]

**YAML files and logs**

Attach or copy paste the custom resources you used to deploy the Kafka cluster and the relevant YAMLs created by the Cluster Operator.
Attach or copy and paste also the relevant logs.

*To easily collect all YAMLs and logs, you can use our [report script](https://github.com/strimzi/strimzi-kafka-operator/blob/master/tools/report.sh) which will automatically collect all files and prepare a ZIP archive which can be easily attached to this issue.
The usage of this script is:
`./report.sh [--namespace <string>] [--cluster <string>]`*

**Additional context**
Add any other context about the problem here.
