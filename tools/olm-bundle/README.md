# OLM bundle generator
Bash script for generating an OLM bundle for OperatorHub from existing CRD, ClusterRole, Configmap, and Deployment files.
The generator copies and renames these existing files from the `../../packaging/install/cluster-operator` directory.

Then the generator creates a CSV file generating the following CSV fields automatically:
- `metadata.name`
- `metadata.annotations.containerImage`
- `metadata.annotations.createdAt`
- `spec.name`
- `spec.version`
- `spec.replaces`
- `spec.install.spec.permissions`
- `spec.install.spec.clusterPermissions`
- `spec.install.spec.deployments`
- `spec.relatedImages`

All other CSV fields must be updated manually in the template CSV file, `./csv-template/bases/strimzi-cluster-operator.clusterserviceversion.yaml`

## Requirements
- bash 5+ (GNU)
- yq 4.6+ (YAML processor)
- operator-sdk 1.6.2+

## Usage
```
./generate-olm-bundle.sh <BUNDLE_VERSION>

```
for example:
```
/generate-olm-bundle.sh 2.3.0
```
