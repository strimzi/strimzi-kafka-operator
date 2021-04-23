#!/usr/bin/env bash
#
# Generates OLM manifests using existing CRDs
#
#
PACKAGE_NAME=strimzi-cluster-operator
VERSION=$1
BUNDLE_NAME="${PACKAGE_NAME}-v${VERSION}"

CSV_TEMPLATE_DIR=./csv-template
CSV_TEMPLATE=${CSV_TEMPLATE_DIR}/bases/${PACKAGE_NAME}.clusterserviceversion.yaml

CRD_DIR=../install/cluster-operator
MANIFESTS=./manifests
CSV_FILE=${MANIFESTS}/${PACKAGE_NAME}.clusterserviceversion.yaml

: <<'END'
# Generates CSV and bundle from exising CRDs using the operator-sdk
# 
# There are a couple of parts which the operator-sdk does not fully support:
#   - Copying ClusterRole resources, including: 
#       a) Operator namespaced Cluster Roles to CSV file from CRDs dir
#       b) Strimzi Kafka client Cluster Roles to manifests from CRDs dir
#   - Copying alm-examples field to CSV file from CSV template
# 
# Outstanding RFEs to cover these issues:
#   - https://github.com/operator-framework/operator-sdk/issues/4503
#
generate_bundle_with_operator_sdk() {
  rm -rf ${MANIFESTS}
  
  # To generate a bundle from existing CRDs using the operator-sdk we must
  #   1.) Specify package name "--package" and bundle version "--version"
  #   2.) Set `--input-dir` to the directory containing CRDs
  #   3.) Place our template CSV file in a directory in the kustomize format: 
  #       "<some-dir>/bases/<package-name>.clusterserviceversion.yaml"
  # *Note* the package name must match the name used in the CSV template
  operator-sdk generate bundle --input-dir=$CRD_DIR --kustomize-dir=$CSV_TEMPLATE_DIR --version=$VERSION --package=$PACKAGE_NAME --output-dir=./

  # Copy Strimzi client roles
  cp ${CRD_DIR}/033-ClusterRole-strimzi-kafka-client.yaml ${MANIFESTS}/strimzi-kafka-client_rbac.authorization.k8s.io_v1_clusterrole.yaml
  # Copy annotations
  yq ea -i 'select(fi==0).metadata.annotations = select(fi==1).metadata.annotations | select(fi==0)' ${CSV_FILE} ${CSV_TEMPLATE}
  # Update namespaced cluster roles
  yq ea -i 'select(fi==0).spec.install.spec.permissions[0].rules = select(fi==1).rules | select(fi==0)' ${CSV_FILE} ${CRD_DIR}/020-ClusterRole-strimzi-cluster-operator-role.yaml

  generate_related_images
  generate_image_digests
  
  # Cleanup
  rm ${MANIFESTS}/strimzi-cluster-operator_v1_serviceaccount.yaml
  rm bundle.Dockerfile
  rm -rf metadata
}
END

# Generates CSV and bundle from exising CRDs WITHOUT using the operator-sdk
generate_bundle_without_operator_sdk() {
  rm -rf ${MANIFESTS}
  mkdir -p ${MANIFESTS}
  
  # Copy Custom Resource Definitions (CRDs)
  for file in $CRD_DIR/*; do
    name=$(yq ea '.metadata.name' ${file})
    kind=$(yq ea '.kind' ${file})
    if [ "$kind" = "CustomResourceDefinition" ] || [ "$kind" = "ConfigMap" ] ||  [ "$kind" = "ClusterRole" ]; then
      if [ "$name" != "strimzi-cluster-operator-namespaced" ] && [ "$name" != "strimzi-cluster-operator-global" ]; then
        if [ "$kind" = "CustomResourceDefinition" ]; then
          kind="crd"
        else
          kind=$(echo "$kind" | tr '[:upper:]' '[:lower:]')
        fi
        dest="${MANIFESTS}/${name}.${kind}.yaml"
        echo "Copying CRD file $file to $dest"
        cp $file $dest
      fi
    fi  
  done
  
  # Copy template
  cp ${CSV_TEMPLATE} ${CSV_FILE}
  # Update bundle name
  yq ea -i ".metadata.name = \"${BUNDLE_NAME}\"" ${CSV_FILE}
  # Update bundle version
  yq ea -i ".spec.version = \"${VERSION}\"" ${CSV_FILE}
  yq ea -i ".spec.replaces = \"$(yq ea ".spec.version" ${CSV_TEMPLATE})\" | .spec.replaces style=\"\"" ${CSV_FILE}
  # Update deployment
  yq ea -i 'select(fi==0).spec.install.spec.deployments[0].spec = select(fi==1).spec | select(fi==0)' ${CSV_FILE} ${CRD_DIR}/060-Deployment-strimzi-cluster-operator.yaml
  yq ea -i ".spec.install.spec.deployments[0].name = \"${BUNDLE_NAME}\"" ${CSV_FILE}
  # Update namespaced cluster roles
  yq ea -i 'select(fi==0).spec.install.spec.permissions[0].rules = select(fi==1).rules | select(fi==0)' ${CSV_FILE} ${CRD_DIR}/020-ClusterRole-strimzi-cluster-operator-role.yaml
  # Update global cluster roles
  yq ea -i 'select(fi==0).spec.install.spec.clusterPermissions[0].rules = select(fi==1).rules | select(fi==0)' ${CSV_FILE} ${CRD_DIR}/021-ClusterRole-strimzi-cluster-operator-role.yaml
  # Update creation timestamp
  yq ea -i ".metadata.annotations.createdAt = \"$(date +'%Y-%m-%d %H:%M:%S')\"" ${CSV_FILE}
  
  generate_related_images
  generate_image_digests
}

# The `spec.relatedImages` field is a specific to OpenShift and not
# in the operator-sdk spec, so we have to generate it ourselves.
generate_related_images() {
  yq ea -i '.spec.relatedImages = null' ${CSV_FILE}

  ENV="*-operator *BRIDGE_IMAGE *JMXTRANS_IMAGE *EXECUTOR_IMAGE"
  for env in $ENV; 
  do
    image=$(yq ea ".. | select(has(\"name\")).env[] | (select (.name == \"$env\")).value" ${CSV_FILE})
    if [ -z "$image" ]; then
      # Find operator image
      image=$(yq ea ".. | select(has(\"image\")) | (select (.name == \"$env\")).image" ${CSV_FILE} | head -n 1) 
    fi
    name="strimzi-$(echo $image | cut -d':' -f1 | cut -d'@' -f1 | rev | cut -d'/' -f1 | rev)";
    yq ea -i ".spec.relatedImages += [{\"name\": \"$name\", \"image\": \"$image\"}]" ${CSV_FILE};
  done

  # Add Kafka images to relatedImages section.
  KAFKA_IMAGE_VALUES=$(yq eval '.. | select(has("name")).env[] | (select (.name == "STRIMZI_KAFKA_IMAGES")).value' ${CSV_FILE})
  for val in $KAFKA_IMAGE_VALUES;
  do
    name="strimzi-kafka-$(echo "$val" | cut -d'=' -f1 | tr -d '.')";
    image=$(echo "${val}" | cut -d'=' -f2);
    yq ea -i ".spec.relatedImages += [{\"name\": \"$name\", \"image\": \"$image\"}]" ${CSV_FILE};
  done
}

# Iterates through images listed in newly generated relatedImages section
# and replaces image tags with image digests
generate_image_digests() {

  IMAGES=$(yq eval '.spec.relatedImages[].image' "${CSV_FILE}")
  for image_tag in $IMAGES;
  do
    tag=$(echo $image_tag | cut -d':' -f2);
    image=$(echo $image_tag | cut -d'@' -f1 | cut -d':' -f1);
    
    registry=$(echo $image | cut -d'/' -f1);
    org=$(echo $image | rev | cut -d'/' -f2 | rev);
    repo=$(echo $image | rev | cut -d'/' -f1 | rev);
    
    echo "Get digest from remote registry for: $image_tag"; 
    digest="$(curl -s -H "Accept: application/vnd.docker.distribution.manifest.v2+json" https://${registry}/v2/${org}/${repo}/manifests/${tag} | sha256sum | head -c 64)";
    
    image_digest="${image}@sha256:${digest}"
    sed -i "s|${image_tag}|${image_digest}|g" ${CSV_FILE};
  done
}

# Only one of the following generation methods should be uncommented
#generate_bundle_with_operator_sdk
generate_bundle_without_operator_sdk
