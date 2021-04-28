#!/usr/bin/env bash
#
# Generates OLM manifests using existing CRDs
#
#
PACKAGE_NAME=strimzi-cluster-operator
VERSION=$1
PREVIOUS_BUNDLE_VERSION=$(curl -s https://raw.githubusercontent.com/operator-framework/community-operators/master/community-operators/strimzi-kafka-operator/strimzi-kafka-operator.package.yaml | yq e '.channels[0].currentCSV' -)
BUNDLE_NAME="${PACKAGE_NAME}-v${VERSION}"

CSV_TEMPLATE=templates/bundle.clusterserviceversion.yaml

CRD_DIR=../../packaging/install/cluster-operator
MANIFESTS=./manifests
CSV_FILE=${MANIFESTS}/bundle.clusterserviceversion.yaml

# Generates manifests files using existing CRDs
generate_manifests() {
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
  yq ea -i ".spec.replaces = \"${PREVIOUS_BUNDLE_VERSION}\" | .spec.replaces style=\"\"" ${CSV_FILE}
  # Update deployment
  yq ea -i 'select(fi==0).spec.install.spec.deployments[0].spec = select(fi==1).spec | select(fi==0)' ${CSV_FILE} ${CRD_DIR}/060-Deployment-strimzi-cluster-operator.yaml
  yq ea -i ".spec.install.spec.deployments[0].name = \"${BUNDLE_NAME}\"" ${CSV_FILE}
  
  # Update namespaced RBAC rules with Cluster Operator roles
  yq ea -i 'select(fi==0).spec.install.spec.permissions[0].rules = select(fi==1).rules | select(fi==0)' ${CSV_FILE} ${CRD_DIR}/020-ClusterRole-strimzi-cluster-operator-role.yaml
  # Update namespaced RBAC rules with Entity Operator roles
  yq ea -i 'select(fi==0).spec.install.spec.permissions[0].rules + select(fi==1).rules | select(fi==0)' ${CSV_FILE} ${CRD_DIR}/031-ClusterRole-strimzi-entity-operator.yaml
   
  # Update global RBAC rules with Cluster Operator roles
  yq ea -i 'select(fi==0).spec.install.spec.clusterPermissions[0].rules = select(fi==1).rules | select(fi==0)' ${CSV_FILE} ${CRD_DIR}/021-ClusterRole-strimzi-cluster-operator-role.yaml
  # Update creation timestamp
  yq ea -i ".metadata.annotations.createdAt = \"$(date +'%Y-%m-%d %H:%M:%S')\"" ${CSV_FILE}
  
  generate_related_images
  generate_image_digests
}

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
    image=$(echo $image_tag | cut -d':' -f1);
    
    registry=$(echo $image | cut -d'/' -f1);
    org=$(echo $image | rev | cut -d'/' -f2 | rev);
    repo=$(echo $image | rev | cut -d'/' -f1 | rev);
    
    echo "Get digest from remote registry for: $image_tag"; 
    digest="$(curl -s -H "Accept: application/vnd.docker.distribution.manifest.v2+json" https://${registry}/v2/${org}/${repo}/manifests/${tag} | sha256sum | head -c 64)";
    
    image_digest="${image}@sha256:${digest}"
    sed -i "s|${image_tag}|${image_digest}|g" ${CSV_FILE};
  done
}

generate_manifests
