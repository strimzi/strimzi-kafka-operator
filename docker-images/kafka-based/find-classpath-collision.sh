#! /usr/bin/env bash
image=$1 #strimzi/kafka:latest-kafka-2.2.1
image_jar_dir=$2 #/opt/kafka/libs
whilelist_file=$3
DOCKER_CMD=${DOCKER_CMD:-docker}

jars_dir=$(mktemp -d)
classes_root=$(mktemp -d)
exit_handler() {
  [ -e $jars_dir ] && rm -rf $jars_dir
  [ -e $classes_root ] && rm -rf $classes_root
}
trap exit_handler EXIT

${DOCKER_CMD} run --name temp-container-name "$image" /bin/true || exit 2
${DOCKER_CMD} cp "temp-container-name:$image_jar_dir" "$jars_dir"
${DOCKER_CMD} rm temp-container-name > /dev/null

$(dirname $0)/../artifacts/extract-jars.sh "$jars_dir" "$classes_root"

collisions=$($(dirname "$0")/../artifacts/find-colliding-classes.sh "$classes_root" | awk '{printf("%s\t%s\n",$1,$2);}' | \
    grep -vFf "$whilelist_file")

if [ "$collisions" != "" ] ; then
  echo "ERROR: Different class files with same name from different jars found!"
  echo "$collisions"
  echo "(Ignoring jars from Kafka distribution containing different class files with same name:"
  sed -e 's/^/  /' "$whilelist_file"
  echo ")"
  echo "It's likely that either two third party jars are using different versions "
  echo "of a common (transitive) dependency or a single third party jar is using a"
  echo "dependency which is also a (transitive) dependency of Kafka."
  echo "In either case the solution is a judicious <exclude> of the dependency."
  exit 1
fi
