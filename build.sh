#!/usr/bin/env bash
set -e

export STRIMZI_VERSION="$(cat release.version)"
export IMAGE_TAG="${STRIMZI_VERSION}"
# Supported Kafka versions to build, scan and export (override via env: KAFKA_VERSIONS="4.0.0 4.1.0")
KAFKA_VERSIONS=(${KAFKA_VERSIONS:-4.0.0 4.1.0})
export TRIVY_SEVERITY="HIGH,CRITICAL"
export TRIVY_EXIT_CODE="1"
MVN_ARGS="${MVN_ARGS:--B -ntp -DskipTests}"
IMAGES_DIR="images/${STRIMZI_VERSION}"

make clean
echo "Основной проект очищен"

make -C docker-images/base clean
make -C docker-images/artifacts clean
make -C docker-images/operator clean
echo "Каталоги сборки Docker-образов очищены"

make java_install MVN_ARGS="$MVN_ARGS"
echo "Java-модули собраны и установлены"

make -C docker-images/artifacts java_install MVN_ARGS="$MVN_ARGS"
echo "Артефакты для Docker-образов собраны"

make -C docker-images/base docker_build RELEASE_VERSION="$STRIMZI_VERSION" BUILD_TAG="$STRIMZI_VERSION" BUILD_ID="$CI_PIPELINE_ID" BUILD_COMMIT="$CI_COMMIT_SHA"
echo "Базовый Docker-образ собран"

make -C docker-images/operator docker_build RELEASE_VERSION="$STRIMZI_VERSION" DOCKER_REGISTRY="$CI_REGISTRY" DOCKER_ORG="$CI_PROJECT_PATH" DOCKER_TAG="$IMAGE_TAG" BUILD_TAG="$STRIMZI_VERSION" BUILD_ID="$CI_PIPELINE_ID" BUILD_COMMIT="$CI_COMMIT_SHA"
echo "Docker-образ оператора собран и отправлен в реестр"

make -C docker-images/kafka-based docker_build DOCKER_REGISTRY="$CI_REGISTRY" DOCKER_ORG="$CI_PROJECT_PATH" DOCKER_TAG="$IMAGE_TAG" BUILD_TAG="$STRIMZI_VERSION" BUILD_ID="$STRIMZI_VERSION"
echo "Docker-образ Kafka собран и отправлен в реестр"

# Official-style tags: strimzi/kafka:<strimzi>-kafka-<kafka>
#IMAGES_TO_SAVE=()
STRIMZI_OPERATOR_IMAGE="strimzi/operator:${IMAGE_TAG}"
IMAGES_TO_SAVE+=("$STRIMZI_OPERATOR_IMAGE")

STRIMZI_KAFKA_IMAGES=()
for kafka_version in "${KAFKA_VERSIONS[@]}"; do
  kafka_image="strimzi/kafka:${STRIMZI_VERSION}-kafka-${kafka_version}"
  STRIMZI_KAFKA_IMAGES+=("$kafka_image")
  IMAGES_TO_SAVE+=("$kafka_image")
done
echo "Переменные образов Strimzi настроены"

scan_status=0
#trivy image --no-progress --severity "$TRIVY_SEVERITY" --exit-code "$TRIVY_EXIT_CODE" --format json --output trivy-operator.json "$STRIMZI_OPERATOR_IMAGE" || scan_status=$?
#trivy image --no-progress --severity "$TRIVY_SEVERITY" --format table "$STRIMZI_OPERATOR_IMAGE"

#for kafka_image in "${STRIMZI_KAFKA_IMAGES[@]}"; do
#  kafka_version="${kafka_image##*-kafka-}"
#  trivy image --no-progress --severity "$TRIVY_SEVERITY" --exit-code "$TRIVY_EXIT_CODE" \
#    --format json --output "trivy-kafka-${kafka_version}.json" "$kafka_image" || scan_status=$?
#  trivy image --no-progress --severity "$TRIVY_SEVERITY" --format table "$kafka_image"
#done

mkdir -p "$IMAGES_DIR"
for image in "${IMAGES_TO_SAVE[@]}"; do
  # strimzi/kafka:0.49.1-kafka-4.0.0 -> kafka-0.49.1-kafka-4.0.0.tar
  # strimzi/operator:0.49.1 -> operator-0.49.1.tar
  archive_name="${image#strimzi/}"
  archive_name="${archive_name//:/-}.tar"
  echo "Сохранение $image -> ${IMAGES_DIR}/${archive_name}"
  docker save -o "${IMAGES_DIR}/${archive_name}" "$image"
done
echo "Образы сохранены в ${IMAGES_DIR}"

exit "$scan_status"
