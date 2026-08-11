#!/usr/bin/env bash
set -e

export STRIMZI_VERSION="$(cat release.version)"
export IMAGE_TAG="${STRIMZI_VERSION}"
export KAFKA_VERSION="4.2.0"
MVN_ARGS="${MVN_ARGS:--B -ntp -DskipTests}"

make clean
echo "Основной проект очищен"

make java_install MVN_ARGS="$MVN_ARGS"
echo "Java-модули собраны и установлены"

make -C docker-images/base clean
make -C docker-images/artifacts clean
make -C docker-images/operator clean
echo "Каталоги сборки Docker-образов очищены"

make -C docker-images/artifacts java_install MVN_ARGS="$MVN_ARGS"
echo "Артефакты для Docker-образов собраны"

make -C docker-images/base docker_build RELEASE_VERSION="$STRIMZI_VERSION" BUILD_TAG="$STRIMZI_VERSION" BUILD_ID="$CI_PIPELINE_ID" BUILD_COMMIT="$CI_COMMIT_SHA"
echo "Базовый Docker-образ собран"

make -C docker-images/operator docker_build RELEASE_VERSION="$STRIMZI_VERSION" DOCKER_REGISTRY="$CI_REGISTRY" DOCKER_ORG="$CI_PROJECT_PATH" DOCKER_TAG="$IMAGE_TAG" BUILD_TAG="$STRIMZI_VERSION" BUILD_ID="$CI_PIPELINE_ID" BUILD_COMMIT="$CI_COMMIT_SHA"
echo "Docker-образ оператора собран и отправлен в реестр"

make -C docker-images/kafka-based docker_build RELEASE_VERSION="$STRIMZI_VERSION" DOCKER_REGISTRY="$CI_REGISTRY" DOCKER_ORG="$CI_PROJECT_PATH" DOCKER_TAG="$IMAGE_TAG" BUILD_ID="$CI_PIPELINE_ID" BUILD_COMMIT="$CI_COMMIT_SHA"
echo "Docker-образ Kafka собран и отправлен в реестр"

export STRIMZI_OPERATOR_IMAGE="strimzi/operator:${IMAGE_TAG}"
export STRIMZI_KAFKA_IMAGE="strimzi/kafka:${IMAGE_TAG}-kafka-${KAFKA_VERSION}"
echo "Переменные образов Strimzi настроены"

scan_status=0
trivy image --no-progress --severity "$TRIVY_SEVERITY" --exit-code "$TRIVY_EXIT_CODE" --format json --output trivy-operator.json "$STRIMZI_OPERATOR_IMAGE" || scan_status=$?
trivy image --no-progress --severity "$TRIVY_SEVERITY" --exit-code "$TRIVY_EXIT_CODE" --format json --output trivy-kafka.json "$STRIMZI_KAFKA_IMAGE" || scan_status=$?

trivy image --no-progress --severity "$TRIVY_SEVERITY" --format table "$STRIMZI_OPERATOR_IMAGE"
trivy image --no-progress --severity "$TRIVY_SEVERITY" --format table "$STRIMZI_KAFKA_IMAGE"
exit "$scan_status"