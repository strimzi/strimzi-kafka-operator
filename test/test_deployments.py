import pytest
import logging
import re
import uuid
import sys
import random
import string
from cluster import *
from kafka import Kafka

logging.basicConfig(level=logging.DEBUG)

def random_topic_name():
    return "topic"+''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))

def random_group_name(prefix=""):
    return "group"+''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(8))

def openshift_template_inmemory_deployment(session):
    if not session.is_openshift:
        pytest.skip("only supported on openshift, because k8s doesn't have templates")
    session.create("../kafka-inmemory/resources/openshift-template.yaml")
    session.new_app("barnabas")
    ## TODO: don't hardcode the expected number of endpoints
    session.await_endpoint("kafka", 3)
    def cleanup():
        session.delete("-f", "../kafka-inmemory/resources/openshift-template.yaml")
    session.addfinalizer(cleanup)
    return session

def openshift_template_statefulsets_deployment(session):
    if not session.is_openshift:
        pytest.skip("only supported on openshift, because k8s doesn't have templates")
    session.create("../kafka-statefulsets/resources/openshift-template.yaml")
    session.new_app("barnabas")
    # TODO: don't hardcode the expected number of endpoints
    session.await_endpoint("kafka", 3)
    def cleanup():
        session.delete("-f", "../kafka-statefulsets/resources/openshift-template.yaml")
    session.addfinalizer(cleanup)
    return session

def inmemory_deployment(session):
    if session.is_openshift:
        pytest.skip("not supported on openshift, due to needing root user")
    session.apply("../kafka-inmemory/resources/zookeeper.yaml",
                  "../kafka-inmemory/resources/zookeeper-headless-service.yaml",
                  "../kafka-inmemory/resources/zookeeper-service.yaml",
                  "../kafka-inmemory/resources/kafka.yaml",
                  "../kafka-inmemory/resources/kafka-headless-service.yaml",
                  "../kafka-inmemory/resources/kafka-service.yaml")
    # TODO: don't hardcode the expected number of endpoints
    session.await_endpoint("kafka", 3)
    return session

def statefulsets_deployment(session):
    if session.is_openshift:
        pytest.skip("not supported on openshift, due to needing root user")
    session.apply("../kafka-statefulsets/resources/cluster-volumes.yaml",
        "../kafka-statefulsets/resources/zookeeper.yaml",
        "../kafka-statefulsets/resources/zookeeper-headless-service.yaml",
        "../kafka-statefulsets/resources/zookeeper-service.yaml",
        "../kafka-statefulsets/resources/kafka.yaml",
        "../kafka-statefulsets/resources/kafka-headless-service.yaml",
        "../kafka-statefulsets/resources/kafka-service.yaml")
    # TODO: don't hardcode the expected number of endpoints
    session.await_endpoint("kafka", 3)
    return session

deployments = ["openshift_template_inmemory_deployment", "openshift_template_statefulsets_deployment", "inmemory_deployment", "statefulsets_deployment"]

def pytest_generate_tests(metafunc):
     if 'deployment' in metafunc.fixturenames:
         metafunc.parametrize("deployment", deployments, indirect=True)

@pytest.fixture
def deployment(request, session):
    sess = session
    LOGGER.info("Deploying %s on %s", request.param, session)
    if request.param == 'openshift_template_inmemory_deployment':
        return openshift_template_inmemory_deployment(sess)
    elif request.param == 'openshift_template_statefulsets_deployment':
        return openshift_template_statefulsets_deployment(sess)
    elif request.param == 'inmemory_deployment':
        return inmemory_deployment(sess)
    elif request.param == 'statefulsets_deployment':
        return statefulsets_deployment(sess)
    else:
        raise ValueError("invalid config")

#@pytest.mark.parametrize("deployment", deployments)
def test_produce_consume(request, deployment):
    logging.info("test_produce_consume with %s" % (deployment))
    sess = deployment
    ## Find zk host:port
    regex = re.compile("(?P<name>\S+)\s+(?P<cluster_ip>\S+)\s+(?P<external_ip>\S+)\s+(?P<port>\S+)/(?P<proto>\S+)\s+(?P<age>\S+)")
    match = regex.match(sess.get("svc", "zookeeper")[0])
    zookeeper_ip = match.group("cluster_ip")
    zookeeper_port = match.group("port")
    zookeeper = zookeeper_ip+":"+zookeeper_port
    # TODO find all kafka pods
    # TODO test from a non-broker pod
    # TODO test from outside the cluster if there's an external IP
    kafka = Kafka(sess, "kafka-0")
    # Create a topic for the test
    topic = random_topic_name()
    group = random_group_name()
    num_messages=10
    kafka.create_topic(zookeeper, topic)
    # Produce to this topic
    send_results = kafka.verifiable_producer(topic, "localhost:9092", max_messages=num_messages)
    assert(send_results.sent == send_results.acked)
    # Consume from this topic
    receive_results = kafka.verifiable_consumer(topic, group, "localhost:9092", max_messages=num_messages,
                                          cmd_timeout_secs=10, verbose=True)
    assert(send_results.sent == receive_results.count)
    # clean up
    kafka.delete_topic(zookeeper, topic, if_exists=True)


if __name__ == '__main__':
    pytest.main(args=sys.argv)
