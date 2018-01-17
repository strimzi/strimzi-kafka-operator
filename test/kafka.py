
import json
import logging
from exec_ import *

LOGGER = logging.getLogger("kafka")

class Kafka:
    """
    Run Kafka commands on a container within a pod
    """
    def __init__(self, session, pod, container=None):
        """The given container in the given pod, or the first container in the given pod if no container given."""
        self.session = session
        self.pod = pod
        self.container = container

    def _copy_configs(self, configs):
        """
        Dump the given configs to a local file, copy them to a temporary file on the container,
        return the remote temporary file name.
        :param configs: The configs, can be string, dict or (local) file descriptor
        :return: The remote temporary file name
        """
        raise Exception("Not implemented yet")
        ## TODO copy the string to a local file
        ## then copy that file to the pod
        #session.cp(src, pod, container, dest)

    def verifiable_producer(self, topic, broker_list,
                            max_messages=-1, throughput=-1, acks=-1, producer_config_string=None,
                            value_prefix=None,
                            cmd_timeout_secs=30.0):
        """
        Execute ``kafka-verifiable-producer.sh`` via `oc/kubectl exec` in the given container of the given pod,
        returning the JSON it emits, or throwing an Exception

        :param topic: The topic to produce records in
        :param broker_list: The bootstrap brokers
        :param max_messages: See ``--max-messages``
        :param throughput: See ``--throughput``
        :param acks: See ``--acks``
        :param producer_config_string: The producer config as a string. This will be copied into the pod and passed to ``kafka-verifiable-producer.sh``
        :param value_prefix: See ``--value-prefix``
        :param cmd_timeout_secs:  A timeout, in seconds. An :py:class:`ExecTimeoutException` will be raised if ``oc/kubectl exec`` takes longer.
        :return: A :ref:`ToolData` representing the tool_data event in the JSON emitted by ``kafka-verifiable-producer.sh``.
        """
        LOGGER.info("Producing %s messages to topic %s", max_messages, topic)
        cmd = ["/opt/kafka/bin/kafka-verifiable-producer.sh",
               "--topic", topic,
               "--broker-list", broker_list]
        if max_messages > 0:
            cmd += ["--max-messages", str(max_messages)]
        if throughput > 0:
            cmd += ["--thoughput", str(throughput)]
        if acks > 0:
            cmd += ["--acks", str(acks)]
        if producer_config_string:
            raise Exception("Not implemented yet")
            ## TODO copy the string to a local file
            ## then copy that file to the pod
            #session.cp(src, pod, container, dest)
            remote_configs = self._copy_configs(producer_config_string)
            cmd  += ["--producer-condfig", remote_configs]
        if value_prefix:
            cmd  += ["--value-prefix", value_prefix]
        LOGGER.debug("pod: %s, container: %s, cmd %s", self.pod, self.container, " ".join(cmd))
        lines_of_json = self.session.exec_(self.pod, cmd, self.container, cmd_timeout_secs=cmd_timeout_secs)
        events = [json.loads(json_string) for json_string in lines_of_json.splitlines()]
        if events[-1]['name'] == 'tool_data':
            return Kafka.ToolData(events[-1])
        else:
            raise Exception("Couldn't find tool_data event in JSON output of kafka-verifiable-producer.sh")

    class ToolData(object):
        """
        Representation of the "tool_data" event in the JSON output of kafka-verifiable-producer.sh, e.g.

            {"avg_throughput":39.0625,"target_throughput":-1,"name":"tool_data","sent":10,"acked":10}
        """
        def __init__(self, dict):
            for key,item in dict.iteritems():
                setattr(self, key, item)

        def __str__(self):
            return "sent %s, acked %s" % (self.sent, self.acked)

    def verifiable_consumer(self, topic, group_id, broker_list, max_messages=None, session_timeout=None, verbose=None, enable_auto_commit=None,
                            reset_policy=None, assignment_strategy=None, consumer_config=None, cmd_timeout_secs=30):
        """
        Execute ``kafka-verifiable-consumer.sh`` via `oc/kubectl exec` in the given container of the given pod,
        returning the JSON it emits, or throwing an Exception

        :param topic: The topic to produce records in.
        :param group_id: The consumer group id.
        :param broker_list: The bootstrap brokers.
        :param max_messages: See ``--max-messages``.
        :param session_timeout: A timeout for the consumer session.
        :param verbose: See ``--verbose``
        :param enable_auto_commit: See ``--enable-autocommit``
        :param reset_policy: See ``--reset-policy``
        :param assignment_strategy: See ``--assignment-strategy``
        :param consumer_config: See ``--consumer.config``
        :param cmd_timeout_secs:  A timeout, in seconds. An :py:class:`ExecTimeoutException` will be raised if ``oc/kubectl exec`` takes longer.
        :return:
        """
        LOGGER.info("Consuming %s messages from topic %s", max_messages, topic)
        cmd = ["/opt/kafka/bin/kafka-verifiable-consumer.sh",
               "--topic", topic,
               "--group-id", group_id,
               "--broker-list", broker_list]
        if max_messages:
            cmd += ["--max-messages", str(max_messages)]
        if session_timeout:
            cmd += ["--session-timeout", str(session_timeout)]
        if verbose:
            cmd += ["--verbose"]
        if enable_auto_commit:
            cmd += ["--enable-autocommit"]
        if reset_policy:
            cmd += ["--reset-policy", reset_policy]
        if assignment_strategy:
            cmd += ["--assignment-strategy", assignment_strategy]
        if consumer_config:
            # TODO like the producer config
            cmd += ["--consumer.config", consumer_config]
        # KAFKA-6130 means we have to timeout the command because it doesn't exit
        LOGGER.debug("pod: %s, container: %s, cmd %s", self.pod, self.container, " ".join(cmd))
        try:
            lines_of_json = self.session.exec_(self.pod, cmd, self.container, cmd_timeout_secs=cmd_timeout_secs)
        except ExecTimeoutException as e:
            logging.info("%s timed-out" % (cmd))
            logging.info("KAFKA-6130 means we have to timeout the command because it doesn't exit")
            lines_of_json = e.output
        logging.debug(lines_of_json)
        events = [json.loads(json_string) for json_string in lines_of_json.splitlines()]
        try:
            records_consumed_event = next(event for event in events if event['name'] == 'records_consumed')
            return Kafka.RecordsConsumed(records_consumed_event)
        except StopIteration:
            raise Exception("Couldn't find records_consoc umed event in JSON output of kafka-verifiable-producer.sh: %s" % (events))

    class RecordsConsumed(object):
        """
        Representation of the "records_consumer" event in the JSON output of kafka-verifiable-consumer.sh, e.g.

        {"timestamp":1509019939248,"count":278,"name":"records_consumed","partitions":[{"topic":"my_topic","partition":0,"count":10,"minOffset":82,"maxOffset":91}]}
        """
        def __init__(self, dict):
            for key,item in dict.iteritems():
                setattr(self, key, item)

        def __str__(self):
            return "topic %s, partition %s, count %s" % (self.topic, self.partition, self.count)

    def create_topic(self, zookeeper, topic, partitions=1, replicas=1, disable_rack_aware=False, if_not_exists=False,
                     cmd_timeout_secs=30, **configs):
        LOGGER.info("Creating topics %s", str(topic))
        cmd = ["/opt/kafka/bin/kafka-topics.sh", "--zookeeper", zookeeper, "--create", "--topic", topic, "--partitions", str(partitions)]

        if isinstance(replicas, int):
            cmd += ["--replication-factor", str(replicas)]
        elif isinstance(replicas, list):
            cmd += ["--replica-assignment", replicas]#TODO
        else:
            raise

        if disable_rack_aware:
            cmd += ["--disable-rack-aware", disable_rack_aware]

        if if_not_exists:
            cmd.append("--if-not-exists")

        if configs:
            for key, item in configs.iteritems():
                cmd += ["--config", "%s=%s" % (key, item)]

        LOGGER.debug("pod: %s, container: %s, cmd %s", self.pod, self.container, " ".join(cmd))
        self.session.exec_(self.pod, cmd, self.container, cmd_timeout_secs=cmd_timeout_secs)

    def list_topics(self, zookeeper, cmd_timeout_secs=30):
        """
        Execute ``kafka-topics.sh --list`` via `oc/kubectl exec` in the given container of the given pod.

        :param zookeeper: Zookeeper host:port.
        :param cmd_timeout_secs:  A timeout, in seconds. An :py:class:`ExecTimeoutException` will be raised if ``oc/kubectl exec`` takes longer.
        """
        LOGGER.info("listing topics")
        cmd = ["/opt/kafka/bin/kafka-topics.sh", "--zookeeper", zookeeper, "--list"]
        LOGGER.debug("pod: %s, container: %s, cmd %s", self.pod, self.container, " ".join(cmd))
        return self.session.exec_(self.pod, cmd, self.container, cmd_timeout_secs=cmd_timeout_secs).splitlines()

    def describe_topics(self, zookeeper, topic, cmd_timeout_secs=30):
        """
        Execute ``kafka-topics.sh --describe`` via `oc/kubectl exec` in the given container of the given pod.

        :param zookeeper: Zookeeper host:port.
        :param topic: A topic, or topics or None (to describe all the topics)
        :param cmd_timeout_secs:  A timeout, in seconds. An :py:class:`ExecTimeoutException` will be raised if ``oc/kubectl exec`` takes longer.
        :return:
        """
        LOGGER.info("describing topics %s", str(topic))
        cmd = ["/opt/kafka/bin/kafka-topics.sh", "--zookeeper", zookeeper, "--describe"]
        if isinstance(topic, str):
            cmd += ["--topic", topic]
        elif isinstance(topic, list):
            for t in topic:
                cmd += ["--topic", t]
        LOGGER.debug("pod: %s, container: %s, cmd %s", self.pod, self.container, " ".join(cmd))
        self.session.exec_(self.pod, cmd, self.container, cmd_timeout_secs=cmd_timeout_secs)
        # TODO parse output

    def delete_topic(self, zookeeper, topic, if_exists=False,
                     cmd_timeout_secs=30):
        """
        Execute ``kafka-topics.sh --delete`` via `oc/kubectl exec` in the given container of the given pod

        :param zookeeper: Zookeeper ``host:port``.
        :param topic: A topic or topics
        :param if_exists: See ``--if-exists``.
        :param cmd_timeout_secs:  A timeout, in seconds. An :py:class:`ExecTimeoutException` will be raised if ``oc/kubectl exec`` takes longer.
        :return:
        """
        LOGGER.info("deleting topics %s", str(topic))
        cmd = ["/opt/kafka/bin/kafka-topics.sh", "--zookeeper", zookeeper, "--delete"]

        if if_exists:
            cmd.append("--if-exists")

        if isinstance(topic, str) or isinstance(topic, unicode):
            cmd += ["--topic", topic]
        elif isinstance(topic, list):
            for t in topic:
                cmd += ["--topic", t]
        else:
            raise Exception("topics parameter can't be a " + str(type(topic)))
        LOGGER.debug("pod: %s, container: %s, cmd %s", self.pod, self.container, " ".join(cmd))
        self.session.exec_(self.pod, cmd, self.container, cmd_timeout_secs=cmd_timeout_secs)

    # TODO wrappers for kafka-reassign-partitions.sh etc
    # TODO kill broker, bounce broker
    # TODO also provide a Zookeeper abstraction like this one.