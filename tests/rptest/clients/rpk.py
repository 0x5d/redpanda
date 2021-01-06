# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import subprocess
import tempfile
import time
import re


class RpkPartition:
    def __init__(self, id, leader, replicas, hw):
        self.id = id
        self.leader = leader
        self.replicas = replicas
        self.high_watermark = hw

    def __str__(self):
        return "id: {}, leader: {}, replicas: {}, hw: {}".format(
            self.id, self.leader, self.replicas, self.high_watermark)


class RpkTool:
    """
    Wrapper around rpk.
    """
    def __init__(self, redpanda):
        self._redpanda = redpanda

    def start_container_cluster(self, nodes=3):
        class Broker():
            def __init__(self, node_id, addr):
                self.node_id = node_id
                self.addr = addr

        cmd = [
            self._rpk_binary(),
            'container',
            'start',
            '-n',
            str(nodes),
            '--network', 'rp',
        ]

        def parse_output(out):
            ls = [l.strip() for l in out.splitlines()]
            brokers = []
            for l in ls:
                m = re.match(r"(?P<node_id>\d+)\s+(?P<addr>.+)", l)
                if m is not None:
                    brokers.append(Broker(m.group('node_id'), m.group('addr')))
            return brokers

        return parse_output(self._execute(cmd))

    def purge_container_cluster(self, nodes=3):
        cmd = [self._rpk_binary(), 'container', 'purge']
        return self._parse_container_stop_out(self._execute(cmd))

    def stop_container_cluster(self, nodes=3):
        cmd = [self._rpk_binary(), 'container', 'stop']
        return self._parse_container_stop_out(self._execute(cmd))

    def create_topic(self, topic, partitions=1):
        cmd = ["topic", "create", topic]
        cmd += ["--partitions", str(partitions)]
        return self._run_api(cmd)

    def list_topics(self):
        cmd = ["topic", "list"]

        output = self._run_api(cmd)
        if "No topics found." in output:
            return []

        def topic_line(line):
            parts = line.split()
            assert len(parts) == 3
            return parts[0]

        lines = output.splitlines()
        for i, line in enumerate(lines):
            if line.split() == ["Name", "Partitions", "Replicas"]:
                return map(topic_line, lines[i + 1:])

        assert False, "Unexpected output format"

    def produce(self, topic, key, msg, headers=[], partition=None):
        cmd = [
            'produce', '--brokers',
            self._redpanda.brokers(), '--key', key, topic
        ]
        if headers:
            cmd += ['-H ' + h for h in headers]
        if partition:
            cmd += ['-p', str(partition)]
        return self._run_api(cmd, stdin=msg)

    def describe_topic(self, topic):
        cmd = ['topic', 'describe', topic]
        output = self._run_api(cmd)
        if "not found" in output:
            return None
        lines = output.splitlines()

        def partition_line(line):
            m = re.match(
                r" *(?P<id>\d+) +(?P<leader>\d+) +\[(?P<replicas>.+?)\] + \[.+\] +(?P<hw>\d+) *",
                line)
            if m == None:
                return None
            replicas = map(lambda r: int(r), m.group('replicas').split())
            return RpkPartition(id=int(m.group('id')),
                                leader=int(m.group('leader')),
                                replicas=replicas,
                                hw=int(m.group('hw')))

        return filter(lambda p: p != None, map(partition_line, lines))

    def _run_api(self, cmd, stdin=None, timeout=30):
        cmd = [
            self._rpk_binary(), "api", "--brokers",
            self._redpanda.brokers(1)
        ] + cmd
        return self._execute(cmd, stdin=stdin, timeout=timeout)

    def _execute(self, cmd, stdin=None, timeout=30):
        self._redpanda.logger.debug("Executing command: %s", cmd)
        try:
            output = None
            f = subprocess.PIPE

            if stdin:
                f = tempfile.TemporaryFile()
                f.write(stdin)
                f.seek(0)

            # rpk logs everything on STDERR by default
            p = subprocess.Popen(cmd,
                                 stderr=subprocess.PIPE,
                                 stdin=f,
                                 text=True)
            start_time = time.time()

            ret = None
            while time.time() < start_time + timeout:
                ret = p.poll()
                if ret != None:
                    break
                time.sleep(0.5)

            if ret is None:
                p.terminate()

            if p.returncode:
                raise Exception('command %s returned %d' %
                                (' '.join(cmd), p.returncode))

            output = p.stderr.read()

            self._redpanda.logger.debug(output)
            return output
        except subprocess.CalledProcessError as e:
            self._redpanda.logger.info("Error (%d) executing command: %s",
                                       e.returncode, e.output)

    def _rpk_binary(self):
        return self._redpanda.find_binary("rpk")

    def _parse_container_stop_out(self, out):
        ls = [l.strip() for l in out.splitlines()]
        ids = []
        for l in ls:
            m = re.match(r'Stopping node (?P<node_id>\d)', l)
            if m is not None:
                ids.append(m.group('node_id'))
        return ids
