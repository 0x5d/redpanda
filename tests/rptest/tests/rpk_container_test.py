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

from ducktape.utils.util import wait_until

from ducktape.tests.test import Test
from rptest.clients.rpk import RpkTool


class RpkContainerTest(Test):

    def __init__(self, ctx):
        super(RpkContainerTest, self).__init__(ctx)
        # super(RpkToolTest, self).__init__(test_context=ctx)
        self._ctx = ctx
        # self._rpk = RpkTool(self.redpanda)

    def start(self, n=1):
        def condition():
            brokers = self.start_container_cluster(n)
            if len(brokers) != n:
                return False

            for b in brokers:
                if not b.addr or not b.node_id:
                    return False

            return True

        return condition

    def purge(self, n):
        def condition():
            ids = self.purge_container_cluster()
            return len(ids) == n

        return condition

    def stop(self, n):
        def condition():
            ids = self.stop_container_cluster()
            return len(ids) == n

        return condition

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

    def _parse_container_stop_out(self, out):
        ls = [l.strip() for l in out.splitlines()]
        ids = []
        for l in ls:
            m = re.match(r'Stopping node (?P<node_id>\d)', l)
            if m is not None:
                ids.append(m.group('node_id'))
        return ids

    def _rpk_binary(self):
        rp_install_path_root = self._ctx.globals.get(
            "rp_install_path_root", None)
        return f"{rp_install_path_root}/bin/rpk"

    def test_start_purge(self):
        # Preemptively purge any existing clusters
        self.purge_container_cluster()

        wait_until(self.start(3),
                   timeout_sec=10,
                   backoff_sec=1,
                   err_msg="Cluster wasn't created.")

        wait_until(self.purge(3),
                   timeout_sec=10,
                   backoff_sec=1,
                   err_msg="Cluster wasn't purged.")

    def test_restart_purge(self):
        # Preemptively purge any existing clusters
        self.purge_container_cluster()

        wait_until(self.start(3),
                   timeout_sec=10,
                   backoff_sec=1,
                   err_msg="Cluster wasn't created.")

        wait_until(self.stop(3),
                   timeout_sec=10,
                   backoff_sec=1,
                   err_msg="Cluster wasn't stopped.")

        wait_until(self.start(),
                   timeout_sec=10,
                   backoff_sec=1,
                   err_msg="Cluster wasn't restarted.")

        wait_until(self.purge(3),
                   timeout_sec=10,
                   backoff_sec=1,
                   err_msg="Cluster wasn't purged.")

    def _execute(self, cmd, stdin=None, timeout=30):
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
                raise Exception(f'command {" ".join(cmd)} returned {p.returncode}. Output: "{p.stdout}"')

            output = p.stderr.read()

            return output
        except subprocess.CalledProcessError as e:
            raise Exception(f'command {" ".join(cmd)} returned {p.returncode}. Output: "{e.output}"')
