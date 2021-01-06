# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.utils.util import wait_until

from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool
from rptest.services.rpk_consumer import RpkConsumer

import time
import threading
import random


class RpkToolTest(RedpandaTest):
    def __init__(self, ctx):
        super(RpkToolTest, self).__init__(test_context=ctx)
        self._ctx = ctx
        self._rpk = RpkTool(self.redpanda)

    def test_start_purge(self):
        wait_until(lambda: "lol" in self._rpk.start_container_cluster(),
                   timeout_sec=10,
                   backoff_sec=1,
                   err_msg="Cluster wasn't created.")

