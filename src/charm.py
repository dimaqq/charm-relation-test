#!/usr/bin/env python3
# Copyright 2024 Danny Cocks
# See LICENSE file for licensing details.

"""Charm the application."""

import logging

import ops

from faker import Faker
import time
import subprocess
import json
import tempfile
import os

logger = logging.getLogger(__name__)


class CharmRelationTestCharm(ops.CharmBase):
    """Charm the application."""

    def __init__(self, framework: ops.Framework):
        super().__init__(framework)
        framework.observe(self.on.start, self._on_start)
        framework.observe(self.on.set_random_blob_action, self._on_set_random_blob_action)
        framework.observe(self.on.get_blobs_action, self._on_get_blobs_action)
        framework.observe(self.on.reset_action, self._on_reset_action)


    def _on_start(self, event: ops.StartEvent):
        self.unit.status = ops.ActiveStatus()

    def _on_reset_action(self, event):
        blob_test = self.model.get_relation("blob-test")
        for key in list(blob_test.data[self.unit]):
            del blob_test.data[self.unit][key]

    def _on_set_random_blob_action(self, event):
        blob_test = self.model.get_relation("blob-test")

        size = event.params["size"]
        method = event.params["method"]

        times = []
        for bucket in range(event.params["buckets"]):
            key = f"key-{bucket}"
            for iteration in range(event.params["repetitions"]):
                data = self.gen_fake_data(size)
                t = self.set_relation_data(blob_test, key, data, method)
                event.log(f"Time using {method} for size {len(data)/1024/1024:.3}Mi, bucket {key}, iter {iteration}: {t}")
                times.append(t)

        event.set_results({
            "times": times,
            "max": max(times),
            "avg": sum(times) / len(times),
            "min": min(times),
        })

    def _on_get_blobs_action(self, event):
        blob_test = self.model.get_relation("blob-test")
        if not blob_test or len(blob_test.units) == 0:
            event.fail("No blob-test peer yet - add another unit")
            return

        method = event.params["method"]
        other_unit = list(blob_test.units)[0]

        times = []
        for bucket in range(event.params["buckets"]):
            key = f"key-{bucket}"
            for iteration in range(event.params["repetitions"]):
                t,data = self.get_relation_data(blob_test, key, other_unit, method)
                event.log(f"Time using {method} for size {len(data)/1024/1024:.3}Mi, bucket {key}, iter {iteration}: {t}")
                times.append(t)

        event.set_results({
            "times": times,
            "max": max(times),
            "avg": sum(times) / len(times),
            "min": min(times),
        })

    #####

    def set_relation_data(self, rel, key, data, method):
        if method == "ops":
            tstart = time.time()
            rel.data[self.unit][key] = data
        elif method == "relation-set":
            file = tempfile.NamedTemporaryFile("w", delete=False)
            file.write(json.dumps({key: data}))
            file.close()
            # Don't count the time to create the temp file
            tstart = time.time()
            cmd = ["relation-set", "-r", str(rel.id), "--file", file.name]
            subprocess.check_call(cmd)
            os.unlink(file.name)
        else:
            raise Exception("Shouldn't get here")
        return time.time() - tstart

    def get_relation_data(self, rel, key, other_unit, method):
        tstart = time.time()
        if method == "ops":
            s = rel.data[other_unit][key]
        elif method == "relation-get":
            cmd = ["relation-get", "-r", str(rel.id), key, other_unit.name]
            s = subprocess.check_output(cmd)
        t = time.time() - tstart
        return t,s

    def gen_fake_data(self, size):
        fake = Faker()
        # Speed this up by only randomly generating a small amount and then repeating it
        small_size = min(100, size)
        small = fake.pystr(small_size, small_size)
        return small * (size // small_size)
        

if __name__ == "__main__":  # pragma: nocover
    ops.main(CharmRelationTestCharm)  # type: ignore
