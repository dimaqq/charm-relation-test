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
        framework.observe(self.on.config_changed, self._on_config_changed)
        framework.observe(self.on.monitors_relation_joined, self._on_monitors_relation_joined)
        framework.observe(self.on.blob_test_relation_joined, self._on_blob_test_relation_joined)
        framework.observe(self.on.mon_consume_relation_joined, self._on_mon_consume_relation_joined)
        framework.observe(self.on.mon_consume_relation_changed, self._on_mon_consume_relation_changed)

    def _on_start(self, event: ops.StartEvent):
        """Handle start event."""
        self.unit.status = ops.ActiveStatus()

    def _on_config_changed(self, event: ops.ConfigChangedEvent):
        logger.info("Running config_changed handler")

        self.update_monitors_data()
        self.update_peer_blobs()

    def _on_monitors_relation_joined(self, event):
        logger.info("Running monitors_relation_joined handler")
        event.relation.data[self.unit]["zzz"] = str(42)

    def _on_blob_test_relation_joined(self, event):
        logger.info("Running blob_test_relation_joined handler")
        self.update_peer_blobs()

    def _on_mon_consume_relation_joined(self, event):
        name = event.unit.name
        data = event.relation.data[event.unit].get("monitors", "")
        logger.info(f"The unit {name} joined the mon-consume relation with {len(data)} sized 'monitors' data")
    def _on_mon_consume_relation_changed(self, event):
        name = event.unit.name
        data = event.relation.data[event.unit].get("monitors", "")
        logger.info(f"The unit {name} changed their data with {len(data)} sized 'monitors' data")

    #####

    def update_kind(self):
        update_kind = self.model.config["relation-update-kind"]
        if self.model.config["relation-update-kind"] not in ["ops","binary","binary-yaml"]:
            self.unit.status = ops.FailedStatus(f"{update_kind} is not 'ops', or 'binary'")
            raise Exception("Can't continue with bad update kind")
        return update_kind

    def update_peer_blobs(self):
        blob_test = self.model.get_relation("blob-test")
        if not blob_test:
            return

        rel_size = self.model.config["size-relation-data"]
        fake = Faker()

        data = fake.text(rel_size)

        t = self.set_relation_data(blob_test, "data", data)
        logger.warning(f"Time to set faker data of size {rel_size/1024/1024:.3}Mi: {t}")

        data = fake.text(rel_size)
        t = self.set_relation_data(blob_test, "data", data)
        logger.warning(f"Time to set 2nd set of faker data of size {rel_size/1024/1024:.3}Mi: {t}")

    def update_monitors_data(self):
        for mon_rel in self.model.relations["monitors"]:
            old_val = mon_rel.data[self.unit].get("config_changed", "0")
            mon_rel.data[self.unit]["config_changed"] = str(int(old_val) + 1)

            fake = Faker()
            # Creating a set of fake monitors
            mons = {}
            # My own dodgy version of uniqueness
            nwords = 3
            for i in range(self.model.config["num-monitors"]):
                for attempt in range(100):
                    name = ''.join(x.capitalize() for x in fake.words(nwords))
                    if name in mons:
                        continue
                    mons[name] = {"command": name}
                    break
                else:
                    raise Exception("Failed to find a new unique name")

            tstart = time.time()
            s = json.dumps(mons)
            t = self.set_relation_data(mon_rel, "monitors", s)
            logger.warning(f"Time to set monitors data (size {len(s)/1024/1024:.3}Mi): {t}")


    #####

    def set_relation_data(self, rel, key, data):
        tstart = time.time()
        if self.update_kind() == "ops":
            rel.data[self.unit][key] = data
        elif self.update_kind() == "binary":
            cmd = ["relation-set", "-r", str(rel.id), f"{key}={data}"]
            if self.model.config["debug"]:
                logger.debug(f"Trying to run: {cmd}")
            subprocess.check_call(cmd)
        elif self.update_kind() == "binary-yaml":
            file = tempfile.NamedTemporaryFile("w", delete=False)
            file.write(json.dumps({key: data}))
            file.close()
            # Don't count the time to create the temp file
            tstart = time.time()
            cmd = ["relation-set", "-r", str(rel.id), "--file", file.name]
            if self.model.config["debug"]:
                logger.debug(f"Trying to run: {cmd}")
            subprocess.check_call(cmd)
            os.unlink(file.name)
        else:
            raise Exception("Shouldn't get here")
        logger.warning(f"Set using {self.update_kind()}")
        return time.time() - tstart

if __name__ == "__main__":  # pragma: nocover
    ops.main(CharmRelationTestCharm)  # type: ignore
