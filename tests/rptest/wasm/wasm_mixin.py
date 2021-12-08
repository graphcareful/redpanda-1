# Copyright 2021 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import uuid
import os
import random
import string
from rptest.clients.rpk import RpkTool
from rptest.wasm.wasm_build_tool import WasmBuildTool


def random_string(N):
    return ''.join(
        random.choice(string.ascii_uppercase + string.digits)
        for _ in range(N))


class WasmScript:
    def __init__(self, inputs=[], outputs=[], script=None):
        self.name = random_string(10)
        self.inputs = inputs
        self.outputs = outputs
        self.script = script
        self.dir_name = str(uuid.uuid4())

    def get_artifact(self, build_dir):
        artifact = os.path.join(build_dir, self.dir_name, "dist",
                                f"{self.dir_name}.js")
        if not os.path.exists(artifact):
            raise Exception(f"Artifact {artifact} was not built")
        return artifact


class WasmMixin():
    """
    Include this mixin in your tests to work with coprocessors.

    You must ensure you set developer_mode & enable_coproc to true
    in your extra_rp_conf options dictionary
    """
    def __init__(self):
        self._rpk_tool = RpkTool(self.redpanda)
        self._build_tool = WasmBuildTool(self._rpk_tool)
        self.scripts = {}

    def deploy_coprocessor(self, script):
        self._build_tool.build_test_artifacts(script)

        self._rpk_tool.wasm_deploy(
            script.get_artifact(self._build_tool.work_dir), script.name,
            "ducktape")

        self.scripts[script.name] = script

    def remove_coprocessor(self, identifier):
        if identifier not in scripts:
            raise Exception(
                "Attempted to remove coprocessor that was never deployed")

        self._rpk_tool.wasm_remove(identifier)

        del self.scripts[identifier]

    def restart_wasm_engine(self, node):
        self.logger.info(
            f"Begin manually triggered restart of wasm engine on node {node}")
        node.account.kill_process("bin/node", clean_shutdown=False)
        self.redpanda.start_wasm_engine(node)
