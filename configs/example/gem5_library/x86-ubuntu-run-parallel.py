# Copyright (c) 2026 Sudhanshu Agarwal
# All rights reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met: redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer;
# redistributions in binary form must reproduce the above copyright
# notice, this list of conditions and the following disclaimer in the
# documentation and/or other materials provided with the distribution;
# neither the name of the copyright holders nor the names of its
# contributors may be used to endorse or promote products derived from
# this software without specific prior written permission.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

"""
This script uses X86DemoParallelBoard to boot Ubuntu before exiting.

Usage
-----

```
scons build/X86/gem5.opt
./build/X86/gem5.opt configs/example/gem5_library/x86-ubuntu-run-parallel.py
```
"""

import os

from gem5.prebuilt.demo.x86_demo_parallel_board import X86DemoParallelBoard
from gem5.resources.resource import (
    DiskImageResource,
    KernelResource,
)
from gem5.simulate.simulator import Simulator

board = X86DemoParallelBoard()

resource_dir = os.path.expanduser("~/.cache/gem5")
board.set_kernel_disk_workload(
    kernel=KernelResource(
        os.path.join(resource_dir, "x86-linux-kernel-6.8.0-52-generic-1.0.0")
    ),
    disk_image=DiskImageResource(
        os.path.join(resource_dir, "x86-ubuntu-24.04-img-4.0.0"),
        root_partition="2",
    ),
)

simulator = Simulator(board=board)

simulator.run()
