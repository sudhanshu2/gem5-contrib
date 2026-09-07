# Copyright 2026 Google, Inc.
#
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
#
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

from typing import Optional

import m5
from m5.objects import Root

from ...components.boards.x86_board import X86Board
from ...components.cachehierarchies.classic.private_l1_shared_l2_walk_cache_hierarchy import (
    PrivateL1SharedL2WalkCacheHierarchy,
)
from ...components.memory.multi_channel import DualChannelDDR4_2400
from ...components.processors.cpu_types import CPUTypes
from ...components.processors.simple_processor import SimpleProcessor
from ...isas import ISA
from ...utils.override import overrides
from ...utils.requires import requires


class X86DemoParallelBoard(X86Board):
    """
    This prebuilt X86 board is used for demonstrating parallelism in gem5.
    It simulates an X86 3GHz dual-core system with a 4GiB DDR4_2400 memory
    system. The cache hierarchy consists of per-core private L1 instruction and
    data caches (64KiB each) connected to a shared 8MiB L2 cache.
    """

    def __init__(self):
        requires(
            isa_required=ISA.X86,
        )

        memory = DualChannelDDR4_2400("4GiB")
        processor = SimpleProcessor(
            cpu_type=CPUTypes.TIMING,
            isa=ISA.X86,
            num_cores=2,
            clk_freq="3GHz",
        )

        cache_hierarchy = PrivateL1SharedL2WalkCacheHierarchy(
            l1d_size="64KiB", l1i_size="64KiB", l2_size="8MiB"
        )

        super().__init__(
            clk_freq="3GHz",
            processor=processor,
            memory=memory,
            cache_hierarchy=cache_hierarchy,
        )

    @overrides(X86Board)
    def _pre_instantiate(self, full_system: Optional[bool] = None) -> Root:
        root = super()._pre_instantiate(full_system)

        # Cores and L1 Caches to event queue of core number + 1
        for i, core in enumerate(self.get_processor().get_cores()):
            core_obj = core.get_simobject()
            core_obj.eventq_index = i + 1
            for obj in core_obj.descendants():
                obj.eventq_index = i + 1

            # L1 Caches
            l1i = self.cache_hierarchy.l1icaches[i]
            l1i.eventq_index = i + 1
            for obj in l1i.descendants():
                obj.eventq_index = i + 1

            l1d = self.cache_hierarchy.l1dcaches[i]
            l1d.eventq_index = i + 1
            for obj in l1d.descendants():
                obj.eventq_index = i + 1

        # L2 Cache and DRAM to event queue 0
        l2 = self.cache_hierarchy.l2cache
        l2.eventq_index = 0
        for obj in l2.descendants():
            obj.eventq_index = 0

        for ctrl in self.memory.get_memory_controllers():
            ctrl.eventq_index = 0
            for obj in ctrl.descendants():
                obj.eventq_index = 0

        # Set simulation quantum to a single cycle to preserve behavior with
        # single threaded gem5.
        m5.ticks.fixGlobalFrequency()

        core_simobj = self.get_processor().get_cores()[0].get_simobject()
        clk_domain = core_simobj.clk_domain

        # Resolve proxy if necessary
        if m5.proxy.isproxy(clk_domain):
            clk_domain = clk_domain.unproxy(core_simobj)

        # clk_domain.clock is a VectorParam.Clock. Get the first one.
        clock = clk_domain.clock[0]
        if m5.proxy.isproxy(clock):
            clock = clock.unproxy(clk_domain)

        clock_period_ticks = clock.getValue()
        root.sim_quantum = 1 * clock_period_ticks

        return root
