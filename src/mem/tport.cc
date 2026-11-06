/*
 * Copyright (c) 2012 ARM Limited
 * All rights reserved.
 *
 * The license below extends only to copyright in the software and shall
 * not be construed as granting a license to any other intellectual
 * property including but not limited to intellectual property relating
 * to a hardware implementation of the functionality of the software
 * licensed hereunder.  You may use the software subject to the license
 * terms below provided that you ensure that this notice is replicated
 * unmodified and in its entirety in all distributions of the software,
 * modified or unmodified, in source code or in binary form.
 *
 * Copyright (c) 2006 The Regents of The University of Michigan
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met: redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer;
 * redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution;
 * neither the name of the copyright holders nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "mem/tport.hh"

#include "base/trace.hh"
#include "debug/SimpleTimingPort.hh"
#include "sim/eventq.hh"
#include "sim/sim_object.hh"

namespace gem5
{

SimpleTimingPort::SimpleTimingPort(const std::string &_name, SimObject *_owner)
    : QueuedResponsePort(_name, queueImpl),
      queueImpl(*_owner, *this),
      eventq(_owner->eventQueue())
{}

DrainState
SimpleTimingPort::drain()
{
    DPRINTF(SimpleTimingPort, "drain: %u pending requests\n",
            pendingReqs.load());
    return pendingReqs == 0 ? DrainState::Drained : DrainState::Draining;
}

void
SimpleTimingPort::recvFunctional(PacketPtr pkt)
{
    if (!respQueue.trySatisfyFunctional(pkt)) {
        // do an atomic access and throw away the returned latency
        recvAtomic(pkt);
    }
}

bool
SimpleTimingPort::recvTimingReq(PacketPtr pkt)
{
    // the SimpleTimingPort should not be used anywhere where there is
    // a need to deal with snoop responses and their flow control
    // requirements
    if (pkt->cacheResponding())
        panic("SimpleTimingPort should never see packets with the "
              "cacheResponding flag set\n");

    Tick latency = pkt->headerDelay + pkt->payloadDelay;
    pkt->headerDelay = pkt->payloadDelay = 0;

    pendingReqs++;
    DPRINTF(SimpleTimingPort,
            "recvTimingReq: %s addr %#x, process at "
            "%llu (delay %llu), %u pending\n",
            pkt->cmdString(), pkt->getAddr(), curTick() + latency, latency,
            pendingReqs.load());
    EventFunctionWrapper *event =
        new EventFunctionWrapper([this, pkt] { processTimingReq(pkt); },
                                 "simpleTimingPort.processTimingReq", true);
    eventq->schedule(event, curTick() + latency);
    return true;
}

void
SimpleTimingPort::processTimingReq(PacketPtr pkt)
{
    DPRINTF(SimpleTimingPort, "processTimingReq: %s addr %#x\n",
            pkt->cmdString(), pkt->getAddr());
    bool needsResponse = pkt->needsResponse();
    Tick latency = recvAtomic(pkt);
    // turn packet around to go back to requestor if response expected
    if (needsResponse) {
        // recvAtomic() should already have turned packet into
        // atomic response
        assert(pkt->isResponse());
        DPRINTF(SimpleTimingPort,
                "processTimingReq: response at %llu curtick %llu\n",
                curTick() + latency, curTick());
        schedTimingResp(pkt, curTick() + latency);
    } else {
        DPRINTF(SimpleTimingPort, "processTimingReq: no response\n");
        delete pkt;
    }

    pendingReqs--;
    DPRINTF(SimpleTimingPort, "processTimingReq: %u pending\n",
            pendingReqs.load());
    if (pendingReqs == 0) {
        signalDrainDone();
    }
}

} // namespace gem5
