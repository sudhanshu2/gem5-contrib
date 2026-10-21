/*
 * Copyright (c) 2017, 2021 Arm Limited
 * All rights reserved
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
 * Copyright (c) 2013 Advanced Micro Devices, Inc.
 * Copyright (c) 2013 Mark D. Hill and David A. Wood
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

#include "pybind11/pybind11.h"
#include "pybind11/stl.h"

#include <unordered_map>
#include <utility>
#include <vector>

#include "base/logging.hh"
#include "sim/control_queue.hh"
#include "sim/eventq.hh"
#include "sim/sim_events.hh"
#include "sim/sim_exit.hh"
#include "sim/simulate.hh"

namespace py = pybind11;

namespace gem5
{

namespace
{

/**
 * The queue each scheduled Python event is on. In parallel mode, Python
 * events aimed at event queue 0 live on the control queue while a
 * simulation runs (see sim/control_queue.hh). Only accessed with the GIL
 * held.
 */
std::unordered_map<Event *, EventQueue *> pyEventQueues;

} // anonymous namespace

/**
 * PyBind wrapper for Events
 *
 * We need to wrap the Event class with some Python glue code to
 * enable method overrides in Python and memory management. Unlike its
 * C++ cousin, PyEvents need to override __call__ instead of
 * Event::process().
 *
 * Memory management is done using reference counting in Python.
 */
class PyEvent : public Event
{
  public:
    PyEvent(Event::Priority priority)
        : Event(priority, Event::Managed)
    {
    }

    void process() override {
        // Call the Python implementation as __call__. This provides a
        // slightly more Python-friendly interface.
        PYBIND11_OVERLOAD_PURE_NAME(void, PyEvent, "__call__", process);
    }

  protected:
    // These can run on an event queue's worker thread (schedule,
    // deschedule and serviceOne), which does not hold the GIL.
    void acquireImpl() override {
        py::gil_scoped_acquire gil;
        py::object obj = py::cast(this);

        if (obj) {
            obj.inc_ref();
        } else {
            panic("Failed to get PyBind object to increase ref count\n");
        }
    }

    void releaseImpl() override {
        py::gil_scoped_acquire gil;
        if (!scheduled()) {
            pyEventQueues.erase(this);
        }
        py::object obj = py::cast(this);

        if (obj) {
            obj.dec_ref();
        } else {
            panic("Failed to get PyBind object to decrease ref count\n");
        }
    }
};

namespace
{

/** Move the Python events on `from` to `to`, keeping their times. */
void
movePyEvents(EventQueue *from, EventQueue *to)
{
    py::gil_scoped_acquire gil;

    std::vector<PyEvent *> events;
    for (auto &[event, queue] : pyEventQueues) {
        if (queue == from && event->scheduled()) {
            events.push_back(static_cast<PyEvent *>(event));
        }
    }

    for (PyEvent *event : events) {
        // Descheduling drops the queue's reference to the Python object;
        // hold one until the event is on the new queue.
        py::object keep = py::cast(event);
        Tick when = event->when();
        from->deschedule(event);
        to->schedule(event, when);
        pyEventQueues[event] = to;
    }
}

} // anonymous namespace

void
pybind_init_event(py::module_ &m_native)
{
    control::setEventMover(&movePyEvents);

    py::module_ m = m_native.def_submodule("event");

    m.def(
        "simulate",
        [](Tick ticks) {
            // In parallel mode every event queue, including queue 0,
            // runs on its own worker thread while this thread waits.
            // Release the GIL so Python callbacks from those threads
            // (PyEvents, stat dumps) can run.
            if (numMainEventQueues > 1) {
                py::gil_scoped_release release;
                return simulate(ticks);
            }
            return simulate(ticks);
        },
        py::arg("ticks") = MaxTick);
    m.def("setMaxTick", &set_max_tick, py::arg("tick"));
    m.def("getMaxTick", &get_max_tick, py::return_value_policy::copy);
    m.def("terminateEventQueueThreads", &terminateEventQueueThreads);
    /* Python-level deprecation wrapper for the legacy exitSimLoop API.
     * Preserve the original behavior and ABI but emit a DeprecationWarning
     * so Python callers start migrating to the replacement matching their
     * compatibility requirements.
     */
    m.def(
        "exitSimLoop",
        [](const std::string &message, int exit_code = 0,
           py::object when = py::none(), py::object repeat = py::none(),
           bool serialize = false) {
            const Tick tick = when.is_none() ? curTick() : when.cast<Tick>();
            const Tick repeat_tick =
                repeat.is_none() ? 0 : repeat.cast<Tick>();
            py::module_ warnings = py::module_::import("warnings");
            warnings.attr("warn")(
                "exitSimLoop is deprecated; use exitSimulationLoopClassic "
                "to preserve legacy cause/code behavior, or "
                "exitSimulationLoop for hypercall-based exits",
                py::module_::import("builtins").attr("DeprecationWarning"));
            exitSimulationLoopClassic(message, exit_code, tick, repeat_tick,
                                      serialize);
        },
        py::arg("message"), py::arg("exit_code") = 0,
        py::arg("when") = py::none(), py::arg("repeat") = py::none(),
        py::arg("serialize") = false);
    m.def(
        "exitSimulationLoop",
        [](uint64_t hypercall_id,
           std::map<std::string, std::string> payload =
               std::map<std::string, std::string>(),
           py::object when = py::none(), py::object repeat = py::none()) {
            const Tick tick = when.is_none() ? curTick() : when.cast<Tick>();
            const Tick repeat_tick =
                repeat.is_none() ? 0 : repeat.cast<Tick>();
            exitSimulationLoop(hypercall_id, std::move(payload), tick,
                               repeat_tick);
        },
        py::arg("hypercall_id"),
        py::arg("payload") = std::map<std::string, std::string>(),
        py::arg("when") = py::none(), py::arg("repeat") = py::none());
    m.def(
        "exitSimulationLoop",
        [](ExitHypercall hypercall,
           std::map<std::string, std::string> payload =
               std::map<std::string, std::string>(),
           py::object when = py::none(), py::object repeat = py::none()) {
            const Tick tick = when.is_none() ? curTick() : when.cast<Tick>();
            const Tick repeat_tick =
                repeat.is_none() ? 0 : repeat.cast<Tick>();
            exitSimulationLoop(hypercall, std::move(payload), tick,
                               repeat_tick);
        },
        py::arg("hypercall"),
        py::arg("payload") = std::map<std::string, std::string>(),
        py::arg("when") = py::none(), py::arg("repeat") = py::none());
    m.def(
        "exitSimulationLoopClassic",
        [](const std::string &message, int exit_code = 0,
           py::object when = py::none(), py::object repeat = py::none(),
           bool serialize = false) {
            const Tick tick = when.is_none() ? curTick() : when.cast<Tick>();
            const Tick repeat_tick =
                repeat.is_none() ? 0 : repeat.cast<Tick>();
            exitSimulationLoopClassic(message, exit_code, tick, repeat_tick,
                                      serialize);
        },
        py::arg("message"), py::arg("exit_code") = 0,
        py::arg("when") = py::none(), py::arg("repeat") = py::none(),
        py::arg("serialize") = false);
    m.def(
        "exitSimulationLoopClassicNow",
        [](const std::string &message, int exit_code = 0,
           py::object repeat = py::none(), bool serialize = false) {
            const Tick repeat_tick =
                repeat.is_none() ? 0 : repeat.cast<Tick>();
            exitSimulationLoopClassicNow(message, exit_code, repeat_tick,
                                         serialize);
        },
        py::arg("message"), py::arg("exit_code") = 0,
        py::arg("repeat") = py::none(), py::arg("serialize") = false);
    m.def("getEventQueue", []() { return curEventQueue(); },
          py::return_value_policy::reference);
    m.def("setEventQueue", [](EventQueue *q) { return curEventQueue(q); });
    m.def("getEventQueue", &getEventQueue,
          py::return_value_policy::reference);

    auto exit_hypercall = py::enum_<ExitHypercall>(m, "ExitHypercall");
    for (const auto &desc : kExitHypercallDescriptors) {
        exit_hypercall.value(desc.name, desc.id, desc.description);
    }
    exit_hypercall.export_values();

    py::class_<EventQueue>(m, "EventQueue")
        .def("name", [](EventQueue *eq) { return eq->name(); })
        .def("dump", &EventQueue::dump)
        .def(
            "schedule",
            [](EventQueue *eq, PyEvent *e, Tick t) {
                EventQueue *q = control::targetQueue(eq);
                q->schedule(e, t);
                pyEventQueues[e] = q;
            },
            py::arg("event"), py::arg("when"))
        .def(
            "deschedule",
            [](EventQueue *eq, PyEvent *e) {
                auto it = pyEventQueues.find(e);
                (it != pyEventQueues.end() ? it->second : eq)->deschedule(e);
            },
            py::arg("event"))
        .def(
            "reschedule",
            [](EventQueue *eq, PyEvent *e, Tick t, bool always) {
                auto it = pyEventQueues.find(e);
                EventQueue *q = e->scheduled() && it != pyEventQueues.end()
                                    ? it->second
                                    : control::targetQueue(eq);
                q->reschedule(e, t, always);
                pyEventQueues[e] = q;
            },
            py::arg("event"), py::arg("tick"), py::arg("always") = false);

    // TODO: Ownership of global exit events has always been a bit
    // questionable. We currently assume they are owned by the C++
    // world. This is what the old SWIG code did, but that will result
    // in memory leaks.
    py::class_<GlobalSimLoopExitEvent,
               std::unique_ptr<GlobalSimLoopExitEvent, py::nodelete>>(
               m, "GlobalSimLoopExitEvent")
        .def("getCause", &GlobalSimLoopExitEvent::getCause)
        .def("getCode", &GlobalSimLoopExitEvent::getCode)
        .def("getPayload", &GlobalSimLoopExitEvent::getPayload)
        .def("getHypercallId", &GlobalSimLoopExitEvent::getHypercallId)
        ;

    // Event base class. These should never be returned directly to
    // Python since they don't have a well-defined life cycle. Python
    // events should be derived from PyEvent instead.
    py::class_<Event> c_event(
        m, "Event");
    c_event
        .def("name", &Event::name)
        .def("dump", &Event::dump)
        .def("scheduled", &Event::scheduled)
        .def("squash", &Event::squash)
        .def("squashed", &Event::squashed)
        .def("isExitEvent", &Event::isExitEvent)
        .def("when", &Event::when)
        .def("priority", &Event::priority)
        ;

    py::class_<PyEvent, Event>(m, "PyEvent")
        .def(py::init<Event::Priority>(),
             py::arg("priority") = (int)Event::Default_Pri)
        ;

#define PRIO(n) c_event.attr(# n) = py::cast((int)Event::n)
    PRIO(Minimum_Pri);
    PRIO(CPU_Exit_Pri);
    PRIO(Debug_Enable_Pri);
    PRIO(Debug_Break_Pri);
    PRIO(CPU_Switch_Pri);
    PRIO(Delayed_Writeback_Pri);
    PRIO(Default_Pri);
    PRIO(DVFS_Update_Pri);
    PRIO(Serialize_Pri);
    PRIO(CPU_Tick_Pri);
    PRIO(Stat_Event_Pri);
    PRIO(Progress_Event_Pri);
    PRIO(Sim_Exit_Pri);
    PRIO(Maximum_Pri);
}

} // namespace gem5
