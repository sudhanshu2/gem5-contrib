#include <gtest/gtest.h>

#include <cstdlib>

#include "dev/x86/i8254.hh"
#include "mem/noncoherent_xbar.hh"
#include "mem/packet_access.hh"
#include "mem/port_wrapper.hh"

using namespace gem5;

namespace
{

class FixedClockDomain : public ClockDomain
{
  public:
    FixedClockDomain() : ClockDomain(*new ClockDomainParams{}, nullptr)
    {
        _clockPeriod = 1;
    }
};

bool
pitRecvTimingResp(PacketPtr pkt)
{
    delete pkt;
    return true;
}

void
writeI8254(RequestPortWrapper *port, PacketPtr pkt)
{
    port->sendTimingReq(pkt);
}

void
sendCpuEvent(RequestPortWrapper &port, EventQueue *cpu_eq, Tick when)
{
    curEventQueue(cpu_eq);
    RequestPtr req(new Request(0, 1, 0, 0));
    PacketPtr pkt = new Packet(req, MemCmd::WriteReq);
    pkt->allocate();
    pkt->setLE<uint8_t>(1);
    EventFunctionWrapper *send = new EventFunctionWrapper(
        std::bind(writeI8254, &port, pkt), "send", true);
    cpu_eq->schedule(send, when);
    cpu_eq->serviceOne();
}

void
drainMainQueue(EventQueue *main_eq, Tick until)
{
    curEventQueue(main_eq);
    main_eq->handleAsyncInsertions();
    while (!main_eq->empty() && main_eq->nextTick() <= until) {
        main_eq->serviceOne();
    }
}

} // anonymous namespace

TEST(I8254ParallelTest, I8254CpuWrite)
{
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_EXIT(
        {
            fixClockFrequency();
            inParallelMode = true;

            EventQueue *main_eq = getEventQueue(0);
            EventQueue *cpu_eq = getEventQueue(1);
            main_eq->setNextSimQuantum(0);

            FixedClockDomain *clk_domain = new FixedClockDomain();

            I8254Params *pit_params = new I8254Params{};
            pit_params->clk_domain = clk_domain;
            X86ISA::I8254 *pit = new X86ISA::I8254(*pit_params);

            NoncoherentXBarParams *xbar_params = new NoncoherentXBarParams{};
            xbar_params->clk_domain = clk_domain;
            xbar_params->width = 1;
            xbar_params->port_cpu_side_ports_connection_count = 1;
            xbar_params->port_mem_side_ports_connection_count = 1;
            NoncoherentXBar *iobus = new NoncoherentXBar(*xbar_params);

            RequestPortWrapper seq_port("seq_port");
            seq_port.setTimingCallbacks(pitRecvTimingResp, [] {});

            seq_port.bind(iobus->getPort("cpu_side_ports", 0));
            iobus->getPort("mem_side_ports", 0).bind(pit->getPort("pio"));
            iobus->regStats();
            pit->init();
            pit->startup();

            sendCpuEvent(seq_port, cpu_eq, 100);
            drainMainQueue(main_eq, 150);
            sendCpuEvent(seq_port, cpu_eq, 200);
            drainMainQueue(main_eq, 250);
            sendCpuEvent(seq_port, cpu_eq, 300);
            drainMainQueue(main_eq, 350);

            std::exit(main_eq->empty() ? 0 : 1);
        },
        ::testing::ExitedWithCode(0), "");
}
