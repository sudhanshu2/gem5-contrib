#include <gtest/gtest.h>

#include <cstdlib>

#include "dev/x86/i8254.hh"
#include "mem/packet.hh"
#include "mem/port_wrapper.hh"
#include "mem/request.hh"
#include "sim/clock_domain.hh"
#include "sim/eventq.hh"

class TestI8254 : public gem5::X86ISA::I8254
{
  public:
    using gem5::X86ISA::I8254::I8254;

    const gem5::Event &
    counterEvent(unsigned int num) const
    {
        return pit.counterEvent(num);
    }
};

gem5::ClockDomain *
createClockDomain()
{
    return new gem5::ClockDomain(*new gem5::ClockDomainParams{}, nullptr);
}

TestI8254 *
createTestI8254(gem5::ClockDomain *clk_domain)
{
    gem5::I8254Params *pit_params = new gem5::I8254Params{};
    pit_params->eventq_index = 0;
    pit_params->clk_domain = clk_domain;
    TestI8254 *test_i8254 = new TestI8254(*pit_params);
    test_i8254->startup();
    return test_i8254;
}

gem5::RequestPortWrapper *
createPort(TestI8254 *test_i8254)
{
    gem5::RequestPortWrapper *port =
        new gem5::RequestPortWrapper("iobus.mem_side_port");
    port->bind(test_i8254->getPort("pio"));
    return port;
}

void
writeByte(gem5::RequestPortWrapper *port, gem5::Addr addr, uint8_t data)
{
    gem5::RequestPtr req(new gem5::Request(addr, 1, 0, 0));
    gem5::Packet *pkt = new gem5::Packet(req, gem5::MemCmd::WriteReq);
    pkt->dataStatic(&data);
    port->sendAtomic(pkt);
}

void
loadCount(gem5::RequestPortWrapper *port)
{
    writeByte(port, 0, 1);
    writeByte(port, 0, 1);
}

class I8254ParallelTest : public ::testing::Test
{
  protected:
    void
    SetUp() override
    {
        GTEST_FLAG_SET(death_test_style, "threadsafe");
        gem5::getEventQueue(0)->setNextSimQuantum(0);
    }
};

TEST_F(I8254ParallelTest, Baseline)
{
    EXPECT_EXIT(
        {
            gem5::inParallelMode = true;
            gem5::EventQueue *main_eq = gem5::getEventQueue(0);

            gem5::ClockDomain *clk_domain = createClockDomain();
            TestI8254 *test_i8254 = createTestI8254(clk_domain);
            gem5::RequestPortWrapper *port = createPort(test_i8254);

            gem5::curEventQueue(main_eq);

            loadCount(port);
            loadCount(port);

            if (main_eq->getHead() == &test_i8254->counterEvent(0)) {
                std::exit(0);
            } else {
                std::exit(1);
            }
        },
        ::testing::ExitedWithCode(0), "");
}

TEST_F(I8254ParallelTest, SingleCPUParallel)
{
    EXPECT_EXIT(
        {
            gem5::inParallelMode = true;
            gem5::EventQueue *main_eq = gem5::getEventQueue(0);
            gem5::EventQueue *cpu_eq = gem5::getEventQueue(1);

            gem5::ClockDomain *clk_domain = createClockDomain();
            TestI8254 *test_i8254 = createTestI8254(clk_domain);
            gem5::RequestPortWrapper *port = createPort(test_i8254);

            gem5::curEventQueue(cpu_eq);

            cpu_eq->lock();

            loadCount(port);
            loadCount(port);

            cpu_eq->unlock();

            gem5::curEventQueue(main_eq);
            main_eq->handleAsyncInsertions();

            if (main_eq->getHead() == &test_i8254->counterEvent(0)) {
                std::exit(0);
            } else {
                std::exit(1);
            }
        },
        ::testing::ExitedWithCode(0), "");
}
