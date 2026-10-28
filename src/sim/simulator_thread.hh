#ifndef __SIM_SIMULATOR_THREAD_HH__
#define __SIM_SIMULATOR_THREAD_HH__

#include <atomic>
#include <barrier>
#include <cstdint>
#include <list>
#include <thread>

namespace gem5
{

class EventQueue;

class SimulatorThread
{
  public:
    SimulatorThread(EventQueue *queue, std::barrier<> &barrier,
                    std::atomic<bool> &terminate);
    void start();
    void execute();
    void join();

  protected:
    EventQueue *queue;
    std::barrier<> &barrier;
    std::atomic<bool> &terminate;
    std::jthread thread;
};

class SimulatorThreads
{
  public:
    SimulatorThreads(uint32_t num_queues);

    ~SimulatorThreads();

    void runUntilLocalExit();
    void terminateThreads();

  protected:
    std::atomic<bool> terminate;
    bool started = false;
    std::list<SimulatorThread> threads;
    std::barrier<> barrier;
};

} // namespace gem5

#endif // __SIM_SIMULATOR_THREAD_HH__
