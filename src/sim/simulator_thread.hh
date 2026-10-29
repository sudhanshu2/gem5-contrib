#ifndef __SIM_SIMULATOR_THREAD_HH__
#define __SIM_SIMULATOR_THREAD_HH__

#include <barrier>
#include <cstdint>
#include <list>
#include <stop_token>
#include <thread>

namespace gem5
{

class Event;
class EventQueue;

class SimulatorThread
{
  public:
    SimulatorThread(EventQueue *_queue, bool _main_queue)
        : queue(_queue), main_queue(_main_queue)
    {}
    void run();
    void stop();
    void wait();
    Event *exitEvent() const { return exit_event; }

  protected:
    Event *simulate();

    EventQueue *queue;
    bool main_queue;
    Event *exit_event = nullptr;
    std::stop_token stop_token;
    std::jthread thread;
};

class SimulatorThreadController
{
  public:
    SimulatorThreadController(uint32_t num_queues);
    ~SimulatorThreadController() { terminate(); };

    Event *run();
    void terminate();

  protected:
    uint32_t num_queues;
    bool started;
    std::list<SimulatorThread> threads;
};

} // namespace gem5

#endif // __SIM_SIMULATOR_THREAD_HH__
