/*
 * Copyright (c) 2026 ARCANA Research Group (University of Illinois
 * Urbana-Champaign) All rights reserved.
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

#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "base/async_queue.hh"

using namespace gem5;

// Checks that queue is FIFO ordered in a single threaded test
TEST(AsyncQueueTest, FifoOrdering)
{
    AsyncQueue<int> async_queue;

    async_queue.push(1);
    async_queue.push(2);
    async_queue.push(3);
    EXPECT_EQ(async_queue.size(), 3);

    EXPECT_EQ(async_queue.pop(), 1);
    EXPECT_EQ(async_queue.pop(), 2);
    EXPECT_EQ(async_queue.pop(), 3);

#ifndef NDEBUG
    EXPECT_ANY_THROW(async_queue.pop());
#endif
    EXPECT_EQ(async_queue.size(), 0);
}

// One producer and the consumer waits for producer to finish
TEST(AsyncQueueTest, SerialPush)
{
    const int num_items = 10000;
    AsyncQueue<int> async_queue;

    std::thread producer([&async_queue]() {
        for (int i = 0; i < num_items; i++) {
            async_queue.push(i);
        }
    });

    producer.join();
    EXPECT_EQ(async_queue.size(), num_items);

    int expected = 0;
    while (expected < num_items) {
        ASSERT_EQ(async_queue.size(), num_items - expected);
        ASSERT_EQ(async_queue.pop(), expected);
        expected++;
    }

#ifndef NDEBUG
    EXPECT_ANY_THROW(async_queue.pop());
#endif
    EXPECT_EQ(async_queue.size(), 0);
}

// Multiple producers and the consumer waits for producers to finish
TEST(AsyncQueueTest, MultithreadPush)
{
    const int num_producers = 4;
    const int items_per_thread = 10000;
    const int num_items = num_producers * items_per_thread;

    AsyncQueue<int> async_queue;
    std::vector<std::thread> producers;
    for (int thread = 0; thread < num_producers; thread++) {
        producers.emplace_back([&async_queue, thread]() {
            for (int i = 0; i < items_per_thread; ++i) {
                async_queue.push((thread * items_per_thread) + i);
            }
        });
    }

    for (int i = 0; i < num_producers; i++) {
        producers[i].join();
    }

    EXPECT_EQ(async_queue.size(), num_items);

    std::vector<int> producer_expected_value(num_producers, 0);
    for (int expected = 0; expected < num_items; expected++) {
        ASSERT_EQ(async_queue.size(), num_items - expected);
        int val = async_queue.pop();
        // Decode which producer pushed the value and its index there.
        int producer = val / items_per_thread;
        int producer_val = val % items_per_thread;
        ASSERT_EQ(producer_val, producer_expected_value[producer]);
        producer_expected_value[producer]++;
    }

#ifndef NDEBUG
    EXPECT_ANY_THROW(async_queue.pop());
#endif
    EXPECT_EQ(async_queue.size(), 0);

    for (int i = 0; i < num_producers; i++) {
        EXPECT_EQ(producer_expected_value[i], items_per_thread);
    }
}

// Multiple producers and the consumer pops data in parallel
TEST(AsyncQueueTest, MultithreadPushAsync)
{
    const int num_producers = 4;
    const int items_per_thread = 10000;
    const int num_items = num_producers * items_per_thread;

    AsyncQueue<int> async_queue;
    std::vector<std::thread> producers;
    for (int thread = 0; thread < num_producers; thread++) {
        producers.emplace_back([&async_queue, thread]() {
            for (int i = 0; i < items_per_thread; ++i) {
                async_queue.push((thread * items_per_thread) + i);
            }
        });
    }

    std::vector<int> producer_expected_value(num_producers, 0);
    for (int expected = 0; expected < num_items; expected++) {
        while (async_queue.size() == 0) {}
        int val = async_queue.pop();
        int producer = val / items_per_thread;
        int producer_val = val % items_per_thread;
        EXPECT_EQ(producer_val, producer_expected_value[producer]);
        producer_expected_value[producer]++;
    }

    for (int i = 0; i < num_producers; i++) {
        producers[i].join();
    }

#ifndef NDEBUG
    EXPECT_ANY_THROW(async_queue.pop());
#endif
    EXPECT_EQ(async_queue.size(), 0);

    for (int i = 0; i < num_producers; i++) {
        EXPECT_EQ(producer_expected_value[i], items_per_thread);
    }
}
