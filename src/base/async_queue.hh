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

#ifndef __BASE_ASYNC_QUEUE_HH__
#define __BASE_ASYNC_QUEUE_HH__

#include <atomic>
#include <cstddef>
#include <utility>

#include "base/logging.hh"

namespace gem5
{

/**
 * This is a multi producer single consumer asynchronous queue that assumes
 * that the single consumer remains consistent throughout the simulation.
 *
 * Based on https://int08h.com/post/ode-to-a-vyukov-queue/#fn:5
 */
template <typename T> class AsyncQueue
{
  private:
    // Singly linked list node
    struct Node
    {
        T data{};
        std::atomic<Node *> next{nullptr};

        Node() = default;
        Node(T _data) : data(std::move(_data)) {}
    };

    // Head is an old node whose data has been popped, only modified on
    // consumer
    alignas(64) std::atomic<Node *> head;
    // Producers insert to tail
    alignas(64) std::atomic<Node *> tail;
    // Number of items in the list
    std::atomic<size_t> length;

  public:
    // Set the head and tail to empty node
    AsyncQueue() : length(0)
    {
        Node *empty = new Node();
        head.store(empty, std::memory_order_relaxed);
        tail.store(empty, std::memory_order_relaxed);
    }

    // Frees the nodes in the list
    ~AsyncQueue()
    {
        Node *node = head.load(std::memory_order_relaxed);
        while (node != nullptr) {
            Node *next = node->next.load(std::memory_order_relaxed);
            delete node;
            node = next;
        }
    }

    // Prevent the queue from being copied
    AsyncQueue(const AsyncQueue &) = delete;
    AsyncQueue &operator=(const AsyncQueue &) = delete;

    // Appends an item in a thread safe manner
    void
    push(T data)
    {
        Node *new_node = new Node(std::move(data));
        Node *prev_tail = tail.exchange(new_node, std::memory_order_acq_rel);

        // The new node is "visible" after this as the head stores stale data
        prev_tail->next.store(new_node, std::memory_order_release);

        // Increases the size and notifies the waiting thread in pop()
        length.fetch_add(1, std::memory_order_release);
        length.notify_one();
    }

    // Remove the first item of the list
    T
    pop()
    {
        // The queue must have at least one item in it
        size_t current_size = size();
        gem5_assert(current_size != 0, "pop() called on an empty AsyncQueue");

        // Safely load the head (this is not necesary but is helpful)
        Node *old_head = head.load(std::memory_order_relaxed);
        Node *new_head;

        // It is possible that a push() is pending and so wait on it
        // Example:
        // Thread A : Has swapped the tail and now is waiting to swap the next
        // Thread B : Completes push() and increments length
        // pop() knows that at least one element is in the list but the next
        // pointer is null as thread A is stuck. So we wait here to make sure
        // pop() returns an element.
        while ((new_head = old_head->next.load(std::memory_order_acquire)) ==
               nullptr) {
            length.wait(current_size, std::memory_order_acquire);
            current_size = size();
        }

        length.fetch_sub(1, std::memory_order_relaxed);
        // Move the head and delete the old head to prevent memory leak
        head.store(new_head, std::memory_order_relaxed);
        delete old_head;

        return std::move(new_head->data);
    }

    // Number of items in the list
    size_t
    size() const noexcept
    {
        return length.load(std::memory_order_acquire);
    }
};

} // namespace gem5

#endif // __BASE_ASYNC_QUEUE_HH__
