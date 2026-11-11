/*
 * Copyright (c) 2026 ARCANA Research Group (University of Illinois
 * Urbana-Champaign)
 * All rights reserved
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

#ifndef __BASE_THREAD_HH__
#define __BASE_THREAD_HH__

#include <pthread.h>

#include <csignal>

#include "base/logging.hh"

namespace gem5
{

/**
 * Block signal `signo` on the calling thread. Children thread inherit the
 * blocked signal.
 */
inline void
blockSignal(int signo)
{
    sigset_t block_mask;
    sigemptyset(&block_mask);
    sigaddset(&block_mask, signo);
    int err = pthread_sigmask(SIG_BLOCK, &block_mask, nullptr);
    panic_if(err != 0, "Failed to block signal %d: %d", signo, err);
}

/**
 * Unblock signal `signo` on the calling thread. Does not unblock existing
 * children threads.
 */
inline void
unblockSignal(int signo)
{
    sigset_t unblock_mask;
    sigemptyset(&unblock_mask);
    sigaddset(&unblock_mask, signo);
    int err = pthread_sigmask(SIG_UNBLOCK, &unblock_mask, nullptr);
    panic_if(err != 0, "Failed to unblock signal %d: %d", signo, err);
}

} // namespace gem5

#endif // __BASE_THREAD_HH__
