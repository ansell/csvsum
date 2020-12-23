/*
 * Copyright (c) 2016, Peter Ansell
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.github.ansell.csv.util;

import java.util.Queue;
import java.util.function.Consumer;

/**
 * A Java-8 Consumer based thread, using a sentinel object to signal the
 * consumer to complete.
 *
 * @author Peter Ansell p_ansell@yahoo.com
 * @param <T>
 *            The type of the objects being consumed
 */
public class ConsumerRunnable<T> implements Runnable, Consumer<T> {

    private final Queue<T> queue;
    private final Consumer<T> consumer;
    private final T sentinel;

    private ConsumerRunnable(Queue<T> queue, Consumer<T> consumer, T sentinel) {
        this.queue = queue;
        this.consumer = consumer;
        this.sentinel = sentinel;
    }

    public static <T> ConsumerRunnable<T> from(Queue<T> queue, Consumer<T> consumer, T sentinel) {
        return new ConsumerRunnable<>(queue, consumer, sentinel);
    }

    @Override
    public void run() {
        while (true) {
            try {
                if (Thread.currentThread().isInterrupted()) {
                    return;
                }

                final T take = queue.poll();

                if (take == null) {
                    Thread.sleep(100);
                    continue;
                }

                // If the item on the queue was the same object as the sentinel
                // then we return
                if (sentinel == take) {
                    return;
                }

                this.accept(take);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void accept(T take) {
        consumer.accept(take);
    }
}
