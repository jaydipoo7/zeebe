/*
 * Copyright © 2020 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.raft;

import io.atomix.utils.concurrent.Scheduled;
import io.atomix.utils.concurrent.ScheduledFutureImpl;
import io.atomix.utils.concurrent.ThreadContext;
import java.time.Duration;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.jmock.lib.concurrent.DeterministicScheduler;

public class DeterministicSingleThreadContext implements ThreadContext {

  private final DeterministicScheduler deterministicScheduler;
  private final Consumer<Throwable> uncaughtExceptionObserver;

  public DeterministicSingleThreadContext(
      final DeterministicScheduler executor, final Consumer<Throwable> uncaughtExceptionObserver) {
    deterministicScheduler = executor;
    this.uncaughtExceptionObserver = uncaughtExceptionObserver;
  }

  public DeterministicScheduler getDeterministicScheduler() {
    return deterministicScheduler;
  }

  public static ThreadContext createContext(
      final ThreadFactory factory, final Consumer<Throwable> unCaughtExceptionHandler) {
    return new DeterministicSingleThreadContext(
        new DeterministicScheduler(), unCaughtExceptionHandler);
  }

  @Override
  public Scheduled schedule(final long delay, final TimeUnit timeUnit, final Runnable callback) {
    final var future = deterministicScheduler.schedule(callback, delay, timeUnit);
    return new ScheduledFutureImpl<>(future);
  }

  @Override
  public Scheduled schedule(final Duration delay, final Runnable callback) {
    final var future =
        deterministicScheduler.schedule(callback, delay.toMillis(), TimeUnit.MILLISECONDS);
    return new ScheduledFutureImpl<>(future);
  }

  @Override
  public Scheduled schedule(
      final long initialDelay,
      final long interval,
      final TimeUnit timeUnit,
      final Runnable callback) {
    final ScheduledFuture<?> future =
        deterministicScheduler.scheduleAtFixedRate(callback, initialDelay, interval, timeUnit);
    return new ScheduledFutureImpl<>(future);
  }

  @Override
  public Scheduled schedule(
      final Duration initialDelay, final Duration interval, final Runnable callback) {
    final ScheduledFuture<?> future =
        deterministicScheduler.scheduleAtFixedRate(
            callback, initialDelay.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    return new ScheduledFutureImpl<>(future);
  }

  @Override
  public void execute(final Runnable command) {
    deterministicScheduler.execute(command);
  }

  @Override
  public boolean isCurrentContext() {
    return true;
  }

  @Override
  public void checkThread() {
    // always assume running on the right context
  }

  @Override
  public boolean isBlocked() {
    return false;
  }

  @Override
  public void block() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void unblock() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() {
    // deterministicScheduler.shutdown();
  }
}
