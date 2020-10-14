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

import io.atomix.cluster.MemberId;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import org.slf4j.LoggerFactory;

public class RaftOperation {

  final BiConsumer<ControllableRaftContexts, MemberId> operation;
  private final String name;

  RaftOperation(final String name, final BiConsumer<ControllableRaftContexts, MemberId> operation) {
    this.name = name;
    this.operation = operation;
  }

  public static RaftOperation of(
      final String name, final BiConsumer<ControllableRaftContexts, MemberId> operation) {
    return new RaftOperation(name, operation);
  }

  public void run(
      final ControllableRaftContexts controllableRaftContexts, final MemberId memberId) {
    LoggerFactory.getLogger("TEST").info("Running {} on {}", name, memberId);
    operation.accept(controllableRaftContexts, memberId);
  }

  @Override
  public String toString() {
    return name;
  }

  public static List<RaftOperation> getDefaultRaftOperations(final List<MemberId> serverIds) {
    final List<RaftOperation> defaultRaftOperation = new ArrayList<>();
    defaultRaftOperation.add(
        RaftOperation.of("runNextTask", (raftRule, memberId) -> raftRule.runNextTask(memberId)));
    defaultRaftOperation.add(
        RaftOperation.of(
            "processAllMessage", (raftRule, memberId) -> raftRule.processAllMessage(memberId)));
    defaultRaftOperation.add(
        RaftOperation.of(
            "processNextMessage", (raftRule, memberId) -> raftRule.processNextMessage(memberId)));
    defaultRaftOperation.add(
        RaftOperation.of(
            "tickElectionTimeout", (raftRule, memberId) -> raftRule.tickElectionTimeout(memberId)));
    defaultRaftOperation.add(
        RaftOperation.of(
            "tickHeartBeatTimeout",
            (raftRule, memberId) -> raftRule.tickHeartbeatTimeout(memberId)));
    defaultRaftOperation.add(
        RaftOperation.of("tick 50ms", (raftRule, m) -> raftRule.tick(m, Duration.ofMillis(50))));
    defaultRaftOperation.add(
        RaftOperation.of("clientAppendOnLeader", (raftRule, m) -> raftRule.clientAppendOnLeader()));

    serverIds.forEach(
        target ->
            defaultRaftOperation.add(
                RaftOperation.of(
                    "deliverAllMessage to " + target.id(),
                    (raftRule, m) -> raftRule.getServerProtocol(m).deliverAll(target))));
    serverIds.forEach(
        target ->
            defaultRaftOperation.add(
                RaftOperation.of(
                    "deliverNextMessage to " + target.id(),
                    (raftRule, m) -> raftRule.getServerProtocol(m).deliverNextMessage(target))));
    serverIds.forEach(
        target ->
            defaultRaftOperation.add(
                RaftOperation.of(
                    "dropNextMessage to " + target.id(),
                    (raftRule, m) -> raftRule.getServerProtocol(m).dropNextMessage(target))));
    return defaultRaftOperation;
  }
}
