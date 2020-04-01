/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.tranaction;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.core.transaction.TransactionAction;
import org.apache.carbondata.core.transaction.TransactionHandler;

import org.apache.spark.sql.SparkSession;

public class SessionTransactionManager implements TransactionHandler<SparkSession> {

  private Map<String, SparkSession> transactionIdToSessionMap;

  private Map<SparkSession, String> sessionToTransactionIdMap;

  private Map<String, Queue<TransactionAction>> openTransactionActions;

  private Map<String, Queue<TransactionAction>> closedTransactionActions;

  public SessionTransactionManager() {
    this.transactionIdToSessionMap = new ConcurrentHashMap<>();
    this.sessionToTransactionIdMap = new ConcurrentHashMap<>();
    this.openTransactionActions = new ConcurrentHashMap<>();
    this.closedTransactionActions = new ConcurrentHashMap<>();
  }

  @Override
  public String startTransaction(SparkSession sparkSession) {
    String transactionId = sessionToTransactionIdMap.get(sparkSession);
    if (null != transactionId) {
      throw new RuntimeException(
          "Single transaction is supported on Session." + " Commit old transaction first.");
    }
    transactionId = UUID.randomUUID().toString();
    transactionIdToSessionMap.put(transactionId, sparkSession);
    sessionToTransactionIdMap.put(sparkSession, transactionId);
    return transactionId;
  }

  @Override
  public void commitTransaction(String transactionId) {
    if (null == this.transactionIdToSessionMap.get(transactionId)) {
      throw new RuntimeException("No current transaction is running on this session");
    }
    Queue<TransactionAction> openTransactions = openTransactionActions.get(transactionId);
    Queue<TransactionAction> closeTransactions = closedTransactionActions.get(transactionId);
    if (null == closeTransactions) {
      closeTransactions = new ArrayDeque<>();
      closedTransactionActions.put(transactionId, closeTransactions);
    }
    while (!openTransactions.isEmpty()) {
      TransactionAction poll = openTransactions.poll();
      try {
        poll.commit();
        closeTransactions.add(poll);
      } catch (Exception e) {
        closeTransactions.add(poll);
        throw new RuntimeException(e);
      }
    }
    openTransactionActions.remove(transactionId);
    closedTransactionActions.remove(transactionId);
    sessionToTransactionIdMap.remove(transactionIdToSessionMap.remove(transactionId));
  }

  @Override
  public void rollbackTransaction(String transactionId) {
    if (null == this.transactionIdToSessionMap.get(transactionId)) {
      throw new RuntimeException("No current transaction is running on this session");
    }
    Queue<TransactionAction> openTransactions = openTransactionActions.get(transactionId);
    Queue<TransactionAction> closeTransactions = closedTransactionActions.get(transactionId);
    while (!closeTransactions.isEmpty()) {
      try {
        closeTransactions.poll().rollback();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    while (!openTransactions.isEmpty()) {
      try {
        openTransactions.poll().rollback();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    openTransactionActions.remove(transactionId);
    closedTransactionActions.remove(transactionId);
    sessionToTransactionIdMap.remove(transactionIdToSessionMap.remove(transactionId));
  }

  @Override
  public void recordTransactionAction(String transactionId, TransactionAction transactionAction) {
    if (null == this.transactionIdToSessionMap.get(transactionId)) {
      throw new RuntimeException("No current transaction is running on this session");
    }
    Queue<TransactionAction> transactionActions = openTransactionActions.get(transactionId);
    if (null == transactionActions) {
      transactionActions = new ArrayDeque<>();
    }
    transactionActions.add(transactionAction);
    openTransactionActions.put(transactionId, transactionActions);
  }

  public boolean isTransactionEnabled(SparkSession session) {
    return sessionToTransactionIdMap.containsKey(session);
  }

  public String getTransactionId(SparkSession session) {
    return sessionToTransactionIdMap.get(session);
  }

  @Override
  public TransactionHandler getTransactionManager() {
    return this;
  }
}
