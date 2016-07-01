/*
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
package com.facebook.presto.execution;

import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.google.common.collect.ImmutableSet;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.Executor;

import static com.facebook.presto.execution.QueryStatisticsStateMachine.QueryStatisticsState.COLLECTING_FINAL;
import static com.facebook.presto.execution.QueryStatisticsStateMachine.QueryStatisticsState.CURRENT;
import static com.facebook.presto.execution.QueryStatisticsStateMachine.QueryStatisticsState.FINAL;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class QueryStatisticsStateMachine
{
    public enum QueryStatisticsState
    {
        CURRENT, COLLECTING_FINAL, FINAL
    }

    private final StateMachine<QueryStatisticsState> queryStatisticsState;

    public QueryStatisticsStateMachine(String queryId, Executor executor)
    {
        this.queryStatisticsState = new StateMachine<>("queryStatistics " + queryId, executor, CURRENT, ImmutableSet.of(QueryStatisticsState.FINAL));
    }

    public QueryStatisticsState getQueryStatisticsState()
    {
        return queryStatisticsState.get();
    }

    public boolean transitionToCollectingFinal()
    {
        return queryStatisticsState.compareAndSet(CURRENT, COLLECTING_FINAL);
    }

    public boolean transitionToFinal()
    {
        return queryStatisticsState.compareAndSet(COLLECTING_FINAL, FINAL);
    }

    public void addStateChangeListener(StateChangeListener<QueryStatisticsState> stateChangeListener)
    {
        queryStatisticsState.addStateChangeListener(stateChangeListener);
    }
}
