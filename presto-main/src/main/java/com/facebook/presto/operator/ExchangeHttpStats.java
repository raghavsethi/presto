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

package com.facebook.presto.operator;

import io.airlift.stats.CounterStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;

public class ExchangeHttpStats
{
    private final CounterStat requestsSucceeded;
    private final CounterStat requestsFailed;
    private final CounterStat requestsErrored;

    @Inject
    public ExchangeHttpStats()
    {
        requestsSucceeded = new CounterStat();
        requestsFailed = new CounterStat();
        requestsErrored = new CounterStat();
    }

    public void recordSuccess()
    {
        requestsSucceeded.update(1);
    }

    public void recordFailure()
    {
        requestsFailed.update(1);
    }

    public void recordError()
    {
        requestsErrored.update(1);
    }

    @Managed
    @Nested
    public CounterStat getRequestsSucceeded()
    {
        return requestsSucceeded;
    }

    @Managed
    @Nested
    public CounterStat getRequestsFailed()
    {
        return requestsFailed;
    }

    @Managed
    @Nested
    public CounterStat getRequestsErrored()
    {
        return requestsErrored;
    }
}
