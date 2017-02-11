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

import com.facebook.presto.client.FailureInfo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;
import javax.validation.constraints.NotNull;

import java.net.URI;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

@Immutable
public class TaskFailureInfo
{
    private final FailureInfo failureInfo;
    private final Optional<String> failureTask;
    private final Optional<URI>  failureTaskUri;

    @JsonCreator
    public TaskFailureInfo(
            @JsonProperty("cause") FailureInfo failureInfo,
            @JsonProperty("task") Optional<String> taskId,
            @JsonProperty("uri") Optional<URI> uri)
    {
        requireNonNull(uri, "uri is null");
        requireNonNull(taskId, "task is null");

        this.failureInfo = requireNonNull(failureInfo, "failureInfo is null");
        this.failureTask = requireNonNull(taskId, "task is null");
        this.failureTaskUri = requireNonNull(uri, "uri is null");
    }

    public TaskFailureInfo(FailureInfo failureInfo)
    {
        this(failureInfo, Optional.empty(), Optional.empty());
    }

    public TaskFailureInfo(FailureInfo failureInfo, String taskId, URI uri)
    {
        this(failureInfo, Optional.of(taskId), Optional.of(uri));
    }

    @NotNull
    @JsonProperty
    public FailureInfo getFailureInfo()
    {
        return failureInfo;
    }

    @NotNull
    @JsonProperty
    public Optional<String> getTask()
    {
        return failureTask;
    }

    @NotNull
    @JsonProperty
    public Optional<URI> getUri()
    {
        return failureTaskUri;
    }
}
