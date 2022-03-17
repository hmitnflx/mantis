/*
 * Copyright 2022 Netflix, Inc.
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

package io.mantisrx.runtime.beam;

import java.util.Collections;
import org.apache.beam.sdk.metrics.*;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * Alternative implementation of {@link MantisPipelineResult} used to avoid throwing Exceptions in
 * certain situations.
 */
public class FailedRunningPipelineResults extends MantisPipelineResult {

  private final RuntimeException cause;

  public FailedRunningPipelineResults(RuntimeException cause) {
    this.cause = cause;
  }

  public RuntimeException getCause() {
    return cause;
  }

  @Override
  public State getState() {
    return State.DONE;
  }

  @Override
  public State cancel() {
    return State.DONE;
  }

  @Override
  public State waitUntilFinish(Duration duration) {
    return State.DONE;
  }

  @Override
  public State waitUntilFinish() {
    return State.DONE;
  }

  @Override
  public MetricResults metrics() {
    return new MetricResults() {
      @Override
      public MetricQueryResults queryMetrics(@Nullable MetricsFilter filter) {
        return new EmptyMetricQueryResults();
      }
    };
  }

  public static class EmptyMetricQueryResults extends MetricQueryResults {
    @Override
    public Iterable<MetricResult<Long>> getCounters() {
      return Collections.emptyList();
    }

    @Override
    public Iterable<MetricResult<DistributionResult>> getDistributions() {
      return Collections.emptyList();
    }

    @Override
    public Iterable<MetricResult<GaugeResult>> getGauges() {
      return Collections.emptyList();
    }
  }
}
