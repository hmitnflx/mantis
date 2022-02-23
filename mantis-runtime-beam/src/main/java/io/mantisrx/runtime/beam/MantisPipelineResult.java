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

import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricResults;
import org.joda.time.Duration;

import java.io.IOException;

public class MantisPipelineResult implements PipelineResult {
  @Override
  public State getState() {
    return null;
  }

  @Override
  public State cancel() throws IOException {
    return null;
  }

  @Override
  public State waitUntilFinish(Duration duration) {
    return null;
  }

  @Override
  public State waitUntilFinish() {
    return null;
  }

  @Override
  public MetricResults metrics() {
    return null;
  }
}