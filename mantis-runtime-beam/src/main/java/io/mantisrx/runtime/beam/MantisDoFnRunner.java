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

import org.apache.beam.runners.core.DoFnRunner;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;

public class MantisDoFnRunner<I, O> implements DoFnRunner<I, O> {
  @Override
  public void startBundle() {}

  @Override
  public void processElement(WindowedValue<I> elem) {}

  @Override
  public <KeyT> void onTimer(
      String timerId,
      String timerFamilyId,
      KeyT key,
      BoundedWindow window,
      Instant timestamp,
      Instant outputTimestamp,
      TimeDomain timeDomain) {}

  @Override
  public void finishBundle() {}

  @Override
  public <KeyT> void onWindowExpiration(BoundedWindow window, Instant timestamp, KeyT key) {}

  @Override
  public DoFn<I, O> getFn() {
    return null;
  }
}
