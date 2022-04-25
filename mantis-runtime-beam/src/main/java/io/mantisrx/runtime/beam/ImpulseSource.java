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

import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.source.Index;
import io.mantisrx.runtime.source.Source;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.WindowedValue;
import rx.Observable;

public class ImpulseSource implements Source<byte[]> {
  private final Coder outputCoder;

  public ImpulseSource(Coder outputCoder) {
    this.outputCoder = outputCoder;
  }

  @Override
  public Observable<Observable<byte[]>> call(Context context, Index index) {
    byte[] value = Utils.encode(WindowedValue.valueInGlobalWindow(new byte[0]), outputCoder);
    return Observable.just(Observable.just(value));
  }

    @Override
    public void close() {
    }
}
