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

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.core.DoFnRunners;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import rx.subjects.PublishSubject;

import java.util.Map;

@Slf4j
class MantisParDoOutputManager implements DoFnRunners.OutputManager {
    private final Map<TupleTag<?>, Coder<?>> outputCoders;
    private PublishSubject<byte[]> subject;
    private final String transformName;

    MantisParDoOutputManager(String transformName, Map<TupleTag<?>, Coder<?>> outputCoders) {
        this.transformName = transformName;
        this.outputCoders = outputCoders;
    }

    public PublishSubject<byte[]> getSubject() {
        return subject;
    }

    @Override
    public <R> void output(TupleTag<R> tag, WindowedValue<R> output) {
        Coder coder = outputCoders.get(tag);
        log.warn("hmittal: output record {}", output);
        byte[] encode = Utils.encode(output, coder);
        subject.onNext(encode);
    }

    public void clear() {
        subject = PublishSubject.create();
    }
}
