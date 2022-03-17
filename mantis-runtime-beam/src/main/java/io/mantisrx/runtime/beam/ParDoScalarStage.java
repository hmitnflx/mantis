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
import io.mantisrx.runtime.computation.ScalarComputation;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.core.*;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.nullness.qual.Nullable;
import rx.Observable;
import rx.subjects.PublishSubject;

@Slf4j
public class ParDoScalarStage<T, R> implements ScalarComputation<byte[], byte[]> {
  private static final SideInputReader emptySideInputReader = new SideInputReader() {
    @Override
    public <T> @Nullable T get(PCollectionView<T> view, BoundedWindow window) {
      return null;
    }

    @Override
    public <T> boolean contains(PCollectionView<T> view) {
      return false;
    }

    @Override
    public boolean isEmpty() {
      return false;
    }
  };

  private final AppliedPTransform<?, ?, ?> appliedTransform;
  private final DoFnRunner<T, R> doFnRunner;
  private final Coder inputCoder;
  private final MantisParDoOutputManager pardoOut;

  public ParDoScalarStage(AppliedPTransform<?, ?, ?> appliedTransform) {
    this.appliedTransform = appliedTransform;
    final TupleTag<R> mainOutputTag;
    try {
      mainOutputTag = (TupleTag<R>) ParDoTranslation.getMainOutputTag(appliedTransform);
    } catch (IOException e) {
      log.warn("failed to create mainOutputTag ", e);
      throw new RuntimeException(e);
    }
    List<TupleTag<?>> otherOutputTags =
        Objects.requireNonNull(Utils.getOutputs(appliedTransform)).keySet().stream()
            .filter(ks -> !ks.equals(mainOutputTag))
            .collect(Collectors.toList());

    PCollection input = (PCollection) Utils.getInput(appliedTransform);
    // Get the coder corresponding to the WindowedValue
    inputCoder = Utils.getCoder(input);
    Map<TupleTag<?>, Coder<?>> outputValueCoders = Utils.getOutputValueCoders(appliedTransform);
    pardoOut = new MantisParDoOutputManager(outputValueCoders);
    this.doFnRunner = DoFnRunners.simpleRunner(
            appliedTransform.getPipeline().getOptions(),
            (DoFn<T, R>) Utils.getDoFn(appliedTransform),
            emptySideInputReader,
            pardoOut,
            mainOutputTag,
            otherOutputTags,
            new NotImplementedStepContext(),
            // We generally pass around boxed values (inside WindowedValue)
            // and we need to pass the coder for that inner type to SimpleRunner
            // as we have already unboxed the value
            input.getCoder(),
            Utils.getOutputValueCoders(appliedTransform),
            ((PCollection<?>) input).getWindowingStrategy(),
            ParDoTranslation.getSchemaInformation(appliedTransform),
            ParDoTranslation.getSideInputMapping(appliedTransform));
  }

  @Override
  public Observable<byte[]> call(Context context, Observable<byte[]> tObservable) {
    tObservable.forEach(bytes -> {
      doFnRunner.startBundle();
      WindowedValue<T> objectWindowedValue = Utils.decodeWindowedValue(bytes, inputCoder);
      doFnRunner.processElement(objectWindowedValue);
      doFnRunner.finishBundle();
    });

    return pardoOut.asObservable();
  }

  private class NotImplementedStepContext implements StepContext {
    @Override
    public StateInternals stateInternals() {
      throw new UnsupportedOperationException("Unimplemented state internals for stateless ParDo");
    }

    @Override
    public TimerInternals timerInternals() {
      throw new UnsupportedOperationException("Unimplemented timer internals for stateless ParDo");
    }
  }

  private static class MantisParDoOutputManager implements DoFnRunners.OutputManager {
    private final Map<TupleTag<?>, Coder<?>> outputCoders;
    private final PublishSubject<byte[]> subject;

    MantisParDoOutputManager(Map<TupleTag<?>, Coder<?>> outputCoders) {
      this.outputCoders = outputCoders;
      this.subject = PublishSubject.create();
    }

    Observable<byte[]> asObservable() {
      return subject.asObservable();
    }

    @Override
    public <R> void output(TupleTag<R> tag, WindowedValue<R> output) {
      Coder coder = outputCoders.get(tag);
      byte[] encode = Utils.encode(output, coder);
      subject.onNext(encode);
    }
  }
}
