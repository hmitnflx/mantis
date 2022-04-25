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
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.core.*;
import org.apache.beam.runners.core.construction.ParDoTranslation;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnInvoker;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import rx.Observable;
import rx.Subscription;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
public class ParDoScalarStage<T, R> extends BeamScalarComputation<byte[], byte[]> {

  private final DoFnRunner<T, R> doFnRunner;
  private final Coder inputCoder;
  private final MantisParDoOutputManager pardoOut;
    private final DoFnInvoker<T, R> doFnInvoker;

    public ParDoScalarStage(AppliedPTransform<?, ?, ?> appliedTransform) {
    super(appliedTransform);
    final TupleTag<R> mainOutputTag;
    try {
      mainOutputTag = (TupleTag<R>) ParDoTranslation.getMainOutputTag(appliedTransform);
    } catch (IOException e) {
      log.warn("failed to create mainOutputTag ", e);
      throw new RuntimeException(e);
    }
    Map<TupleTag<?>, PCollection<?>> outputs = Utils.getOutputs(appliedTransform);
    List<TupleTag<?>> otherOutputTags =
            Objects.requireNonNull(outputs).keySet().stream()
                    .filter(ks -> !ks.equals(mainOutputTag))
                    .collect(Collectors.toList());

    PCollection input = (PCollection) Utils.getInput(appliedTransform);
    // Get the coder corresponding to the WindowedValue
    inputCoder = Utils.getCoder(input);
    Map<TupleTag<?>, Coder<?>> outputValueCoders = outputs.entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey, e -> Utils.getWindowedValueCoder(e.getValue())));

    pardoOut = new MantisParDoOutputManager(stringifyTransform(), outputValueCoders);
    DoFn<T, R> doFn = (DoFn<T, R>) Utils.getDoFn(appliedTransform);
    this.doFnRunner = DoFnRunners.simpleRunner(
            appliedTransform.getPipeline().getOptions(),
            doFn,
            EmptySideInputReader.INSTANCE,
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
    // Need a way to close this out here!
    this.doFnInvoker = DoFnInvokers.tryInvokeSetupFor(doFn, appliedTransform.getPipeline().getOptions());
  }

  @Override
  public Observable<byte[]> call(Context context, Observable<byte[]> tObservable) {
    pardoOut.clear();
    // consider using lift here instead to make the implementation simpler
    Subscription sub = tObservable.doOnNext(bytes -> {
      doFnRunner.startBundle();
      WindowedValue<T> objectWindowedValue = Utils.decodeWindowedValue(bytes, inputCoder);
      doFnRunner.processElement(objectWindowedValue);
      doFnRunner.finishBundle();
    }).subscribeOn(Schedulers.computation()).subscribe();

    return pardoOut.getSubject().doOnSubscribe(() ->
                    log.info("subscription received for {}", getAppliedTransform().getFullName()))
            .doOnUnsubscribe(() ->
                    log.info("unsubscribe received for {}", getAppliedTransform().getFullName()));
  }

  private static class NotImplementedStepContext implements StepContext {
    @Override
    public StateInternals stateInternals() {
      throw new UnsupportedOperationException("Unimplemented state internals for stateless ParDo");
    }

    @Override
    public TimerInternals timerInternals() {
      throw new UnsupportedOperationException("Unimplemented timer internals for stateless ParDo");
    }
  }

}
