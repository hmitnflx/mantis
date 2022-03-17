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

import io.mantisrx.common.MantisGroup;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.GroupToScalar;
import io.mantisrx.runtime.ScalarToGroup;
import io.mantisrx.runtime.ScalarToScalar;
import io.mantisrx.runtime.computation.GroupToScalarComputation;
import io.mantisrx.runtime.computation.ScalarComputation;
import io.mantisrx.runtime.computation.ToGroupComputation;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.repackaged.direct_java.runners.core.InMemoryTimerInternals;
import org.apache.beam.runners.core.LateDataUtils;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.*;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.joda.time.Instant;
import rx.Observable;

public class MantisTransformTranslators {

  public static class ReadSourceTranslator<T>
      implements IMantisTransformTranslator<PTransform<PBegin, PCollection<T>>> {

    @Override
    public MantisJobBuilder translate(
        Pipeline pipeline,
        AppliedPTransform<?, ?, ?> appliedTransform,
        TransformHierarchy.Node node,
        MantisTranslationContext context) {
      Map.Entry<TupleTag<?>, PCollection<?>> output = Utils.getOutput(appliedTransform);

      SerializablePipelineOptions pipelineOptions = context.getOptions();
      AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>> readTransform =
          (AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>>)
              appliedTransform;
      return context
          .getMantisJobBuilder()
          .addStage(new BeamSource<>(readTransform, pipelineOptions));
    }
  }

  public static class CreateViewTranslator<T>
      implements IMantisTransformTranslator<PTransform<PCollection<T>, PCollection<T>>> {
    @Override
    public MantisJobBuilder translate(
        Pipeline pipeline,
        AppliedPTransform<?, ?, ?> appliedTransform,
        TransformHierarchy.Node node,
        MantisTranslationContext context) {

      // This stuff should be done inside the relevant ParDoRunner
      // using SideInputReader mechanism actually.
      return context.getMantisJobBuilder();
    }
  }

  public static class GroupByKeyTranslator<K, V>
      implements IMantisTransformTranslator<
                PTransform<PCollection<KV<K, V>>, PCollection<KV<K, Iterable<V>>>>> {

    @Override
    public MantisJobBuilder translate(
        Pipeline pipeline,
        AppliedPTransform<?, ?, ?> appliedTransform,
        TransformHierarchy.Node node,
        MantisTranslationContext context) {
      PCollection<KV<K, V>> input = (PCollection<KV<K, V>>) Utils.getInput(appliedTransform);
        WindowedValue.WindowedValueCoder<KV<K, V>> inputCoder = Utils.getWindowedValueCoder(input);
        KvCoder<K, V> kvValueCoder = (KvCoder<K, V>) inputCoder.getValueCoder();
        Map.Entry<TupleTag<?>, PCollection<?>> output = Utils.getOutput(appliedTransform);
      Coder<WindowedValue<KV<K, Iterable<V>>>> outputCoder =
              Utils.getCoder((PCollection) Utils.getOutput(appliedTransform).getValue());

      MantisJobBuilder jobBuilder = context.getMantisJobBuilder()
              .addStage((ToGroupComputation<byte[], byte[], byte[]>) (context1, obs) ->
                      obs.map(item -> {
                        WindowedValue<KV<K, V>> windowedkv =
                                Utils.decodeWindowedValue(item, inputCoder);
                        KV<K, V> kv = windowedkv.getValue();
                        //todo(hmittal): drop late arriving data here
                        //   also, expired windows here to avoid extra data transmission!
                        return new MantisGroup<>(Utils.encode(kv.getKey(), kvValueCoder.getKeyCoder()), item);
                      }), new ScalarToGroup.Config<byte[], byte[], byte[]>()
                      .description(String.format("group stage for beam group-by-key %s", appliedTransform.getFullName()))
                      .codec(MantisByteCodec.INSTANCE))
              .addStage((GroupToScalarComputation<byte[], byte[], byte[]>) (ctx, obs) -> {
                //todo(hmittal): do things here using ReduceFnRunner
                return null;
              }, new GroupToScalar.Config<byte[], byte[], byte[]>()
                      .description(String.format("scalar stage for beam group-by-key %s", appliedTransform.getFullName()))
                      .codec(MantisByteCodec.INSTANCE));
      return jobBuilder;
    }
  }

  public static class ParDoTranslatorI<T>
      implements IMantisTransformTranslator<PTransform<PCollection<T>, PCollectionTuple>> {
    @Override
    public MantisJobBuilder translate(
        Pipeline pipeline,
        AppliedPTransform<?, ?, ?> appliedTransform,
        TransformHierarchy.Node node,
        MantisTranslationContext context) {
      context.getMantisJobBuilder().addStage(new ParDoScalarStage(appliedTransform), new ScalarToScalar.Config());
      return context.getMantisJobBuilder();
    }
  }

  public static class FlattenTranslator<T>
      implements IMantisTransformTranslator<PTransform<PCollectionList<T>, PCollection<T>>> {
    @Override
    public MantisJobBuilder translate(
        Pipeline pipeline,
        AppliedPTransform<?, ?, ?> appliedTransform,
        TransformHierarchy.Node node,
        MantisTranslationContext context) {
      Map<String, Coder> inputCoders =
              Utils.getCoders(
                      Utils.getInputs(appliedTransform), e -> Utils.getTupleTagId(e.getValue()));
      Map.Entry<TupleTag<?>, PCollection<?>> output = Utils.getOutput(appliedTransform);
      Coder outputCoder = Utils.getCoder(output.getValue());
      return context.getMantisJobBuilder().addStage(
              (ScalarComputation<byte[], byte[]>) (context1, observable) -> observable.map(item -> {
                WindowedValue<Object> windowedValue = Utils.decodeWindowedValue(item, inputCoders.values().stream().findFirst().get());
                return Utils.encode(windowedValue, outputCoder);
              }), new ScalarToScalar.Config<>());
    }
  }

  public static class ImpulseTranslator
      implements IMantisTransformTranslator<PTransform<PBegin, PCollection<byte[]>>> {

    @Override
    public MantisJobBuilder translate(
        Pipeline pipeline,
        AppliedPTransform<?, ?, ?> appliedTransform,
        TransformHierarchy.Node node,
        MantisTranslationContext context) {

      Coder outputCoder =
          Utils.getCoder((PCollection) Utils.getOutput(appliedTransform).getValue());
      MantisJobBuilder job = context.getMantisJobBuilder();
      job.addStage(new ImpulseSource(outputCoder));
      return job;
    }
  }

  public static class WindowTranslator<T>
      implements IMantisTransformTranslator<PTransform<PCollection<T>, PCollection<T>>> {
    @Override
    public MantisJobBuilder translate(
        Pipeline pipeline,
        AppliedPTransform<?, ?, ?> appliedTransform,
        TransformHierarchy.Node node,
        MantisTranslationContext context) {
      WindowingStrategy<T, BoundedWindow> windowingStrategy =
          (WindowingStrategy<T, BoundedWindow>)
              ((PCollection) Utils.getOutput(appliedTransform).getValue()).getWindowingStrategy();
      PCollection<WindowedValue> input =
          (PCollection<WindowedValue>) Utils.getInput(appliedTransform);
      Coder inputCoder = Utils.getCoder(input);
      Coder outputCoder =
          Utils.getCoder((PCollection) Utils.getOutput(appliedTransform).getValue());

      MantisJobBuilder job = context.getMantisJobBuilder()
              .addStage(new ScalarComputation<byte[], byte[]>() {
                  private WindowAssignCtx windowsAssignCtx;

                  @Override
                  public void init(Context context) {
                     windowsAssignCtx = new WindowAssignCtx(windowingStrategy.getWindowFn());
                  }

                  @Override
                  public Observable<byte[]> call(Context context, Observable<byte[]> observable) {
                      return observable.map(bytes -> {
                          WindowedValue<T> item = Utils.decodeWindowedValue(bytes, inputCoder);
                          try {
                              windowsAssignCtx.setValue(item);
                              Collection<BoundedWindow> windows = windowingStrategy.getWindowFn().assignWindows(windowsAssignCtx);
                              WindowedValue<T> output = WindowedValue.of(
                                      item.getValue(),
                                      item.getTimestamp(),
                                      windows,
                                      item.getPane()
                              );
                              return Utils.encode(output, outputCoder);
                          } catch (Exception e) {
                              throw new RuntimeException(e);
                          }
                      });
                  }
              }, new ScalarToScalar.Config());

      return job;
    }

    private Collection<? extends BoundedWindow> dropLateWindows(Collection<? extends BoundedWindow> windows, WindowingStrategy<T, BoundedWindow> windowingStrategy) {
      return windows.stream()
              .filter(w -> !isExpiredWindow(w, windowingStrategy))
              .collect(Collectors.toList());
    }

    private boolean isExpiredWindow(BoundedWindow w, WindowingStrategy<T, BoundedWindow> windowingStrategy) {
      //todo(hmittal): use the correct value here!
      InMemoryTimerInternals timerInternals = new InMemoryTimerInternals();
      Instant inputWM = timerInternals.currentInputWatermarkTime();
      return LateDataUtils.garbageCollectionTime(w, windowingStrategy).isBefore(inputWM);
    }

      private class WindowAssignCtx extends WindowFn<T, BoundedWindow>.AssignContext {
          private WindowedValue<T> value;

          public WindowAssignCtx(WindowFn<T, BoundedWindow> fn) {
              fn.super();
          }

          private void setValue(WindowedValue<T> value) {
              if (Iterables.size(value.getWindows()) != 1) {
                  throw new IllegalArgumentException(
                          String.format(
                                  "%s passed to window assignment must be in a single window, but it was in %s: %s",
                                  WindowedValue.class.getSimpleName(),
                                  Iterables.size(value.getWindows()),
                                  value.getWindows()));
              }
              this.value = value;
          }

          @Override
          public T element() {
              return value.getValue();
          }

          @Override
          public Instant timestamp() {
              return value.getTimestamp();
          }

          @Override
          public BoundedWindow window() {
              return Iterables.getOnlyElement(value.getWindows());
          }
      }
  }
}
