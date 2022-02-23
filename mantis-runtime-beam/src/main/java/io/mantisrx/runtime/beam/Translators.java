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

import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.*;
import sun.security.provider.certpath.Vertex;

import java.util.Map;

public class Translators {

  public static class ReadSourceTranslator<T>
      implements MantisTransformTranslator<PTransform<PBegin, PCollection<T>>> {

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
      implements MantisTransformTranslator<PTransform<PCollection<T>, PCollection<T>>> {
    @Override
    public MantisJobBuilder translate(
        Pipeline pipeline,
        AppliedPTransform<?, ?, ?> appliedTransform,
        TransformHierarchy.Node node,
        MantisTranslationContext context) {
      return null;
    }
  }

  public static class GroupByKeyTranslator<T, InputT>
      implements MantisTransformTranslator<
          PTransform<PCollection<KV<T, InputT>>, PCollection<KV<T, Iterable<InputT>>>>> {

    @Override
    public MantisJobBuilder translate(
        Pipeline pipeline,
        AppliedPTransform<?, ?, ?> appliedTransform,
        TransformHierarchy.Node node,
        MantisTranslationContext context) {

      return context.getMantisJobBuilder();
    }
  }

  public static class ParDoTranslator<T>
      implements MantisTransformTranslator<PTransform<PCollection<T>, PCollectionTuple>> {
    @Override
    public MantisJobBuilder translate(
        Pipeline pipeline,
        AppliedPTransform<?, ?, ?> appliedTransform,
        TransformHierarchy.Node node,
        MantisTranslationContext context) {
      return null;
    }
  }

  public static class FlattenTranslator<T>
      implements MantisTransformTranslator<PTransform<PCollectionList<T>, PCollection<T>>> {
    @Override
    public MantisJobBuilder translate(
        Pipeline pipeline,
        AppliedPTransform<?, ?, ?> appliedTransform,
        TransformHierarchy.Node node,
        MantisTranslationContext context) {
      return null;
    }
  }

  public static class ImpulseTranslator
      implements MantisTransformTranslator<PTransform<PBegin, PCollection<byte[]>>> {

    @Override
    public MantisJobBuilder translate(
        Pipeline pipeline,
        AppliedPTransform<?, ?, ?> appliedTransform,
        TransformHierarchy.Node node,
        MantisTranslationContext context) {

      Map.Entry<TupleTag<?>, PCollection<?>> output = Utils.getOutput(appliedTransform);
      Coder outputCoder =
          Utils.getCoder((PCollection) Utils.getOutput(appliedTransform).getValue());
      MantisJobBuilder job = context.getMantisJobBuilder();
      job.addStage(new ImpulseSource(outputCoder));
      return job;
    }
  }

  public static class WindowTranslator<T>
      implements MantisTransformTranslator<PTransform<PCollection<T>, PCollection<T>>> {
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
      Map.Entry<TupleTag<?>, PCollection<?>> output = Utils.getOutput(appliedTransform);
      Coder outputCoder =
          Utils.getCoder((PCollection) Utils.getOutput(appliedTransform).getValue());

      String transformName = appliedTransform.getFullName();
      MantisJobBuilder job = context.getMantisJobBuilder();

      Vertex vertex = job.addStage(new );
          dagBuilder.addVertex(
              vertexId,
              AssignWindowP.supplier(inputCoder, outputCoder, windowingStrategy, vertexId));
      return job;
    }
  }
}
