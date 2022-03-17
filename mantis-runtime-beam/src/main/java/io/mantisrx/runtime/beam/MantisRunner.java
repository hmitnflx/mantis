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

import io.mantisrx.runtime.MantisJob;
import io.mantisrx.runtime.beam.api.MantisConfig;
import io.mantisrx.runtime.beam.api.MantisInstance;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import org.apache.beam.runners.core.SplittableParDoViaKeyedWorkItems;
import org.apache.beam.runners.core.construction.PTransformMatchers;
import org.apache.beam.runners.core.construction.SplittableParDo;
import org.apache.beam.runners.core.construction.SplittableParDoNaiveBounded;
import org.apache.beam.runners.core.construction.UnconsumedReads;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.transforms.PTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MantisRunner extends PipelineRunner<MantisPipelineResult> {
  private static final Logger LOG = LoggerFactory.getLogger(MantisRunner.class);

  /**
   * Needed because that's how apache-beam pipeline is instatiated via reflection when the runner is
   * specified.
   *
   * @param options
   * @return
   */
  public static MantisRunner fromOptions(PipelineOptions options) {
    return fromOptions(options, Mantis::newMantisClient, Mantis::translatorProvider);
  }

  public static MantisRunner fromOptions(
      PipelineOptions options,
      Function<MantisConfig, MantisInstance> mantisProvider,
      Function<PTransform<?, ?>, IMantisTransformTranslator<?>> translatorProvider) {
    return new MantisRunner(options, mantisProvider, translatorProvider);
  }

  private final MantisPipelineOptions options;
  private final Function<MantisConfig, MantisInstance> mantisProvider;
  private final Function<PTransform<?, ?>, IMantisTransformTranslator<?>> translatorProvider;

  private MantisRunner(
      PipelineOptions options,
      Function<MantisConfig, MantisInstance> mantisProvider,
      Function<PTransform<?, ?>, IMantisTransformTranslator<?>> translatorProvider) {
    this.options = validate(options.as(MantisPipelineOptions.class));
    this.mantisProvider = mantisProvider;
    this.translatorProvider = translatorProvider;
  }

  @Override
  public MantisPipelineResult run(Pipeline pipeline) {
    try {
      normalize(pipeline);
      MantisJob jobDag = translate(pipeline);
      return run(jobDag);
    } catch (UnsupportedOperationException uoe) {
      LOG.error("Failed running pipeline!", uoe);
      return new FailedRunningPipelineResults(uoe);
    }
  }

  private MantisPipelineResult run(MantisJob jobDag) {
    return null;
  }

  private MantisJob translate(Pipeline pipeline) {
    MantisGraphVisitor graphVisitor = new MantisGraphVisitor(options, translatorProvider);
    pipeline.traverseTopologically(graphVisitor);
    return graphVisitor.getJob();
  }

  private void normalize(Pipeline pipeline) {
    pipeline.replaceAll(getDefaultOverrides());
    UnconsumedReads.ensureAllReadsConsumed(pipeline);
  }

  private List<PTransformOverride> getDefaultOverrides() {
    return Arrays.asList(
        PTransformOverride.of(
            PTransformMatchers.splittableParDo(), new SplittableParDo.OverrideFactory<>()),
        PTransformOverride.of(
            PTransformMatchers.splittableProcessKeyedBounded(),
            new SplittableParDoNaiveBounded.OverrideFactory<>()),
        PTransformOverride.of(
            PTransformMatchers.splittableProcessKeyedUnbounded(),
            new SplittableParDoViaKeyedWorkItems.OverrideFactory<>()));
  }

  private static MantisPipelineOptions validate(MantisPipelineOptions options) {
    return options;
  }
}
