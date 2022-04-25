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

import io.mantisrx.runtime.Job;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PValue;

@Slf4j
public class MantisGraphVisitor extends Pipeline.PipelineVisitor.Defaults {

  private boolean finalized = false;
  private final PipelineOptions options;
  private final MantisTranslationContext translationContext;
  private final Function<PTransform<?, ?>, IMantisTransformTranslator<?>> translatorProvider;

  public MantisGraphVisitor(
      MantisPipelineOptions options,
      Function<PTransform<?, ?>, IMantisTransformTranslator<?>> translatorProvider) {
    this.options = options;
    this.translationContext = new MantisTranslationContext(options);
    this.translatorProvider = translatorProvider;
  }

  @Override
  public CompositeBehavior enterCompositeTransform(TransformHierarchy.Node node) {
    if (finalized) {
      throw new IllegalStateException("Attempting to traverse an already finalized pipeline!");
    }

    PTransform<?, ?> transform = node.getTransform();
    if (transform != null) {

      IMantisTransformTranslator<?> translator = translatorProvider.apply(transform);
      log.info("composite transform {}: enter, directMapComposite {}", transform.getName(), translator != null);
      if (translator != null) {
        translate(node, translator);
        return CompositeBehavior.DO_NOT_ENTER_TRANSFORM;
      }
    }
    return CompositeBehavior.ENTER_TRANSFORM;
  }

  @Override
  public void leaveCompositeTransform(TransformHierarchy.Node node) {
    if (finalized) {
      throw new IllegalStateException("Attempting to traverse an already finalized pipeline!");
    }
    if (node.getTransform() != null) {
      log.info("composite transform {}: leave", node.getTransform().getName());
    }
    if (node.isRootNode()) {
      finalized = true;
    }
  }

  @Override
  public void visitPrimitiveTransform(TransformHierarchy.Node node) {
    PTransform<?, ?> transform = node.getTransform();
    IMantisTransformTranslator<?> translator = translatorProvider.apply(transform);
    if (translator == null) {
      String transformUrn = PTransformTranslation.urnForTransform(transform);
      throw new UnsupportedOperationException(
          String.format("The transform %s is currently not supported.", transformUrn));
    }
    translate(node, translator);
  }

  private <T extends PTransform<?, ?>> void translate(
      TransformHierarchy.Node node, IMantisTransformTranslator<?> translator) {

    @SuppressWarnings("unchecked")
    IMantisTransformTranslator<T> typedTranslator = (IMantisTransformTranslator<T>) translator;
    Pipeline pipeline = getPipeline();
    AppliedPTransform<?, ?, ?> appliedTransform = node.toAppliedPTransform(pipeline);
    PTransform<?, ?> transform = node.getTransform();
    if (transform == null) {
      log.info("graph. node {} transform: null", node.getFullName());
    } else {
      log.info("graph. node {}", node.getFullName());
      log.info("graph. stringify {}", Utils.stringifyTransform(appliedTransform));
    }
    typedTranslator.translate(pipeline, appliedTransform, node, translationContext);
  }

  @Override
  public void visitValue(PValue value, TransformHierarchy.Node producer) {
    // do nothing here
  }

  public Job getJob() {
    return translationContext.getMantisJobBuilder().buildJob();
  }
}
