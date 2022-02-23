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
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.core.construction.ReadTranslation;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import rx.Observable;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
public class BeamSource<T> implements Source<T> {

  private final AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>>
      transform;
  private final SerializablePipelineOptions pipelineOptions;
  private List<UnboundedSource.UnboundedReader<T>> readers;

  public BeamSource(
      AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>> transform,
      SerializablePipelineOptions pipelineOptions) {
    this.transform = transform;
    this.pipelineOptions = pipelineOptions;
  }

  @Override
  public void init(Context context, Index index) {
    PipelineOptions pipelineOptions = transform.getPipeline().getOptions();
    try {
      if (Utils.isBounded(transform)) {
        BoundedSource<T> source = ReadTranslation.boundedSourceFromTransform(transform);
      } else {
        UnboundedSource<T, ?> source = ReadTranslation.unboundedSourceFromTransform(transform);
        if (source.requiresDeduping()) {
          throw new UnsupportedOperationException("Sources requiring de-duping aren't supported");
        }
        List<? extends UnboundedSource<T, ?>> allShards =
            source.split(index.getTotalNumWorkers(), pipelineOptions);
        List<? extends UnboundedSource<T, ?>> myShards =
            Utils.roundRobinSubList(
                allShards, context.getWorkerInfo().getWorkerIndex(), index.getTotalNumWorkers());
        readers =
            myShards.stream()
                .map(
                    sh -> {
                      try {
                        return sh.createReader(pipelineOptions, null);
                      } catch (IOException e) {
                        log.warn("failed to create reader for shard {}", sh, e);
                      }
                      return null;
                    })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        readers.forEach(
            r -> {
              try {
                log.info("reader {} has records - {}", r, r.start());
              } catch (IOException e) {
                log.warn("failed to start reader {}", r, e);
              }
            });
      }
    } catch (Exception e) {
      log.warn(
          "failed to create reader shards for source {} of type isBounded ({})",
          transform,
          Utils.isBounded(transform),
          e);
    }
  }

  @Override
  public Observable<Observable<T>> call(Context context, Index index) {
    if (readers == null || readers.isEmpty()) {
      return Observable.just(Observable.empty());
    }
    List<Observable<Observable<T>>> collect =
        readers.stream()
            .map(
                r ->
                    Observable.create(
                        (Observable.OnSubscribe<Observable<T>>)
                            subscriber -> {
                              T elem = null;
                              try {
                                if (r.advance()) {
                                  elem = r.getCurrent();
                                }
                              } catch (IOException e) {
                              }
                              subscriber.onNext(Observable.just(elem));
                            }))
            .collect(Collectors.toList());
    return Observable.merge(collect);
  }
}
