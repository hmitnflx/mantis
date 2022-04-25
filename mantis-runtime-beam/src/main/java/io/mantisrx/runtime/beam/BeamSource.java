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

import com.mantisrx.common.utils.Closeables;
import io.mantisrx.runtime.Context;
import io.mantisrx.runtime.source.Index;
import io.mantisrx.runtime.source.Source;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.beam.runners.core.construction.ReadTranslation;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Source.Reader;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import rx.Observable;
import rx.subjects.PublishSubject;

@Slf4j
public class BeamSource<T> implements Source<byte[]> {

    private final AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>> transform;
    private final SerializablePipelineOptions serializedPipelineOptions;
    private final Coder outputCoder;
    private List<Reader<T>> readers;

  public BeamSource(
      AppliedPTransform<PBegin, PCollection<T>, PTransform<PBegin, PCollection<T>>> transform,
      SerializablePipelineOptions pipelineOptions) {
    this.transform = transform;
    Map.Entry<TupleTag<?>, PCollection<?>> output = Utils.getOutput(transform);
    this.outputCoder = Utils.getCoder(output.getValue());
    this.serializedPipelineOptions = pipelineOptions;
  }

  @Override
  public void init(Context context, Index index) {
    PipelineOptions pipelineOptions = transform.getPipeline().getOptions();
    try {
      if (Utils.isBounded(transform)) {
        createBoundedReaders(pipelineOptions, context, index);
      } else {
        createUnboundedReaders(pipelineOptions, context, index);
      }
    } catch (Exception e) {
      log.warn(
          "failed to create reader shards for source {} of type isBounded ({})",
          transform,
          Utils.isBounded(transform),
          e);
    }
  }

  private void createBoundedReaders(PipelineOptions pipelineOptions, Context context, Index index)
      throws Exception {
    BoundedSource<T> source = ReadTranslation.boundedSourceFromTransform(transform);
      long estimatedSizeBytes = source.getEstimatedSizeBytes(pipelineOptions);
      long desiredSizeBytes = (long) Math.ceil(1.0 * estimatedSizeBytes / index.getTotalNumWorkers());
      List<? extends BoundedSource<T>> allShards = source.split(desiredSizeBytes, pipelineOptions);
    List<? extends BoundedSource<T>> myShards =
        Utils.roundRobinSubList(
            allShards, context.getWorkerInfo().getWorkerIndex(), index.getTotalNumWorkers());
    createAllReaders(
        myShards,
        bounded -> {
          try {
            return ((BoundedSource) bounded).createReader(pipelineOptions);
          } catch (IOException e) {
            log.warn("failed to create unbounded reader for source {}", bounded, e);
          }
          return null;
        });
    startAllReaders();
  }

  private void createUnboundedReaders(PipelineOptions pipelineOptions, Context context, Index index)
      throws Exception {
    UnboundedSource<T, ?> source = ReadTranslation.unboundedSourceFromTransform(transform);
    if (source.requiresDeduping()) {
      throw new UnsupportedOperationException("Sources requiring de-duping aren't supported");
    }
    List<? extends UnboundedSource<T, ?>> allShards =
        source.split(index.getTotalNumWorkers(), pipelineOptions);
    List<? extends UnboundedSource<T, ?>> myShards =
        Utils.roundRobinSubList(
            allShards, context.getWorkerInfo().getWorkerIndex(), index.getTotalNumWorkers());
    createAllReaders(
        myShards,
        unbounded -> {
          try {
            return ((UnboundedSource) unbounded).createReader(pipelineOptions, null);
          } catch (IOException e) {
            log.warn("failed to create unbounded reader for source {}", unbounded, e);
          }
          return null;
        });
    startAllReaders();
  }

  private void createAllReaders(
      List<? extends org.apache.beam.sdk.io.Source<?>> myShards,
      Function<org.apache.beam.sdk.io.Source<?>, Reader<T>> createReaderFn) {
    readers =
        myShards.stream()
            .map(createReaderFn)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
  }

  private void startAllReaders() {
    readers.forEach(
        r -> {
          try {
            log.info("reader {} has records - {}", r, r.start());
          } catch (IOException e) {
            log.warn("failed to start reader {}", r, e);
          }
        });
  }

  @Override
  public Observable<Observable<byte[]>> call(Context context, Index index) {
      if (readers == null || readers.isEmpty()) {
        return Observable.just(Observable.empty());
      }
      PublishSubject<Observable<byte[]>> subjects = PublishSubject.create();
      readers.stream()
            .map(
                r -> {
                    PublishSubject<byte[]> subject = PublishSubject.create();
                    while (true) {
                        try {
                            if (!r.advance()) {
                                log.info("finished reading all elements from reader {}", r);
                                break;
                            }
                            WindowedValue<T> twValue = WindowedValue.timestampedValueInGlobalWindow(
                                    r.getCurrent(), r.getCurrentTimestamp());
                            subject.onNext(Utils.encode(twValue, outputCoder));
                        } catch (IOException ex) {
                            log.warn("failed to read element from reader {}", r, ex);
                        }
                    }
                    return subject.asObservable();
                })
                .forEach(subjects::onNext);


    return subjects.asObservable();
  }

    @Override
    public void close() throws IOException {
        Closeables.combine(readers).close();
    }
}
