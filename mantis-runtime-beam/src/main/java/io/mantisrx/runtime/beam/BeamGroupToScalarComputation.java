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
import io.mantisrx.runtime.computation.GroupToScalarComputation;
import org.apache.beam.runners.core.*;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.checkerframework.checker.units.qual.K;
import rx.Observable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BeamGroupToScalarComputation<K, V> implements GroupToScalarComputation<byte[], byte[], byte[]> {
    private final AppliedPTransform<?, ?, ?> appliedTransform;
    private final WindowingStrategy<?, ? extends BoundedWindow> windowingStrategy;
    private final SystemReduceFn<K, V, Iterable<V>, Iterable<V>, ? extends BoundedWindow> reduceFn;
    private final MantisParDoOutputManager outputManager;
    private final WindowedValue.WindowedValueCoder<KV<K, V>> inputCoder;
    private final Coder<WindowedValue<KV<K, Iterable<V>>>> outputCoder;
    private final TupleTag<KV<K, Iterable<V>> mainTag;

    public BeamGroupToScalarComputation(AppliedPTransform<?, ?, ?> appliedTransform) {
        this.appliedTransform = appliedTransform;
        this.windowingStrategy = (WindowingStrategy<?, BoundedWindow>)
            ((PCollection) Utils.getOutput(appliedTransform).getValue()).getWindowingStrategy();
        PCollection<KV<K, V>> input = (PCollection<KV<K, V>>) Utils.getInput(appliedTransform);
        Map.Entry<TupleTag<?>, PCollection<?>> output = Utils.getOutput(appliedTransform);
        this.mainTag = output.getKey();
        this.outputCoder = Utils.getCoder(output.getValue());
        this.inputCoder = Utils.getWindowedValueCoder(input);
        this.outputManager = new MantisParDoOutputManager(Utils.stringifyTransform(appliedTransform), Utils.getOutputValueCoders(appliedTransform));
        this.reduceFn = SystemReduceFn.buffering();
    }

    @Override
    public void init(Context context) {
    }

    @Override
    public Observable<byte[]> call(Context context, Observable<MantisGroup<byte[], byte[]>> obs) {
        DoFn<KeyedWorkItem<K, V>, KV<K, Iterable<V>>> keyedWorkItemKVDoFn = GroupAlsoByWindowViaWindowSetNewDoFn.create(
            windowingStrategy,
            new InMemoryStateInternalsFactory<K>(),
            new InMemoryTimerInternalsFactory<K>(),
            EmptySideInputReader.INSTANCE,
            reduceFn,
            outputManager,
            mainTag);
        doOnNext = x.doOnNext(i -> toKvItem(i)).map(kvi -> )
            .subscribe();
        obs.groupBy(MantisGroup::getKeyValue, MantisGroup::getValue)
            .map(go -> go.map(tokvItem()).subscribe(t -> {
                    reducefnrunner.initialize();
                    reducefnrunner.processBundle(kvi);

            })).subscribe();

            WindowedValue<KV<K, V>> windowedkv = Utils.decodeWindowedValue(x.getValue(), inputCoder);
            // convert v to KV and then eventually to KeyedWorkItem
            return windowedkv;
        }
        return outputManager.getSubject().asObservable();
        //todo(hmittal): do things here using ReduceFnRunner
        Observable<byte[]> retValue = obs.groupBy(MantisGroup::getKeyValue, MantisGroup::getValue)
            .flatMap(gobs -> gobs.collect(() -> new ArrayList<byte[]>(), (acc, i) -> acc.add(i)))
            .map(x -> x.get(0));
        return retValue;
    }

    private static class InMemoryStateInternalsFactory<K> implements StateInternalsFactory<K>, Serializable {
        @Override
        public StateInternals stateInternalsForKey(K key) {
            return InMemoryStateInternals.forKey(key);
        }
    }

    private static class InMemoryTimerInternalsFactory<K> implements TimerInternalsFactory<K>, Serializable {
        private final Map<K, TimerInternals> timerMap = new ConcurrentHashMap<>();
        @Override
        public TimerInternals timerInternalsForKey(K key) {
            //todo(hmittal): I think for mantis, we can simplify and
            // only use one timerInternal across everything
            return timerMap.computeIfAbsent(key, (k) -> new InMemoryTimerInternals());
        }
    }
}
