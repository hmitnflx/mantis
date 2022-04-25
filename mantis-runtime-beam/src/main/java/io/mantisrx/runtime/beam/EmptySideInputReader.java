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

import org.apache.beam.runners.core.SideInputReader;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollectionView;
import org.checkerframework.checker.nullness.qual.Nullable;

public class EmptySideInputReader implements SideInputReader {
    public static final SideInputReader INSTANCE = new EmptySideInputReader();

    private EmptySideInputReader() {
    }

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
}
