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

import io.mantisrx.common.codec.Codec;

public class MantisByteCodec implements Codec<byte[]> {
    public static final MantisByteCodec INSTANCE = new MantisByteCodec();

    @Override
    public byte[] decode(byte[] bytes) {
        return bytes;
    }

    @Override
    public byte[] encode(byte[] value) {
        return value;
    }
}
