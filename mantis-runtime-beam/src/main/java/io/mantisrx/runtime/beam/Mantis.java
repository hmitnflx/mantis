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

import io.mantisrx.runtime.beam.api.MantisConfig;
import io.mantisrx.runtime.beam.api.MantisInstance;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.sdk.transforms.PTransform;

import java.util.HashMap;
import java.util.Map;

public class Mantis {
  private static final Map<String, MantisTransformTranslator<?>> TRANSLATORS = new HashMap<>();

  static {
    TRANSLATORS.put(
        PTransformTranslation.READ_TRANSFORM_URN, new Translators.ReadSourceTranslator());
    TRANSLATORS.put(PTransformTranslation.PAR_DO_TRANSFORM_URN, new Translators.ParDoTranslator());
    TRANSLATORS.put(
        PTransformTranslation.GROUP_BY_KEY_TRANSFORM_URN, new Translators.GroupByKeyTranslator<>());
    TRANSLATORS.put(
        PTransformTranslation.FLATTEN_TRANSFORM_URN, new Translators.FlattenTranslator());
    TRANSLATORS.put(
        PTransformTranslation.ASSIGN_WINDOWS_TRANSFORM_URN, new Translators.WindowTranslator());
    TRANSLATORS.put(
        PTransformTranslation.IMPULSE_TRANSFORM_URN, new Translators.ImpulseTranslator());
  }

  public static MantisInstance newMantisClient(MantisConfig mantisConfig) {
    return new MantisInstance();
  }

  static MantisTransformTranslator<?> translatorProvider(PTransform<?, ?> pTransform) {
    String urn = PTransformTranslation.urnForTransformOrNull(pTransform);
    return (urn == null) ? null : TRANSLATORS.get(urn);
  }
}
