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

import io.mantisrx.runtime.*;
import io.mantisrx.runtime.computation.*;
import io.mantisrx.runtime.sink.SelfDocumentingSink;
import io.mantisrx.runtime.sink.Sink;
import io.mantisrx.runtime.source.Source;

public class MantisJobBuilder {
  private SourceHolder sourceHolder;
  private Stages currentStage;
  private Config jobConfig;

  MantisJobBuilder() {}

  public MantisJobBuilder addStage(Source source) {
    this.sourceHolder = MantisJob.source(source);
    return this;
  }

  public MantisJobBuilder addStage(Computation compFn, Object config) {
    if (this.currentStage == null) {
      if (this.sourceHolder == null) {
        throw new IllegalArgumentException(
            "SourceHolder not currently set. Uninitialized MantisJob configuration");
      }
      this.currentStage = addInitStage(compFn, config);
    } else {
      this.currentStage = addMoreStages(compFn, config);
    }
    return this;
  }

  public MantisJobBuilder addStage(Sink sink) {
    ScalarStages scalarStages = (ScalarStages) this.currentStage;
    this.jobConfig = scalarStages.sink(sink);
    return this;
  }

  public MantisJobBuilder addStage(SelfDocumentingSink sink) {
    ScalarStages scalarStages = (ScalarStages) this.currentStage;
    this.jobConfig = scalarStages.sink(sink);
    return this;
  }

  private Stages addInitStage(Computation compFn, Object config) {
    if (config instanceof ScalarToScalar.Config) {
      return this.sourceHolder.stage((ScalarComputation) compFn, (ScalarToScalar.Config) config);
    } else if (config instanceof ScalarToKey.Config) {
      return this.sourceHolder.stage((ToKeyComputation) compFn, (ScalarToKey.Config) config);
    }
    return this.sourceHolder.stage((ToGroupComputation) compFn, (ScalarToGroup.Config) config);
  }

  private Stages addMoreStages(Computation compFn, Object config) {
    if (this.currentStage instanceof ScalarStages) {
      ScalarStages scalarStages = (ScalarStages) this.currentStage;
      if (config instanceof ScalarToScalar.Config) {
        return scalarStages.stage((ScalarComputation) compFn, (ScalarToScalar.Config) config);
      } else if (config instanceof ScalarToKey.Config) {
        return scalarStages.stage((ToKeyComputation) compFn, (ScalarToKey.Config) config);
      }
      return scalarStages.stage((ToGroupComputation) compFn, (ScalarToGroup.Config) config);
    }
    KeyedStages keyedStages = (KeyedStages) this.currentStage;
    if (config instanceof KeyToScalar.Config) {
      return keyedStages.stage((ToScalarComputation) compFn, (KeyToScalar.Config) config);
    } else if (config instanceof GroupToScalar.Config) {
      return keyedStages.stage((GroupToScalarComputation) compFn, (GroupToScalar.Config) config);
    } else if (config instanceof KeyToKey.Config) {
      return keyedStages.stage((KeyComputation) compFn, (KeyToKey.Config) config);
    }
    return keyedStages.stage((GroupComputation) compFn, (GroupToGroup.Config) config);
  }
}
