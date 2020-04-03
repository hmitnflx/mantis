/*
 * Copyright 2020 Netflix, Inc.
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

package io.mantisrx.server.worker.jobmaster.clutch.experimental;

import com.google.common.util.concurrent.AtomicDouble;

import com.netflix.control.clutch.Clutch;
import com.netflix.control.clutch.ClutchConfiguration;

import com.yahoo.sketches.quantiles.UpdateDoublesSketch;

import io.mantisrx.runtime.descriptor.StageSchedulingInfo;

import io.vavr.Function1;
import io.vavr.Tuple;
import io.vavr.Tuple2;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MantisClutchConfigurationSelector implements Function1<Map<Clutch.Metric, UpdateDoublesSketch>, ClutchConfiguration> {

    private static final Logger logger = LoggerFactory.getLogger(MantisClutchConfigurationSelector.class);

    private final Integer stageNumber;
    private final StageSchedulingInfo stageSchedulingInfo;
    private final AtomicDouble trueCpuMax = new AtomicDouble(0.0);
    private final AtomicDouble trueNetworkMax = new AtomicDouble(0.0);

    public MantisClutchConfigurationSelector(Integer stageNumber, StageSchedulingInfo stageSchedulingInfo) {
        this.stageNumber = stageNumber;
        this.stageSchedulingInfo = stageSchedulingInfo;
    }

    @Override
    public ClutchConfiguration apply(Map<Clutch.Metric, UpdateDoublesSketch> sketches) {

        updateTrueMaxValues(sketches);

        // Setpoint
        double setPoint = 1.1 * sketches.get(Clutch.Metric.RPS).getQuantile(0.5);

        if (isSetpointHigh(sketches)) {
            setPoint *= 0.9;
        } else if (isSetpointLow(sketches)) { // Only allow too cold if we're NOT too hot.
            setPoint *= 1.11;
        }

        if (isUnderprovisioined(sketches)) {
          // No op right now.
          logger.info("Job is underprovisioned see previous messages to determine metric.");
        }

        // ROPE
        Tuple2<Double, Double> rope = Tuple.of(setPoint * 0.3, 0.0);

        // Gain
        long deltaT = stageSchedulingInfo.getScalingPolicy().getCoolDownSecs() / 30l;
        double minMaxMidPoint = stageSchedulingInfo.getScalingPolicy().getMax() - stageSchedulingInfo.getScalingPolicy().getMin();

        double kp = 1.0 / setPoint / deltaT * minMaxMidPoint / 2.0;
        double ki = 0.0;
        double kd = 1.0 / setPoint / deltaT * minMaxMidPoint / 2.0;

        resetSketches(sketches);
        return com.netflix.control.clutch.ClutchConfiguration.builder()
                .metric(Clutch.Metric.RPS)
                .setPoint(setPoint)
                .kp(kp)
                .ki(ki)
                .kd(kd)
                .minSize(stageSchedulingInfo.getScalingPolicy().getMin())
                .maxSize(stageSchedulingInfo.getScalingPolicy().getMax())
                .rope(rope)
                .cooldownInterval(stageSchedulingInfo.getScalingPolicy().getCoolDownSecs())
                .cooldownUnits(TimeUnit.SECONDS)
                .build();
    }

    private void resetSketches(Map<Clutch.Metric, UpdateDoublesSketch> sketches) {
        sketches.values().forEach(UpdateDoublesSketch::reset);
    }

    /**
     * Implements the rules for determine if the setpoint is too low, which runs the job too cold.
     * We are currently defining too cold as CPU or Network spending half their time below half of true max.
     *
     * @param sketches A map of metrics to skethces. Must contain CPU and NETWORK.
     * @return A boolean indicating if the setpoint is too low and we are thus running the job too cold.
     */
    private boolean isSetpointLow(Map<Clutch.Metric, UpdateDoublesSketch> sketches) {
        double cpuMedian = sketches.get(Clutch.Metric.CPU).getQuantile(0.5);
        double networkMedian = sketches.get(Clutch.Metric.NETWORK).getQuantile(0.5);

        boolean cpuTooLow = cpuMedian < trueCpuMax.get() * 0.5;
        boolean networkTooLow = networkMedian < trueNetworkMax.get() * 0.5;

        if (cpuTooLow) {
            logger.info("CPU running too cold for stage {} with median {} and max {}. Recommending increase in setPoint.", stageNumber, cpuMedian, trueCpuMax.get());
        }

        if (networkTooLow) {
            logger.info("Network running too cold for stage {} with median {} and max {}. Recommending increase in setPoint.", stageNumber, networkMedian, trueNetworkMax.get());
        }

        return cpuTooLow || networkTooLow;
    }

    /**
     * Implements the rules for determine if the setpoint is too high, which in turn runs the job too hot.
     * We are currently defining a setpoint as too high if CPU or Network spend half their time over 80% of true max.
     *
     * @param sketches A map of metrics to skethces. Must contain CPU and NETWORK.
     * @return A boolean indicating if the setpoint is too high and we are thus running the job too hot.
     */
    private boolean isSetpointHigh(Map<Clutch.Metric, UpdateDoublesSketch> sketches) {
        double cpuMedian = sketches.get(Clutch.Metric.CPU).getQuantile(0.5);
        double networkMedian = sketches.get(Clutch.Metric.NETWORK).getQuantile(0.5);

        boolean cpuTooHigh = cpuMedian > trueCpuMax.get() * 0.8;
        boolean networkTooHigh = networkMedian > trueNetworkMax.get() * 0.8;

        if (cpuTooHigh) {
            logger.info("CPU running too hot for stage {} with median {} and max {}. Recommending reduction in setPoint.", stageNumber, cpuMedian, trueCpuMax.get());
        }

        if (networkTooHigh) {
            logger.info("Network running too hot for stage {} with median {} and max {}. Recommending reduction in setPoint.", stageNumber, cpuMedian, trueNetworkMax.get());
        }

        return cpuTooHigh || networkTooHigh;
    }

    /**
     * Determines if a job is underprovisioned on cpu or network.
     * We are currently definiing underprovisioned as spending 20% or more of our time above the
     * provisioned resource amount.
     *
     * @param sketches A map of metrics to skethces. Must contain CPU and NETWORK.
     * @return A boolean indicating if the job is underprovisioned.
     */
    private boolean isUnderprovisioined(Map<Clutch.Metric, UpdateDoublesSketch> sketches) {
        double provisionedCpuLimit = stageSchedulingInfo.getMachineDefinition().getCpuCores() * 100.0;
        double provisionedNetworkLimit = stageSchedulingInfo.getMachineDefinition().getNetworkMbps() * 1024.0 * 1024.0;

        double cpu = sketches.get(Clutch.Metric.CPU).getQuantile(0.8);
        double network = sketches.get(Clutch.Metric.NETWORK).getQuantile(0.8);

        // If we spend 20% or more of our time above the CPU provisioned limit
        boolean cpuUnderProvisioned = cpu > provisionedCpuLimit;
        boolean networkUnderProvisioned = network > provisionedNetworkLimit;

        if (cpuUnderProvisioned) {
          logger.error("CPU is underprovisioned! 80% percentile {}% is above provisioned {}%.", cpu, provisionedCpuLimit);
        }

        if (networkUnderProvisioned) {
          logger.error("Network is underprovisioned! 80% percentile {}% is above provisioned {}%.", network, provisionedNetworkLimit);
        }

        return cpuUnderProvisioned || networkUnderProvisioned;
    }

    /**
     * Performs bookkeeping on true maximum values.
     * We need to do this because we reset the sketches constantly so their max is impacted by current setPoint.
     *
     * @param sketches A map of metrics to skethces. Must contain CPU and NETWORK.
     */
    private void updateTrueMaxValues(Map<Clutch.Metric, UpdateDoublesSketch> sketches) {
      double cpuMax = sketches.get(Clutch.Metric.CPU).getMaxValue();
      double networkMax = sketches.get(Clutch.Metric.NETWORK).getMaxValue();

      if (cpuMax > trueCpuMax.get()) {
        trueCpuMax.set(cpuMax);
      }
      if (networkMax > trueNetworkMax.get()) {
        trueNetworkMax.set(networkMax);
      }
    }
}