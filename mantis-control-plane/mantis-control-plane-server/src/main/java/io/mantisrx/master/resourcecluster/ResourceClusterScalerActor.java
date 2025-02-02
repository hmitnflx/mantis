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

package io.mantisrx.master.resourcecluster;

import akka.actor.AbstractActorWithTimers;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import com.netflix.spectator.api.BasicTag;
import io.mantisrx.common.Ack;
import io.mantisrx.common.metrics.Counter;
import io.mantisrx.common.metrics.Metrics;
import io.mantisrx.common.metrics.MetricsRegistry;
import io.mantisrx.common.metrics.spectator.MetricGroupId;
import io.mantisrx.master.resourcecluster.ResourceClusterActor.GetClusterUsageRequest;
import io.mantisrx.master.resourcecluster.proto.GetClusterIdleInstancesRequest;
import io.mantisrx.master.resourcecluster.proto.GetClusterIdleInstancesResponse;
import io.mantisrx.master.resourcecluster.proto.GetClusterUsageResponse;
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterSpec;
import io.mantisrx.master.resourcecluster.proto.MantisResourceClusterSpec.SkuTypeSpec;
import io.mantisrx.master.resourcecluster.proto.ResourceClusterScaleSpec;
import io.mantisrx.master.resourcecluster.proto.ScaleResourceRequest;
import io.mantisrx.master.resourcecluster.resourceprovider.ResourceClusterStorageProvider;
import io.mantisrx.runtime.MachineDefinition;
import io.mantisrx.server.master.resourcecluster.ClusterID;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/**
 * This actor is responsible to handle message regarding cluster usage and makes scaling decisions.
 * [Notes] There can be two communication model between the scaler actor and resource cluster actor. If the state is
 * pushed from resource cluster actor to the scaler actor, the downside is we need to ensure all changes are properly
 * handled and can trigger the push, while pulling state from scaler actor requires explicit timer firing.
 */
@Slf4j
public class ResourceClusterScalerActor extends AbstractActorWithTimers {
    private final ClusterID clusterId;

    private final Duration scalerPullThreshold;

    private final ActorRef resourceClusterActor;
    private final ActorRef resourceClusterHostActor;
    private final ResourceClusterStorageProvider storageProvider;

    private final ConcurrentMap<String, ClusterAvailabilityRule> skuToRuleMap = new ConcurrentHashMap<>();
    private final AtomicReference<MachineDefinitionToSkuMapper> skuMapperRef = new AtomicReference<>(
        new MachineDefinitionToSkuMapper(null));

    private final Clock clock;

    private final Counter numScaleUp;
    private final Counter numScaleDown;
    private final Counter numReachScaleMaxLimit;
    private final Counter numReachScaleMinLimit;
    private final Counter numScaleRuleTrigger;

    public static Props props(
        ClusterID clusterId,
        Clock clock,
        Duration scalerPullThreshold,
        ResourceClusterStorageProvider storageProvider,
        ActorRef resourceClusterHostActor,
        ActorRef resourceClusterActor) {
        return Props.create(
            ResourceClusterScalerActor.class,
            clusterId,
            clock,
            scalerPullThreshold,
            storageProvider,
            resourceClusterHostActor,
            resourceClusterActor);
    }

    public ResourceClusterScalerActor(
        ClusterID clusterId,
        Clock clock,
        Duration scalerPullThreshold,
        ResourceClusterStorageProvider storageProvider,
        ActorRef resourceClusterHostActor,
        ActorRef resourceClusterActor) {
        this.clusterId = clusterId;
        this.resourceClusterActor = resourceClusterActor;
        this.resourceClusterHostActor = resourceClusterHostActor;
        this.storageProvider = storageProvider;
        this.clock = clock;
        this.scalerPullThreshold = scalerPullThreshold;

        MetricGroupId metricGroupId = new MetricGroupId(
            "ResourceClusterScalerActor",
            new BasicTag("resourceCluster", this.clusterId.getResourceID()));

        Metrics m = new Metrics.Builder()
            .id(metricGroupId)
            .addCounter("numScaleDown")
            .addCounter("numReachScaleMaxLimit")
            .addCounter("numScaleUp")
            .addCounter("numReachScaleMinLimit")
            .addCounter("numScaleRuleTrigger")
            .build();
        m = MetricsRegistry.getInstance().registerAndGet(m);
        this.numScaleDown = m.getCounter("numScaleDown");
        this.numReachScaleMaxLimit = m.getCounter("numReachScaleMaxLimit");
        this.numScaleUp = m.getCounter("numScaleUp");
        this.numReachScaleMinLimit = m.getCounter("numReachScaleMinLimit");
        this.numScaleRuleTrigger = m.getCounter("numScaleRuleTrigger");

        // Init timer
        init();
    }

    @Override
    public Receive createReceive() {
        return
            ReceiveBuilder
                .create()
                .match(TriggerClusterUsageRequest.class, this::onTriggerClusterUsageRequest)
                .match(GetClusterUsageResponse.class, this::onGetClusterUsageResponse)
                .match(GetClusterIdleInstancesResponse.class, this::onGetClusterIdleInstancesResponse)
                .match(Ack.class, ack -> log.info("Received ack from {}", sender()))
                .build();
    }

    private void init() {
        this.storageProvider.getResourceClusterScaleRules(this.clusterId.toString())
                .thenAccept(rules ->
                     rules
                        .getScaleRules().values()
                        .forEach(rule -> {
                            log.info("Adding scaleRule: {}", rule);
                            this.skuToRuleMap.put(
                                rule.getSkuId(),
                                new ClusterAvailabilityRule(rule, this.clock));
                        }));

        this.storageProvider.getResourceClusterSpecWritable(this.clusterId.toString())
            .thenAccept(specWritable ->
                this.skuMapperRef.set(new MachineDefinitionToSkuMapper(specWritable.getClusterSpec())));

        getTimers().startTimerWithFixedDelay(
            "ClusterScaler-" + this.clusterId,
            new TriggerClusterUsageRequest(this.clusterId),
            scalerPullThreshold);
    }

    private void onGetClusterUsageResponse(GetClusterUsageResponse usageResponse) {
        log.info("Getting cluster usage: {}", usageResponse);
        this.numScaleRuleTrigger.increment();

        // get usage by mDef
        // for each mdef: locate rule for the mdef, apply rule if under coolDown.
        // update coolDown timer.

        // 1 matcher for usage and rule.
        // 2 rule apply to usage.
        // 3 translate between decision to scale request. (inline for now)

        usageResponse.getUsages().forEach(usage -> {
            Optional<String> skuIdO = this.skuMapperRef.get().map(usage.getDef());

            if (skuIdO.isPresent() && this.skuToRuleMap.containsKey(skuIdO.get())) {
                Optional<ScaleDecision> decisionO = this.skuToRuleMap.get(skuIdO.get()).apply(usage);
                if (decisionO.isPresent()) {
                    log.info("Informing scale decision: {}", decisionO.get());
                    switch (decisionO.get().getType()) {
                        case ScaleDown:
                            log.info("Scaling down, fetching idle instances.");
                            this.numScaleDown.increment();
                            this.resourceClusterActor.tell(
                                GetClusterIdleInstancesRequest.builder()
                                    .clusterID(this.clusterId)
                                    .skuId(skuIdO.get())
                                    .machineDefinition(usage.getDef())
                                    .desireSize(decisionO.get().getDesireSize())
                                    .maxInstanceCount(
                                        Math.max(0, usage.getTotalCount() - decisionO.get().getDesireSize()))
                                    .build(),
                                self());
                            break;
                        case ScaleUp:
                            log.info("Scaling up, informing host actor: {}", decisionO.get());
                            this.numScaleUp.increment();
                            this.resourceClusterHostActor.tell(translateScaleDecision(decisionO.get()), self());
                            break;
                        case NoOpReachMax:
                            this.numReachScaleMaxLimit.increment();
                            break;
                        case NoOpReachMin:
                            this.numReachScaleMinLimit.increment();
                            break;
                        default:
                            throw new RuntimeException("Invalid scale type: " + decisionO);
                    }
                }
            }
            else {
                log.info("No sku rule is available for {}", usage.getDef());
            }
        });

        getSender().tell(Ack.getInstance(), self());
    }


    private void onGetClusterIdleInstancesResponse(GetClusterIdleInstancesResponse response) {
        log.info("On GetClusterIdleInstancesResponse, informing host actor: {}", response);
        this.resourceClusterHostActor.tell(
            ScaleResourceRequest.builder()
                .clusterId(this.clusterId.getResourceID())
                .skuId(response.getSkuId())
                .desireSize(response.getDesireSize())
                .idleInstances(response.getInstanceIds())
                .build(),
            self());
    }

    private void onTriggerClusterUsageRequest(TriggerClusterUsageRequest req) {
        log.trace("Requesting cluster usage: {}", this.clusterId);
        this.resourceClusterActor.tell(new GetClusterUsageRequest(this.clusterId), self());
    }

    private ScaleResourceRequest translateScaleDecision(ScaleDecision decision) {
        return ScaleResourceRequest.builder()
            .clusterId(this.clusterId.getResourceID())
            .skuId(decision.getSkuId())
            .desireSize(decision.getDesireSize())
            .build();
    }

    @Value
    @Builder
    static class TriggerClusterUsageRequest {
        ClusterID clusterID;
    }

    static class MachineDefinitionToSkuMapper {
        private final Map<MachineDefinition, String> mDefToSkuMap = new HashMap<>();

        public MachineDefinitionToSkuMapper(MantisResourceClusterSpec clusterSpec) {
            if (clusterSpec == null) {
                return;
            }

            clusterSpec.getSkuSpecs().forEach(skuSpec -> {
                MachineDefinition mDef = skuToMachineDefinition(skuSpec);
                this.mDefToSkuMap.put(mDef, skuSpec.getSkuId());
                log.info("Add MachineDefinition mapper: {}, {}", mDef, skuSpec.getSkuId());
            });
        }

        /**
         * Map given {@link MachineDefinition} to the cluster SKU id.
         * @param mDef Given Machine definition.
         * @return skuId as string. Empty Optional if not found.
         */
        public Optional<String> map(MachineDefinition mDef)
        {
            return Optional.ofNullable(this.mDefToSkuMap.getOrDefault(mDef, null));
        }

        private static MachineDefinition skuToMachineDefinition(SkuTypeSpec sku) {
            //TODO validate rounding error from TaskExecutor registration.
            return new MachineDefinition(
                sku.getCpuCoreCount(),
                sku.getMemorySizeInBytes(),
                sku.getNetworkMbps(),
                sku.getDiskSizeInBytes(),
                5 // num of ports is currently a hardcoded value from {@link TaskExecutor}.
            );
        }
    }

    static class ClusterAvailabilityRule {
        private final ResourceClusterScaleSpec scaleSpec;
        private final Clock clock;
        private Instant lastActionInstant;

        public ClusterAvailabilityRule(ResourceClusterScaleSpec scaleSpec, Clock clock) {
            this.scaleSpec = scaleSpec;
            this.clock = clock;

            this.lastActionInstant = Instant.MIN;
        }
        public Optional<ScaleDecision> apply(GetClusterUsageResponse.UsageByMachineDefinition usage) {
            // Cool down check
            if (this.lastActionInstant.plusSeconds(this.scaleSpec.getCoolDownSecs()).compareTo(clock.instant()) > 0) {
                log.debug("Scale CoolDown skip: {}, {}", this.scaleSpec.getClusterId(), this.scaleSpec.getSkuId());
                return Optional.empty();
            }

            this.lastActionInstant = clock.instant();

            Optional<ScaleDecision> decision = Optional.empty();
            if (usage.getIdleCount() > scaleSpec.getMaxIdleToKeep()) {
                // too many idle agents, scale down.
                int step = usage.getIdleCount() - scaleSpec.getMaxIdleToKeep();
                int newSize = Math.max(
                    usage.getTotalCount() - step, this.scaleSpec.getMinSize());
                decision = Optional.of(
                    ScaleDecision.builder()
                        .clusterId(this.scaleSpec.getClusterId())
                        .skuId(this.scaleSpec.getSkuId())
                        .desireSize(newSize)
                        .maxSize(newSize)
                        .minSize(newSize)
                        .type(newSize == usage.getTotalCount() ? ScaleType.NoOpReachMin : ScaleType.ScaleDown)
                        .build());
            }
            else if (usage.getIdleCount() < scaleSpec.getMinIdleToKeep()) {
                // scale up
                int step = scaleSpec.getMinIdleToKeep() - usage.getIdleCount();
                int newSize = Math.min(
                    usage.getTotalCount() + step, this.scaleSpec.getMaxSize());
                decision = Optional.of(
                    ScaleDecision.builder()
                        .clusterId(this.scaleSpec.getClusterId())
                        .skuId(this.scaleSpec.getSkuId())
                        .desireSize(newSize)
                        .maxSize(newSize)
                        .minSize(newSize)
                        .type(newSize == usage.getTotalCount() ? ScaleType.NoOpReachMax : ScaleType.ScaleUp)
                        .build());
            }

            log.info("Scale Decision for {}-{}: {}",
                this.scaleSpec.getClusterId(), this.scaleSpec.getSkuId(), decision);
            return decision;
        }
    }

    enum ScaleType {
        NoOpReachMax,
        NoOpReachMin,
        ScaleUp,
        ScaleDown,
    }

    @Value
    @Builder
    static class ScaleDecision {
        String skuId;
        String clusterId;
        int maxSize;
        int minSize;
        int desireSize;
        ScaleType type;
    }
}
