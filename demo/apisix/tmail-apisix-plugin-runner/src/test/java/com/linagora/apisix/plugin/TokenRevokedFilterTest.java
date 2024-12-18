/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linagora.apisix.plugin;

import static com.linagora.apisix.plugin.RedisRevokedTokenRepository.IGNORE_REDIS_ERRORS;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.flatbuffers.FlatBufferBuilder;

import io.github.api7.A6.HTTPReqCall.Action;
import io.github.api7.A6.TextEntry;
import io.lettuce.core.api.reactive.RedisStringReactiveCommands;
import io.netty.channel.embedded.EmbeddedChannel;

import org.apache.apisix.plugin.runner.A6Conf;
import org.apache.apisix.plugin.runner.A6ConfigRequest;
import org.apache.apisix.plugin.runner.A6ConfigResponse;
import org.apache.apisix.plugin.runner.A6ConfigWatcher;
import org.apache.apisix.plugin.runner.HttpRequest;
import org.apache.apisix.plugin.runner.HttpResponse;
import org.apache.apisix.plugin.runner.filter.PluginFilter;
import org.apache.apisix.plugin.runner.handler.BinaryProtocolDecoder;
import org.apache.apisix.plugin.runner.handler.PrepareConfHandler;
import org.apache.apisix.plugin.runner.handler.RpcCallHandler;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

class TokenRevokedFilterTest {
    static final String REDIS_PASSWORD = "secret";
    static final String JWT = "Bearer eyJraWQiOiJkcmpHMXQ1NzZ6VVc2Z3JHcTFVNEFBIiwiYWxnIjoiUlM1MTIifQ.eyJzY29wZSI6Im9wZW5pZCBwcm9maWxlIGVtYWlsIiwiZW1haWwiOiJqYW1lcy11c2VyQHRtYWlsLmNvbSIsInN1YiI6ImphbWVzLXVzZXJAdG1haWwuY29tIiwianRpIjoiYWM2MWMzMGJlMGRjYmQxN2Q2YzhhZmE4NWUxYmI0YTZjZTVkMjFlNmQxNmIwOWRhNDM0YzMxOTE1MGM0ZTg4NyIsIm5hbWUiOiJqYW1lcy11c2VyIiwiYXVkIjpbImphbWVzIl0sImNsaWVudF9pZCI6ImphbWVzIiwiaWF0IjoxNzM0NTE4MjY3LCJzaWQiOiI4UE85czh5ekZ4a1NGUXcxTENrc09SSGVXS29EbmE4UlNzdExUR0x6TzFFIiwiZXhwIjoxNzM0NTE5NDY3LCJpc3MiOiJodHRwOi8vc3NvLmV4YW1wbGUuY29tIn0.ZJ_fzVzSO0Hd40KjoibQg-brsIrybQPzgd8iiPiFQGEfbjShULAhETLy2-K-16DkTdtBKkbIS9UHUw7BptkO26OHuEBIHi6-JBk4bKyJVv4hHvRxASpSMKARnmH9CNdsT0uVhmm-F69MPouxk9fxgw5o_frOOh8J7VhYJDlTqSRsNszcGKUo7kdnSjBEXe2YxxubRR7487hdVgRa1uhWTcGjBaMyNgH151GA8yXFUVwvur7dqGd27bQttcX2VPM3EriFAFNq6bJ2UOBhc-CmF-j8io7vImI5xSiJyefXs-3T-kVB6H9uFmDd1By1mHiufAK5RIzVUIjBAK09wNWH-A";
    static final String SID = "8PO9s8yzFxkSFQw1LCksORHeWKoDna8RSstLTGLzO1E";

    static GenericContainer<?> REDIS_CONTAINER = new GenericContainer<>(DockerImageName.parse("bitnami/redis:7.0.4-debian-11-r25"))
        .withEnv("REDIS_PASSWORD", REDIS_PASSWORD)
        .withExposedPorts(6379);

    RpcCallHandler rpcCallHandler;

    Cache<Long, A6Conf> cache;

    Map<String, PluginFilter> filters;

    List<A6ConfigWatcher> watchers;

    EmbeddedChannel channel;

    PrepareConfHandler prepareConfHandler;

    long confToken;

    @BeforeAll
    static void setup() {
        REDIS_CONTAINER.start();
    }

    @AfterAll
    static void afterAll() {
        REDIS_CONTAINER.stop();
    }

    @BeforeEach
    void beforeEach() {
        RedisStringReactiveCommands<String, String> redisStringCommands = AppConfiguration.initRedisCommandStandalone(
            String.format("%s:%d", REDIS_CONTAINER.getHost(), REDIS_CONTAINER.getMappedPort(6379)),
            REDIS_PASSWORD, Duration.ofSeconds(20));
        RedisRevokedTokenRepository revokedTokenRepository = new RedisRevokedTokenRepository(redisStringCommands, IGNORE_REDIS_ERRORS);

        filters = new HashMap<>();
        filters.put("TokenRevokedFilter", new TokenRevokedFilter(revokedTokenRepository));
        watchers = new ArrayList<>();
        cache = CacheBuilder.newBuilder().expireAfterWrite(3600, TimeUnit.SECONDS).maximumSize(1000).build();
        FlatBufferBuilder builder = new FlatBufferBuilder();

        int cat = builder.createString("TokenRevokedFilter");
        int dog = builder.createString("AAAAA");
        int filter = TextEntry.createTextEntry(builder, cat, dog);

        int confVector = io.github.api7.A6.PrepareConf.Req.createConfVector(builder, new int[]{filter});
        io.github.api7.A6.PrepareConf.Req.startReq(builder);
        io.github.api7.A6.PrepareConf.Req.addConf(builder, confVector);
        builder.finish(io.github.api7.A6.PrepareConf.Req.endReq(builder));
        io.github.api7.A6.PrepareConf.Req req = io.github.api7.A6.PrepareConf.Req.getRootAsReq(builder.dataBuffer());

        A6ConfigRequest request = new A6ConfigRequest(req);
        prepareConfHandler = new PrepareConfHandler(cache, filters, watchers);
        channel = new EmbeddedChannel(new BinaryProtocolDecoder(), prepareConfHandler);
        channel.writeInbound(request);
        channel.finish();
        A6ConfigResponse response = channel.readOutbound();
        confToken = response.getConfToken();

        prepareConfHandler = new PrepareConfHandler(cache, filters, watchers);
        rpcCallHandler = new RpcCallHandler(cache);
        channel = new EmbeddedChannel(new BinaryProtocolDecoder(), prepareConfHandler, rpcCallHandler);
    }

    @AfterEach
    void afterEach() throws IOException, InterruptedException {
        ContainerHelper.unPause(REDIS_CONTAINER);

        REDIS_CONTAINER.execInContainer("redis-cli", "-a", REDIS_PASSWORD, "flushall");
        TimeUnit.MILLISECONDS.sleep(100);
    }

    @Test
    void filterShouldAddCode401ToResponse() throws IOException, InterruptedException {
        REDIS_CONTAINER.execInContainer("redis-cli", "-a", REDIS_PASSWORD, "SET", SID, "true");
        pauseRedisForAwhile();

        FlatBufferBuilder builder = new FlatBufferBuilder();

        io.github.api7.A6.HTTPReqCall.Req.startReq(builder);
        io.github.api7.A6.HTTPReqCall.Req.addConfToken(builder, confToken);
        builder.finish(io.github.api7.A6.HTTPReqCall.Req.endReq(builder));

        io.github.api7.A6.HTTPReqCall.Req req = io.github.api7.A6.HTTPReqCall.Req.getRootAsReq(builder.dataBuffer());
        HttpRequest request = new HttpRequest(req);
        request.getHeaders().put("Authorization", JWT);
        channel.writeInbound(request);
        channel.finish();
        HttpResponse response = channel.readOutbound();
        io.github.api7.A6.HTTPReqCall.Resp resp =
            io.github.api7.A6.HTTPReqCall.Resp.getRootAsResp(response.encode());

        Assertions.assertEquals(resp.actionType(), Action.Stop);
    }

    private void pauseRedisForAwhile() {
        ContainerHelper.pause(REDIS_CONTAINER);
        new Thread(() -> {
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            ContainerHelper.unPause(REDIS_CONTAINER);
        }).start();
    }
}