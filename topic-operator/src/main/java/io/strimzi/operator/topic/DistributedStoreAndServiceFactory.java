/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.topic;

import io.apicurio.registry.utils.ConcurrentUtil;
import io.apicurio.registry.utils.kafka.AsyncProducer;
import io.apicurio.registry.utils.kafka.ProducerActions;
import io.apicurio.registry.utils.streams.diservice.AsyncBiFunctionService;
import io.apicurio.registry.utils.streams.diservice.AsyncBiFunctionServiceGrpcLocalDispatcher;
import io.apicurio.registry.utils.streams.diservice.DefaultGrpcChannelProvider;
import io.apicurio.registry.utils.streams.diservice.DistributedAsyncBiFunctionService;
import io.apicurio.registry.utils.streams.diservice.LocalService;
import io.apicurio.registry.utils.streams.diservice.proto.AsyncBiFunctionServiceGrpc;
import io.apicurio.registry.utils.streams.distore.DistributedReadOnlyKeyValueStore;
import io.apicurio.registry.utils.streams.distore.FilterPredicate;
import io.apicurio.registry.utils.streams.distore.KeyValueSerde;
import io.apicurio.registry.utils.streams.distore.KeyValueStoreGrpcImplLocalDispatcher;
import io.apicurio.registry.utils.streams.distore.UnknownStatusDescriptionInterceptor;
import io.apicurio.registry.utils.streams.distore.proto.KeyValueStoreGrpc;
import io.apicurio.registry.utils.streams.ext.Lifecycle;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.HostInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.lang.Integer.parseInt;

/**
 * Add configuration for distributed store (via gRPC).
 * Required when we're running more than one instance/node of topic operator.
 */
class DistributedStoreAndServiceFactory implements StoreAndServiceFactory {
    private static final Logger log = LoggerFactory.getLogger(DistributedStoreAndServiceFactory.class);

    public StoreContext create(
            Config config,
            Properties kafkaProperties,
            KafkaStreams streams,
            AsyncBiFunctionService.WithSerdes<String, String, Integer> serviceImpl,
            List<AutoCloseable> closeables
    ) {
        String storeName = config.get(Config.STORE_NAME);

        String appServer = config.get(Config.APPLICATION_SERVER);
        String[] hostPort = appServer.split(":");
        if (hostPort.length != 2) {
            throw new IllegalArgumentException("Invalid application server configuration (expecting host:port): " + appServer);
        }
        log.info("Application server gRPC: '{}'", appServer);
        HostInfo hostInfo = new HostInfo(hostPort[0], parseInt(hostPort[1]));

        FilterPredicate<String, Topic> filter = (s, s1, s2, topic) -> true;

        DistributedReadOnlyKeyValueStore<String, Topic> distributedStore = new DistributedReadOnlyKeyValueStore<>(
                streams,
                hostInfo,
                storeName,
                Serdes.String(),
                new TopicSerde(),
                new DefaultGrpcChannelProvider(),
                true,
                filter
        );
        closeables.add(distributedStore);

        ProducerActions<String, TopicCommand> producer = new AsyncProducer<>(
                kafkaProperties,
                Serdes.String().serializer(),
                new TopicCommandSerde()
        );
        closeables.add(producer);

        LocalService<AsyncBiFunctionService.WithSerdes<String, String, Integer>> localService =
                new LocalService<>(WaitForResultService.NAME, serviceImpl);
        DistributedAsyncBiFunctionService<String, String, Integer> service = new DistributedAsyncBiFunctionService<>(
                streams, hostInfo, storeName, localService, new DefaultGrpcChannelProvider()
        );
        closeables.add(service);

        // gRPC

        KeyValueStoreGrpc.KeyValueStoreImplBase kvGrpc = streamsKeyValueStoreGrpcImpl(streams, storeName, filter);
        AsyncBiFunctionServiceGrpc.AsyncBiFunctionServiceImplBase fnGrpc = streamsAsyncBiFunctionServiceGrpcImpl(localService);
        Lifecycle server = streamsGrpcServer(hostInfo, kvGrpc, fnGrpc);
        server.start();
        AutoCloseable serverCloseable = server::stop;
        closeables.add(serverCloseable);

        return new StoreContext(distributedStore, service);
    }

    private KeyValueStoreGrpc.KeyValueStoreImplBase streamsKeyValueStoreGrpcImpl(
            KafkaStreams streams,
            String storeName,
            FilterPredicate<String, Topic> filterPredicate
    ) {
        return new KeyValueStoreGrpcImplLocalDispatcher(
                streams,
                KeyValueSerde
                        .newRegistry()
                        .register(
                                storeName,
                                Serdes.String(), new TopicSerde()
                        ),
                filterPredicate
        );
    }

    private AsyncBiFunctionServiceGrpc.AsyncBiFunctionServiceImplBase streamsAsyncBiFunctionServiceGrpcImpl(
            LocalService<AsyncBiFunctionService.WithSerdes<String, String, Integer>> localWaitForResultService
    ) {
        return new AsyncBiFunctionServiceGrpcLocalDispatcher(Collections.singletonList(localWaitForResultService));
    }

    private Lifecycle streamsGrpcServer(
            HostInfo localHost,
            KeyValueStoreGrpc.KeyValueStoreImplBase streamsStoreGrpcImpl,
            AsyncBiFunctionServiceGrpc.AsyncBiFunctionServiceImplBase streamsAsyncBiFunctionServiceGrpcImpl
    ) {
        UnknownStatusDescriptionInterceptor unknownStatusDescriptionInterceptor =
                new UnknownStatusDescriptionInterceptor(
                        Map.of(
                                IllegalArgumentException.class, Status.INVALID_ARGUMENT,
                                IllegalStateException.class, Status.FAILED_PRECONDITION,
                                InvalidStateStoreException.class, Status.FAILED_PRECONDITION,
                                Throwable.class, Status.INTERNAL
                        )
                );

        Server server = ServerBuilder
                .forPort(localHost.port())
                .addService(
                        ServerInterceptors.intercept(
                                streamsStoreGrpcImpl,
                                unknownStatusDescriptionInterceptor
                        )
                )
                .addService(
                        ServerInterceptors.intercept(
                                streamsAsyncBiFunctionServiceGrpcImpl,
                                unknownStatusDescriptionInterceptor
                        )
                )
                .build();

        return new ServerLifecycle(server);
    }

    private static class ServerLifecycle implements Lifecycle {
        private final Server server;

        public ServerLifecycle(Server server) {
            this.server = server;
        }

        @Override
        public void start() {
            try {
                server.start();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public void stop() {
            ConcurrentUtil
                    .<Server>consumer(Server::awaitTermination)
                    .accept(server.shutdown());
        }

        @Override
        public boolean isRunning() {
            return !(server.isShutdown() || server.isTerminated());
        }
    }

}

