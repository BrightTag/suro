package com.netflix.suro;

import java.util.concurrent.Executors;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import com.netflix.suro.aws.PropertyAWSCredentialsProvider;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.routing.RoutingMap;
import com.netflix.suro.server.StatusServer;
import com.netflix.suro.sink.SinkManager;

/**
 * Guice module for binding {@link AWSCredentialsProvider},
 * Jackson {@link ObjectMapper}, {@link SinkManager}, {@link RoutingMap},
 * {@link SuroService}, {@link StatusServer}
 *
 * @author elandau
 */
public class SuroModule extends AbstractModule {
    private static AsyncEventBus asyncEventBus = new AsyncEventBus(Executors.newCachedThreadPool());

    @Override
    protected void configure() {
        bind(AWSCredentialsProvider.class)
            .annotatedWith(Names.named("credentials")).to(PropertyAWSCredentialsProvider.class);

        bind(ObjectMapper.class).to(DefaultObjectMapper.class).asEagerSingleton();
        bind(AsyncEventBus.class)
            .annotatedWith(Names.named("async event bus")).toInstance(asyncEventBus);
        bind(AWSCredentialsProvider.class).to(PropertyAWSCredentialsProvider.class);
        bind(SuroService.class);
        bind(StatusServer.class);
    }
}
