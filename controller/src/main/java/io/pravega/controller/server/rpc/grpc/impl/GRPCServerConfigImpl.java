/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rpc.grpc.impl;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.pravega.common.Exceptions;
import io.pravega.controller.server.rpc.grpc.GRPCServerConfig;
import lombok.Builder;
import lombok.Data;

/**
 * gRPC server config.
 */
@Data
public class GRPCServerConfigImpl implements GRPCServerConfig {
    private final int port;
    private final String publishedRPCHost;
    private final Integer publishedRPCPort;
    private final boolean authorizationEnabled;
    private final String userPasswordFile;
    private final boolean tlsEnabled;
    private final String tlsCertFile;
    private final String tlsKeyFile;
    private final String tokenSigningKey;
    private final Integer accessTokenTTLInSeconds;
    private final String tlsTrustStore;
    private final boolean replyWithStackTraceOnError;
    private final boolean requestTracingEnabled;

    @Builder
    public GRPCServerConfigImpl(
            int port,
            String publishedRPCHost,
            Integer publishedRPCPort,
            boolean authorizationEnabled,
            String userPasswordFile,
            boolean tlsEnabled,
            String tlsCertFile,
            String tlsKeyFile,
            String tokenSigningKey,
            Integer accessTokenTTLInSeconds,
            String tlsTrustStore,
            boolean replyWithStackTraceOnError,
            boolean requestTracingEnabled
    ) {
        Preconditions.checkArgument(port > 0, "Invalid port.");
        if (publishedRPCHost != null) {
            Exceptions.checkNotNullOrEmpty(publishedRPCHost, "publishedRPCHost");
        }
        if (publishedRPCPort != null) {
            Preconditions.checkArgument(publishedRPCPort > 0, "publishedRPCPort should be a positive integer");
        }
        if (accessTokenTTLInSeconds != null) {
            Preconditions.checkArgument(accessTokenTTLInSeconds == -1 || accessTokenTTLInSeconds >= 0,
                    "accessTokenTtlInSeconds should be -1 (token never expires), 0 (token immediately expires) "
                            + "or a positive integer representing the number of seconds after which the token expires.");
        }

        this.port = port;
        this.publishedRPCHost = publishedRPCHost;
        this.publishedRPCPort = publishedRPCPort;
        this.authorizationEnabled = authorizationEnabled;
        this.userPasswordFile = userPasswordFile;
        this.tlsEnabled = tlsEnabled;
        this.tlsCertFile = tlsCertFile;
        this.tlsKeyFile = tlsKeyFile;
        this.tlsTrustStore = tlsTrustStore;
        this.tokenSigningKey = tokenSigningKey;
        this.accessTokenTTLInSeconds = accessTokenTTLInSeconds;
        this.replyWithStackTraceOnError = replyWithStackTraceOnError;
        this.requestTracingEnabled = requestTracingEnabled;
    }

    @Override
    public String toString() {
        // Note: We don't use Lombok @ToString to automatically generate an implementation of this method,
        // in order to avoid returning a string containing sensitive security configuration.

        return "GRPCServerConfigImpl(" +

                // Endpoint config
                String.format("port: %d, ", port) +
                String.format("publishedRPCHost: %s, ",
                        publishedRPCHost != null ? publishedRPCHost : "null") +
                String.format("publishedRPCPort: %s, ",
                        publishedRPCPort != null ? publishedRPCPort : "null") +

                // Auth config
                String.format("authorizationEnabled: %b, ", authorizationEnabled) +
                String.format("userPasswordFile is %s, ",
                        Strings.isNullOrEmpty(userPasswordFile) ? "unspecified" : "specified") +
                String.format("tokenSigningKey is %s, ",
                        Strings.isNullOrEmpty(tokenSigningKey) ? "unspecified" : "specified") +
                String.format("accessTokenTTLInSeconds: %s, ", accessTokenTTLInSeconds) +

                // TLS config
                String.format("tlsEnabled: %b, ", tlsEnabled) +
                String.format("tlsCertFile is %s, ",
                        Strings.isNullOrEmpty(tlsCertFile) ? "unspecified" : "specified") +
                String.format("tlsKeyFile is %s, ",
                        Strings.isNullOrEmpty(tlsKeyFile) ? "unspecified" : "specified") +
                String.format("tlsTrustStore is %s, ",
                        Strings.isNullOrEmpty(tlsTrustStore) ? "unspecified" : "specified") +

                // Request processing config
                String.format("replyWithStackTraceOnError: %b, ", replyWithStackTraceOnError) +
                String.format("requestTracingEnabled: %b", requestTracingEnabled) +
                ")";
    }
}
