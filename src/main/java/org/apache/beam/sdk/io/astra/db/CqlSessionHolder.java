/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.astra.db;

/*-
 * #%L
 * Beam SDK for Astra
 * --
 * Copyright (C) 2023 DataStax
 * --
 * Licensed under the Apache License, Version 2.0
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Hold singletons for Astra connections.
 */
public class CqlSessionHolder {

  /** Logger. */
  private static final Logger LOG = LoggerFactory.getLogger(CqlSessionHolder.class);

  /**
   * Cache for sessions (SHA-1 / CqlSession).
   */
  private static ConcurrentHashMap<String, CqlSession> cacheSessions;

  /**
   * Message Digest.
   */
  private static MessageDigest messageDigest;

  /**
   * Retrieve a Cluster from a @see AstraDbIO.Write.
   * @param read
   *    current reader
   * @return
   *    current cluster
   */
  public static synchronized CqlSession getCqlSession(AstraDbIO.Read<?> read) {
    return getCqlSession(read.token(), read.secureConnectBundle(), read.keyspace());
  }

  /**
   * Retrieve a Cluster from a @see AstraDbIO.Write.
   * @param write
   *    current writer
   * @return
   *    current cluster
   */
  public static synchronized CqlSession getCqlSession(AstraDbIO.Write<?> write) {
    return getCqlSession(write.token(), write.secureConnectBundle(), write.keyspace());
  }

  /**
   * Get Astra cluster using either hosts and port or cloudSecureBundle.
   *
   * @param token
   *    token (or clientSecret)
   * @param secureConnectBundle
   *    read scb as stream
   * @return
   *    cassandra cluster
   */
  public static synchronized CqlSession getCqlSession(
          ValueProvider<String> token,
          ValueProvider<byte[]> secureConnectBundle,
          ValueProvider<String> keyspace) {
    // Parameters Validations
    if (token == null) throw new IllegalArgumentException("Token is Required");
    if (secureConnectBundle == null) throw new IllegalArgumentException("Cloud Secure Bundle is Required");

    init();

    // Compute SHA-1 for the session
    String hashedSessionKey = hashCqlSessionParams(token.get(), secureConnectBundle.get());
    if (!cacheSessions.containsKey(hashedSessionKey)) {
      long top = System.currentTimeMillis();
      LOG.info("Initializing Cluster...");
      CqlSessionBuilder builder = CqlSession.builder();
      builder.withAuthCredentials("token", token.get());
      if (secureConnectBundle != null) {
        builder.withCloudSecureConnectBundle(new ByteArrayInputStream(secureConnectBundle.get()));
      }
      if (keyspace != null) {
        builder.withKeyspace(keyspace.get());
      }
      cacheSessions.put(hashedSessionKey, builder.build());
      LOG.info("Session created in {} millis.", System.currentTimeMillis() - top);
    }
    return cacheSessions.get(hashedSessionKey);
  }

  /**
   * Generate a fast hash to identify a cluster with a key.
   *
   * @param token
   *    current token
   * @param secureConnectBundle
   *    secure connect bundle binary
   * @return
   *    sha1 cluster
   */
  private static String hashCqlSessionParams(String token, byte[] secureConnectBundle) {
    if (token == null || "".equals(token)) {
      throw new IllegalArgumentException("Token is Required");
    }
    if (secureConnectBundle == null) {
      throw new IllegalArgumentException("Cloud Secure Bundle is Required");
    }
    Formatter formatter = new Formatter();
    for (byte b : messageDigest.digest(secureConnectBundle)) {
        formatter.format("%02x", b);
    }
    return token + "_" + formatter;
  }

  /**
   * Singleton Pattern.
   *
   * @return
   *    singleton instance.
   */
  private static synchronized void init() {
    if (cacheSessions == null) {
      try {
        cacheSessions = new ConcurrentHashMap<>();
        messageDigest = MessageDigest.getInstance("SHA-1");
        LOG.debug("CqlSessionHolder initialized.");
      } catch (NoSuchAlgorithmException e) {
        throw new IllegalStateException("SHA-1 is not supported");
      }
      // Add a Shutdown Hook to close all sessions.
      Runtime.getRuntime().addShutdownHook(new Thread(() -> { cleanup(); }));
    }
  }

  public static void cleanup() {
    cacheSessions.values().stream()
            .filter(s->!s.isClosed())
            .forEach(CqlSession::close);
  }

}
