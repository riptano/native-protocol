/*
 * Copyright (C) 2017-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.cassandra.protocol.internal.request;

import com.datastax.cassandra.protocol.internal.Message;
import com.datastax.cassandra.protocol.internal.MessageTest;
import com.datastax.cassandra.protocol.internal.ProtocolConstants;
import com.datastax.cassandra.protocol.internal.TestDataProviders;
import com.datastax.cassandra.protocol.internal.binary.MockBinaryString;
import com.datastax.cassandra.protocol.internal.request.query.QueryOptions;
import com.datastax.cassandra.protocol.internal.response.QueryOptionsBuilder;
import com.datastax.cassandra.protocol.internal.util.Bytes;
import org.testng.annotations.Test;

import static com.datastax.cassandra.protocol.internal.Assertions.assertThat;

public class QueryTest extends MessageTest<Query> {
  private String queryString = "select * from system.local";

  public QueryTest() {
    super(Query.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new Query.Codec(protocolVersion);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_encode_and_decode_query_with_default_options(int protocolVersion) {
    Query initial = new Query(queryString);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .longString("select * from system.local")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.ONE)
                .byte_(0) // no flags
            );

    assertThat(encodedSize(initial, protocolVersion)).isEqualTo(4 + queryString.length() + 2 + 1);

    Query decoded = decode(encoded, protocolVersion);

    assertThat(decoded.query).isEqualTo(initial.query);
    assertThat(decoded.options.consistency).isEqualTo(ProtocolConstants.ConsistencyLevel.ONE);
    assertThat(decoded.options.positionalValues).isEmpty();
    assertThat(decoded.options.namedValues).isEmpty();
    assertThat(decoded.options.skipMetadata).isFalse();
    assertThat(decoded.options.pageSize).isEqualTo(-1);
    assertThat(decoded.options.pagingState).isNull();
    assertThat(decoded.options.serialConsistency)
        .isEqualTo(ProtocolConstants.ConsistencyLevel.SERIAL);
    assertThat(decoded.options.defaultTimestamp).isEqualTo(Long.MIN_VALUE);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_encode_and_decode_query_with_different_CL(int protocolVersion) {
    QueryOptions options =
        new QueryOptionsBuilder()
            .consistencyLevel(ProtocolConstants.ConsistencyLevel.QUORUM)
            .build();
    Query initial = new Query(queryString, options);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .longString("select * from system.local")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.QUORUM)
                .byte_(0));

    assertThat(encodedSize(initial, protocolVersion)).isEqualTo(4 + queryString.length() + 2 + 1);

    Query decoded = decode(encoded, protocolVersion);

    assertThat(decoded.query).isEqualTo(initial.query);
    assertThat(decoded.options.consistency).isEqualTo(ProtocolConstants.ConsistencyLevel.QUORUM);
    assertThat(decoded.options.positionalValues).isEmpty();
    assertThat(decoded.options.namedValues).isEmpty();
    assertThat(decoded.options.skipMetadata).isFalse();
    assertThat(decoded.options.pageSize).isEqualTo(-1);
    assertThat(decoded.options.pagingState).isNull();
    assertThat(decoded.options.serialConsistency)
        .isEqualTo(ProtocolConstants.ConsistencyLevel.SERIAL);
    assertThat(decoded.options.defaultTimestamp).isEqualTo(Long.MIN_VALUE);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_encode_and_decode_positional_values(int protocolVersion) {
    QueryOptions options = new QueryOptionsBuilder().positionalValue("0xcafebabe").build();
    Query initial = new Query(queryString, options);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .longString("select * from system.local")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.ONE)
                .byte_(0x01)
                .unsignedShort(1)
                .bytes("0xcafebabe") // count + list of values
            );

    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(4 + queryString.length() + 2 + 1 + 2 + 8);

    Query decoded = decode(encoded, protocolVersion);

    assertThat(decoded.query).isEqualTo(initial.query);
    assertThat(decoded.options.consistency).isEqualTo(ProtocolConstants.ConsistencyLevel.ONE);
    assertThat(decoded.options.positionalValues).containsExactly(Bytes.fromHexString("0xcafebabe"));
    assertThat(decoded.options.namedValues).isEmpty();
    assertThat(decoded.options.skipMetadata).isFalse();
    assertThat(decoded.options.pageSize).isEqualTo(-1);
    assertThat(decoded.options.pagingState).isNull();
    assertThat(decoded.options.serialConsistency)
        .isEqualTo(ProtocolConstants.ConsistencyLevel.SERIAL);
    assertThat(decoded.options.defaultTimestamp).isEqualTo(Long.MIN_VALUE);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_encode_non_default_options(int protocolVersion) {
    QueryOptions options =
        new QueryOptionsBuilder()
            .withSkipMetadata()
            .withPageSize(10)
            .withPagingState("0xcafebabe")
            .withSerialConsistency(ProtocolConstants.ConsistencyLevel.LOCAL_SERIAL)
            .withDefaultTimestamp(42)
            .build();
    Query initial = new Query(queryString, options);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .longString("select * from system.local")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.ONE)
                .byte_(0x02 | 0x04 | 0x08 | 0x10 | 0x20)
                .int_(10)
                .bytes("0xcafebabe")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.LOCAL_SERIAL)
                .long_(42));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(4 + queryString.length() + 2 + 1 + 4 + 8 + 2 + 8);

    Query decoded = decode(encoded, protocolVersion);

    assertThat(decoded.query).isEqualTo(initial.query);
    assertThat(decoded.options.consistency).isEqualTo(ProtocolConstants.ConsistencyLevel.ONE);
    assertThat(decoded.options.positionalValues).isEmpty();
    assertThat(decoded.options.namedValues).isEmpty();
    assertThat(decoded.options.skipMetadata).isTrue();
    assertThat(decoded.options.pageSize).isEqualTo(10);
    assertThat(decoded.options.pagingState).isEqualTo(Bytes.fromHexString("0xcafebabe"));
    assertThat(decoded.options.serialConsistency)
        .isEqualTo(ProtocolConstants.ConsistencyLevel.LOCAL_SERIAL);
    assertThat(decoded.options.defaultTimestamp).isEqualTo(42);
  }

  @Test(dataProviderClass = TestDataProviders.class, dataProvider = "protocolV3OrAbove")
  public void should_encode_named_values(int protocolVersion) {
    QueryOptions options = new QueryOptionsBuilder().namedValue("foo", "0xcafebabe").build();
    Query initial = new Query(queryString, options);

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .longString("select * from system.local")
                .unsignedShort(ProtocolConstants.ConsistencyLevel.ONE)
                .byte_(0x01 | 0x40)
                .unsignedShort(1)
                .string("foo")
                .bytes("0xcafebabe") // count + list of values
            );
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(4 + queryString.length() + 2 + 1 + 2 + (2 + "foo".length()) + 8);

    Query decoded = decode(encoded, protocolVersion);

    assertThat(decoded.query).isEqualTo(initial.query);
    assertThat(decoded.options.consistency).isEqualTo(ProtocolConstants.ConsistencyLevel.ONE);
    assertThat(decoded.options.positionalValues).isEmpty();
    assertThat(decoded.options.namedValues)
        .hasSize(1)
        .containsEntry("foo", Bytes.fromHexString("0xcafebabe"));
    assertThat(decoded.options.skipMetadata).isFalse();
    assertThat(decoded.options.pageSize).isEqualTo(-1);
    assertThat(decoded.options.pagingState).isNull();
    assertThat(decoded.options.serialConsistency)
        .isEqualTo(ProtocolConstants.ConsistencyLevel.SERIAL);
    assertThat(decoded.options.defaultTimestamp).isEqualTo(Long.MIN_VALUE);
  }

  @Test(
    dataProviderClass = TestDataProviders.class,
    dataProvider = "protocolV3OrAbove",
    expectedExceptions = IllegalArgumentException.class
  )
  public void should_not_allow_both_named_and_positional_values(int protocolVersion) {
    QueryOptions options =
        new QueryOptionsBuilder()
            .positionalValue("0xcafebabe")
            .namedValue("foo", "0xcafebabe")
            .build();
    Query query = new Query(queryString, options);

    encode(query, protocolVersion);
  }
}