/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.protocol.internal.request;

import static com.datastax.dse.protocol.internal.DseProtocolConstants.Version.DSE_V1;

import com.datastax.dse.protocol.internal.request.query.DseQueryOptions;
import com.datastax.dse.protocol.internal.request.query.DseQueryOptionsCodec;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.request.Query;

/**
 * {@code QUERY} codec for DSE-specific native protocols (DSE 5.1 and above).
 *
 * <p>Similar to the OSS codec, but in addition to regular messages, it can encode {@link
 * RawBytesQuery} (for fluent graph statements), and the options can be a {@link DseQueryOptions}.
 *
 * @see DseQueryCodecV4
 */
public class DseQueryCodec extends Query.Codec {

  public DseQueryCodec(int protocolVersion) {
    super(protocolVersion, new DseQueryOptionsCodec(protocolVersion));
    assert protocolVersion >= DSE_V1;
  }

  @Override
  public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
    if (message instanceof RawBytesQuery) {
      RawBytesQuery query = (RawBytesQuery) message;
      encoder.writeBytes(query.query, dest);
      optionsCodec.encode(dest, query.options, encoder);
    } else {
      super.encode(dest, message, encoder);
    }
  }

  @Override
  public int encodedSize(Message message) {
    if (message instanceof RawBytesQuery) {
      RawBytesQuery query = (RawBytesQuery) message;
      return PrimitiveSizes.sizeOfBytes(query.query) + optionsCodec.encodedSize(query.options);
    } else {
      return super.encodedSize(message);
    }
  }
}
