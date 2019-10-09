/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.protocol.internal.request;

import com.datastax.dse.protocol.internal.request.query.DseQueryOptions;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.util.Bytes;

/**
 * A specialized {@code QUERY} message where the query string is represented directly as a byte
 * array.
 *
 * <p>It is used to avoid materializing a string if the incoming query is already encoded (namely,
 * in DSE graph).
 */
public class RawBytesQuery extends Message {

  public final byte[] query;
  public final DseQueryOptions options;

  public RawBytesQuery(byte[] query, DseQueryOptions options) {
    super(false, ProtocolConstants.Opcode.QUERY);
    this.query = query;
    this.options = options;
  }

  @Override
  public String toString() {
    return "QUERY (" + Bytes.toHexString(query) + ')';
  }
}
