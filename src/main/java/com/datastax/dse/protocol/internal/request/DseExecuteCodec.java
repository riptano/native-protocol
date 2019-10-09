/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.protocol.internal.request;

import static com.datastax.dse.protocol.internal.DseProtocolConstants.Version.DSE_V2;

import com.datastax.dse.protocol.internal.DseProtocolConstants;
import com.datastax.dse.protocol.internal.request.query.DseQueryOptionsCodec;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;

public class DseExecuteCodec extends Message.Codec {

  private final QueryOptions.Codec optionsCodec;

  public DseExecuteCodec(int protocolVersion) {
    super(ProtocolConstants.Opcode.EXECUTE, protocolVersion);
    assert protocolVersion >= DseProtocolConstants.Version.DSE_V1;
    this.optionsCodec = new DseQueryOptionsCodec(protocolVersion);
  }

  @Override
  public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
    Execute execute = (Execute) message;
    encoder.writeShortBytes(execute.queryId, dest);
    if (protocolVersion >= DSE_V2) {
      encoder.writeShortBytes(execute.resultMetadataId, dest);
    }
    optionsCodec.encode(dest, execute.options, encoder);
  }

  @Override
  public int encodedSize(Message message) {
    Execute execute = (Execute) message;
    int size = PrimitiveSizes.sizeOfShortBytes(execute.queryId);
    if (protocolVersion >= DSE_V2) {
      assert execute.resultMetadataId != null;
      size += PrimitiveSizes.sizeOfShortBytes(execute.resultMetadataId);
    }
    size += optionsCodec.encodedSize(execute.options);
    return size;
  }

  @Override
  public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
    byte[] queryId = decoder.readShortBytes(source);
    byte[] resultMetadataId = (protocolVersion >= DSE_V2) ? decoder.readShortBytes(source) : null;
    QueryOptions options = optionsCodec.decode(source, decoder);
    return new Execute(queryId, resultMetadataId, options);
  }
}
