/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.protocol.internal.response.result;

import static com.datastax.dse.protocol.internal.DseProtocolConstants.Version.DSE_V1;
import static com.datastax.dse.protocol.internal.DseProtocolConstants.Version.DSE_V2;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.Result;
import com.datastax.oss.protocol.internal.response.result.Prepared;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;

public class DsePreparedSubCodec extends Result.SubCodec {

  public DsePreparedSubCodec(int protocolVersion) {
    super(ProtocolConstants.ResultKind.PREPARED, protocolVersion);
    assert protocolVersion >= DSE_V1;
  }

  @Override
  public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
    Prepared prepared = (Prepared) message;
    encoder.writeShortBytes(prepared.preparedQueryId, dest);
    if (protocolVersion >= DSE_V2) {
      encoder.writeShortBytes(prepared.resultMetadataId, dest);
    }
    prepared.variablesMetadata.encode(dest, encoder, true, protocolVersion);
    prepared.resultMetadata.encode(dest, encoder, false, protocolVersion);
  }

  @Override
  public int encodedSize(Message message) {
    Prepared prepared = (Prepared) message;
    int size = PrimitiveSizes.sizeOfShortBytes(prepared.preparedQueryId);
    if (protocolVersion >= DSE_V2) {
      assert prepared.resultMetadataId != null;
      size += PrimitiveSizes.sizeOfShortBytes(prepared.resultMetadataId);
    }
    size += prepared.variablesMetadata.encodedSize(true, protocolVersion);
    size += prepared.resultMetadata.encodedSize(false, protocolVersion);
    return size;
  }

  @Override
  public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
    byte[] preparedQueryId = decoder.readShortBytes(source);
    byte[] resultMetadataId = (protocolVersion >= DSE_V2) ? decoder.readShortBytes(source) : null;
    RowsMetadata variablesMetadata = RowsMetadata.decode(source, decoder, true, protocolVersion);
    RowsMetadata resultMetadata = RowsMetadata.decode(source, decoder, false, protocolVersion);
    return new Prepared(preparedQueryId, resultMetadataId, variablesMetadata, resultMetadata);
  }
}
