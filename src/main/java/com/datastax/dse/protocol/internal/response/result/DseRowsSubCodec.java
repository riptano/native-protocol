/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.protocol.internal.response.result;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.response.result.DefaultRows;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class DseRowsSubCodec extends DefaultRows.SubCodec {

  public DseRowsSubCodec(int protocolVersion) {
    super(protocolVersion);
  }

  // No need to override `encode` and `encodedSize`, if the metadata is a DseRowsMetadata it knows
  // how to encode itself.

  @Override
  public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
    DseRowsMetadata metadata = DseRowsMetadata.decode(source, decoder, false, protocolVersion);
    int rowCount = decoder.readInt(source);

    Queue<List<ByteBuffer>> data = new ArrayDeque<>(rowCount);
    for (int i = 0; i < rowCount; i++) {
      List<ByteBuffer> row = new ArrayList<>(metadata.columnCount);
      for (int j = 0; j < metadata.columnCount; j++) {
        row.add(decoder.readBytes(source));
      }
      data.add(row);
    }

    return new DefaultRows(metadata, data);
  }
}
