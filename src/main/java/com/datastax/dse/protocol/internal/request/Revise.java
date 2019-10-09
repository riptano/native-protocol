/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.protocol.internal.request;

import com.datastax.dse.protocol.internal.DseProtocolConstants;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.PrimitiveSizes;

public class Revise extends Message {

  public static Revise cancelContinuousPaging(int streamId) {
    return new Revise(DseProtocolConstants.RevisionType.CANCEL_CONTINUOUS_PAGING, streamId, -1);
  }

  public static Revise requestMoreContinuousPages(int streamId, int amount) {
    return new Revise(DseProtocolConstants.RevisionType.MORE_CONTINUOUS_PAGES, streamId, amount);
  }

  /** @see DseProtocolConstants.RevisionType */
  public final int revisionType;

  public final int streamId;
  public final int nextPages;

  public Revise(int revisionType, int streamId, int nextPages) {
    super(false, DseProtocolConstants.Opcode.REVISE_REQUEST);
    this.revisionType = revisionType;
    this.streamId = streamId;
    this.nextPages = nextPages;
  }

  public static class Codec extends Message.Codec {

    public Codec(int protocolVersion) {
      super(DseProtocolConstants.Opcode.REVISE_REQUEST, protocolVersion);
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
      Revise revise = (Revise) message;
      encoder.writeInt(revise.revisionType, dest);
      encoder.writeInt(revise.streamId, dest);
      if (revise.revisionType == DseProtocolConstants.RevisionType.MORE_CONTINUOUS_PAGES) {
        encoder.writeInt(revise.nextPages, dest);
      }
    }

    @Override
    public int encodedSize(Message message) {
      Revise revise = (Revise) message;
      return PrimitiveSizes.INT
          * (revise.revisionType == DseProtocolConstants.RevisionType.MORE_CONTINUOUS_PAGES
              ? 3
              : 2);
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      int revisionType = decoder.readInt(source);
      int streamId = decoder.readInt(source);
      int nextPages =
          (revisionType == DseProtocolConstants.RevisionType.MORE_CONTINUOUS_PAGES)
              ? decoder.readInt(source)
              : -1;
      return new Revise(revisionType, streamId, nextPages);
    }
  }
}
