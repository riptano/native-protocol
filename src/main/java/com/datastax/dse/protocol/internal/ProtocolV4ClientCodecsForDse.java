/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.protocol.internal;

import com.datastax.dse.protocol.internal.request.DseQueryCodecV4;
import com.datastax.dse.protocol.internal.request.RawBytesQuery;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.AuthResponse;
import com.datastax.oss.protocol.internal.request.Batch;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.request.Register;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.AuthChallenge;
import com.datastax.oss.protocol.internal.response.AuthSuccess;
import com.datastax.oss.protocol.internal.response.Authenticate;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.Event;
import com.datastax.oss.protocol.internal.response.Ready;
import com.datastax.oss.protocol.internal.response.Result;
import com.datastax.oss.protocol.internal.response.Supported;

/**
 * DSE 5.0 still uses an OSS protocol version (V4), but we need to support {@link RawBytesQuery} for
 * fluent graph statements.
 *
 * @see DseQueryCodecV4
 */
public class ProtocolV4ClientCodecsForDse implements FrameCodec.CodecGroup {
  @Override
  public void registerCodecs(Registry registry) {
    registry
        .addEncoder(new AuthResponse.Codec(ProtocolConstants.Version.V4))
        .addEncoder(new Batch.Codec(ProtocolConstants.Version.V4))
        .addEncoder(new Execute.Codec(ProtocolConstants.Version.V4))
        .addEncoder(new Options.Codec(ProtocolConstants.Version.V4))
        .addEncoder(new Prepare.Codec(ProtocolConstants.Version.V4))
        .addEncoder(new DseQueryCodecV4(ProtocolConstants.Version.V4))
        .addEncoder(new Register.Codec(ProtocolConstants.Version.V4))
        .addEncoder(new Startup.Codec(ProtocolConstants.Version.V4));

    registry
        .addDecoder(new AuthChallenge.Codec(ProtocolConstants.Version.V4))
        .addDecoder(new Authenticate.Codec(ProtocolConstants.Version.V4))
        .addDecoder(new AuthSuccess.Codec(ProtocolConstants.Version.V4))
        .addDecoder(new Error.Codec(ProtocolConstants.Version.V4))
        .addDecoder(new Event.Codec(ProtocolConstants.Version.V4))
        .addDecoder(new Ready.Codec(ProtocolConstants.Version.V4))
        .addDecoder(new Result.Codec(ProtocolConstants.Version.V4))
        .addDecoder(new Supported.Codec(ProtocolConstants.Version.V4));
  }
}
