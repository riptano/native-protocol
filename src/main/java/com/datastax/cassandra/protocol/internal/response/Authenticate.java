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
package com.datastax.cassandra.protocol.internal.response;

import com.datastax.cassandra.protocol.internal.Message;
import com.datastax.cassandra.protocol.internal.PrimitiveCodec;
import com.datastax.cassandra.protocol.internal.PrimitiveSizes;
import com.datastax.cassandra.protocol.internal.ProtocolConstants;

public class Authenticate extends Message {
  public final String authenticator;

  public Authenticate(String authenticator) {
    super(true, ProtocolConstants.Opcode.AUTHENTICATE);
    this.authenticator = authenticator;
  }

  public static class Codec extends Message.Codec {
    public Codec(int protocolVersion) {
      super(ProtocolConstants.Opcode.AUTHENTICATE, protocolVersion);
    }

    @Override
    public <B> void encode(B dest, Message message, PrimitiveCodec<B> encoder) {
      Authenticate authenticate = (Authenticate) message;
      encoder.writeString(authenticate.authenticator, dest);
    }

    @Override
    public int encodedSize(Message message) {
      Authenticate authenticate = (Authenticate) message;
      return PrimitiveSizes.sizeOfString(authenticate.authenticator);
    }

    @Override
    public <B> Message decode(B source, PrimitiveCodec<B> decoder) {
      return new Authenticate(decoder.readString(source));
    }
  }
}
