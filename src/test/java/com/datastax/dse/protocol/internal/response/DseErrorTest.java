/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.protocol.internal.response;

import static com.datastax.dse.protocol.internal.DseProtocolConstants.ErrorCode.CLIENT_WRITE_FAILURE;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.protocol.internal.DseProtocolConstants.ErrorCode;
import com.datastax.dse.protocol.internal.DseTestDataProviders;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.MessageTestBase;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.binary.MockBinaryString;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.Error.SingleMessageSubCodec;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class DseErrorTest extends MessageTestBase<Error> {

  private static final String MOCK_MESSAGE = "mock message";

  public DseErrorTest() {
    super(Error.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new Error.Codec(
        protocolVersion, new SingleMessageSubCodec(CLIENT_WRITE_FAILURE, protocolVersion));
  }

  @Test
  @UseDataProvider(location = DseTestDataProviders.class, value = "protocolDseV1OrAbove")
  public void should_encode_and_decode_client_write_failure(int protocolVersion) {
    int errorCode = ErrorCode.CLIENT_WRITE_FAILURE;
    Error initial = new Error(errorCode, MOCK_MESSAGE);
    MockBinaryString encoded = encode(initial, protocolVersion);
    assertThat(encoded).isEqualTo(new MockBinaryString().int_(errorCode).string(MOCK_MESSAGE));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(PrimitiveSizes.INT + (PrimitiveSizes.SHORT + MOCK_MESSAGE.length()));
    Error decoded = decode(encoded, protocolVersion);
    assertThat(decoded.code).isEqualTo(errorCode);
    assertThat(decoded.message).isEqualTo(MOCK_MESSAGE);
  }
}
