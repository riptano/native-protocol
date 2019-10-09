/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.protocol.internal.request;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.protocol.internal.DseProtocolConstants;
import com.datastax.dse.protocol.internal.DseTestDataProviders;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.MessageTestBase;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.binary.MockBinaryString;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class ReviseTest extends MessageTestBase<Revise> {

  public ReviseTest() {
    super(Revise.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new Revise.Codec(protocolVersion);
  }

  @Test
  @UseDataProvider(location = DseTestDataProviders.class, value = "protocolDseV1OrAbove")
  public void should_encode_and_decode_cancel_continuous_paging(int protocolVersion) {
    Revise initial = Revise.cancelContinuousPaging(42);
    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .int_(DseProtocolConstants.RevisionType.CANCEL_CONTINUOUS_PAGING)
                .int_(42));
    assertThat(encodedSize(initial, protocolVersion)).isEqualTo(PrimitiveSizes.INT * 2);

    Revise decoded = decode(encoded, protocolVersion);

    assertThat(decoded.revisionType)
        .isEqualTo(DseProtocolConstants.RevisionType.CANCEL_CONTINUOUS_PAGING);
    assertThat(decoded.streamId).isEqualTo(42);
    assertThat(decoded.nextPages).isEqualTo(-1);
  }

  @Test
  @UseDataProvider(location = DseTestDataProviders.class, value = "protocolDseV2OrAbove")
  public void should_encode_and_decode_request_for_more_pages_in_dse_v2_and_above(
      int protocolVersion) {
    Revise initial = Revise.requestMoreContinuousPages(42, 10);
    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(
            new MockBinaryString()
                .int_(DseProtocolConstants.RevisionType.MORE_CONTINUOUS_PAGES)
                .int_(42)
                .int_(10));
    assertThat(encodedSize(initial, protocolVersion)).isEqualTo(PrimitiveSizes.INT * 3);

    Revise decoded = decode(encoded, protocolVersion);

    assertThat(decoded.revisionType)
        .isEqualTo(DseProtocolConstants.RevisionType.MORE_CONTINUOUS_PAGES);
    assertThat(decoded.streamId).isEqualTo(42);
    assertThat(decoded.nextPages).isEqualTo(10);
  }
}
