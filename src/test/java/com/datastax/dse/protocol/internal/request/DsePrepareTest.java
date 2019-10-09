/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.protocol.internal.request;

import static com.datastax.dse.protocol.internal.DseProtocolConstants.Version.DSE_V1;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.protocol.internal.DseTestDataProviders;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.MessageTestBase;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.binary.MockBinaryString;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class DsePrepareTest extends MessageTestBase<Prepare> {

  public DsePrepareTest() {
    super(Prepare.class);
  }

  @Override
  protected Message.Codec newCodec(int protocolVersion) {
    return new DsePrepareCodec(protocolVersion);
  }

  @Test
  public void should_encode_and_decode_without_flags_in_protocol_dse_v1() {
    Prepare initial = new Prepare("SELECT * FROM foo");

    MockBinaryString encoded = encode(initial, DSE_V1);

    assertThat(encoded).isEqualTo(new MockBinaryString().longString("SELECT * FROM foo"));
    assertThat(encodedSize(initial, DSE_V1))
        .isEqualTo(PrimitiveSizes.INT + "SELECT * FROM foo".length());

    Prepare decoded = decode(encoded, DSE_V1);

    assertThat(decoded.cqlQuery).isEqualTo(initial.cqlQuery);
  }

  @Test
  @UseDataProvider(location = DseTestDataProviders.class, value = "protocolDseV2OrAbove")
  public void should_encode_and_decode_without_keyspace_in_protocol_dse_v2_or_above(
      int protocolVersion) {
    Prepare initial = new Prepare("SELECT * FROM foo");

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(new MockBinaryString().longString("SELECT * FROM foo").int_(0x00));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo((PrimitiveSizes.INT + "SELECT * FROM foo".length()) + PrimitiveSizes.INT);

    Prepare decoded = decode(encoded, protocolVersion);

    assertThat(decoded.cqlQuery).isEqualTo(initial.cqlQuery);
    assertThat(decoded.keyspace).isEqualTo(initial.keyspace);
  }

  @Test
  @UseDataProvider(location = DseTestDataProviders.class, value = "protocolDseV2OrAbove")
  public void should_encode_and_decode_with_keyspace_in_protocol_dse_v2_or_above(
      int protocolVersion) {
    Prepare initial = new Prepare("SELECT * FROM foo", "ks");

    MockBinaryString encoded = encode(initial, protocolVersion);

    assertThat(encoded)
        .isEqualTo(new MockBinaryString().longString("SELECT * FROM foo").int_(0x01).string("ks"));
    assertThat(encodedSize(initial, protocolVersion))
        .isEqualTo(
            (PrimitiveSizes.INT + "SELECT * FROM foo".length())
                + PrimitiveSizes.INT
                + (PrimitiveSizes.SHORT + "ks".length()));

    Prepare decoded = decode(encoded, protocolVersion);

    assertThat(decoded.cqlQuery).isEqualTo(initial.cqlQuery);
    assertThat(decoded.keyspace).isEqualTo(initial.keyspace);
  }
}
