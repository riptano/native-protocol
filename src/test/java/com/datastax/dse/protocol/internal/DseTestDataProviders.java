/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.protocol.internal;

import com.datastax.oss.protocol.internal.TestDataProviders;
import com.tngtech.java.junit.dataprovider.DataProvider;
import java.util.ArrayList;
import java.util.List;

public class DseTestDataProviders {
  @DataProvider
  public static Object[][] protocolDseV1OrAbove() {
    return protocolVersions(null, null);
  }

  @DataProvider
  public static Object[][] protocolDseV2OrAbove() {
    return protocolVersions(DseProtocolConstants.Version.DSE_V2, null);
  }

  /**
   * @param min inclusive
   * @param max inclusive
   */
  private static Object[][] protocolVersions(Integer min, Integer max) {
    if (min == null) {
      min = DseProtocolConstants.Version.MIN;
    }
    if (max == null) {
      max = Math.max(DseProtocolConstants.Version.MAX, DseProtocolConstants.Version.BETA);
    }
    List<Object> l = new ArrayList<>();
    for (int i = min; i <= max; i++) {
      l.add(i);
    }
    return TestDataProviders.fromList(l);
  }
}
