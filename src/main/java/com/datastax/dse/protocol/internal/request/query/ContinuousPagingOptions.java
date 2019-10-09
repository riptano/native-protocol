/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.protocol.internal.request.query;

public class ContinuousPagingOptions {

  public final int maxPages;
  public final int pagesPerSecond;
  public final int nextPages;

  public ContinuousPagingOptions(int maxPages, int pagesPerSecond, int nextPages) {
    this.maxPages = maxPages;
    this.pagesPerSecond = pagesPerSecond;
    this.nextPages = nextPages;
  }
}
