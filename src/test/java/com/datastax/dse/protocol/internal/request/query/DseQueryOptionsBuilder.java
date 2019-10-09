/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.protocol.internal.request.query;

import com.datastax.oss.protocol.internal.request.query.QueryOptionsBuilderBase;

public class DseQueryOptionsBuilder
    extends QueryOptionsBuilderBase<DseQueryOptions, DseQueryOptionsBuilder> {

  protected boolean isPageSizeInBytes;
  protected boolean hasContinuousPagingOptions;
  protected int maxPages;
  protected int pagesPerSecond;
  protected int nextPages;

  public DseQueryOptionsBuilder withPageSizeInBytes() {
    this.isPageSizeInBytes = true;
    this.hasContinuousPagingOptions = true;
    return this;
  }

  public DseQueryOptionsBuilder withMaxPages(int maxPages) {
    this.maxPages = maxPages;
    this.hasContinuousPagingOptions = true;
    return this;
  }

  public DseQueryOptionsBuilder withPagesPerSecond(int pagesPerSecond) {
    this.pagesPerSecond = pagesPerSecond;
    this.hasContinuousPagingOptions = true;
    return this;
  }

  public DseQueryOptionsBuilder withNextPages(int nextPages) {
    this.nextPages = nextPages;
    return this;
  }

  @Override
  public DseQueryOptions build() {
    return new DseQueryOptions(
        consistency,
        positionalValues,
        namedValues,
        skipMetadata,
        pageSize,
        pagingState,
        serialConsistency,
        defaultTimestamp,
        keyspace,
        isPageSizeInBytes,
        hasContinuousPagingOptions
            ? new ContinuousPagingOptions(maxPages, pagesPerSecond, nextPages)
            : null);
  }
}
