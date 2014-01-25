package org.kiji.parquet.gather;

import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.mapreduce.gather.KijiGatherer;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;

import java.io.IOException;

/**
 * Created by milo on 1/24/14.
 */
public class ParquetExportGatherer extends KijiGatherer {
  /** Our data request is just the default, full datarequest of one version of each column. */
  @Override
  public KijiDataRequest getDataRequest() {
    return KijiDataRequest.builder().build();
  }

  @Override
  public void gather(KijiRowData input, GathererContext context) throws IOException {

  }

  @Override
  public Class<?> getOutputKeyClass() {
    return null;
  }

  @Override
  public Class<?> getOutputValueClass() {
    return null;
  }
}
