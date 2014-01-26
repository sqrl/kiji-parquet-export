package org.kiji.parquet;

import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import org.kiji.schema.*;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntegrationTestParquetExportJob extends AbstractKijiIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestParquetExportJob.class);

  @Test
  public void testParquetExportJob() throws Exception {
    final ParquetExportJob pej = new ParquetExportJob();
    final String[] args = new String[2];
    createAndPopulateFooTable();
    args[0] = KijiURI.newBuilder(getKijiURI()).withTableName("foo").build().toString();
    args[1] = getTempDir().toString() + "parquet-out";
    int res = ToolRunner.run(pej, args);

    // TODO: Open up the output file and make sure it's readable Parquet.
    assert(res == 0);
  }
}
