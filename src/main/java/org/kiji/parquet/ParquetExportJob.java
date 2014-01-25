package org.kiji.parquet;

import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.avro.AvroParquetOutputFormat;

import org.kiji.mapreduce.KijiMapReduceJob;
import org.kiji.mapreduce.KijiMapper;
import org.kiji.mapreduce.framework.KijiConfKeys;
import org.kiji.mapreduce.framework.KijiTableInputFormat;
import org.kiji.schema.*;
import org.kiji.schema.layout.KijiTableLayout;

/**
 * Created by milo on 1/24/14.
 */
public class ParquetExportJob  extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetExportJob.class);

  /** A configuration key for transferring the schema to use for the output. */
  private static final String CONF_SCHEMA_KEY = "kiji.parquet.schema";

  /** A connection to the Kiji instance to retrieve our table. */
  private Kiji mKiji = null;

  /** A handle on our Kiji table. Necessary to retrieve its layout. */
  private KijiTable mTable = null;

  public static class ParquestExportMapper
      extends Mapper<EntityId, KijiRowData, Void, GenericRecord> {

  }

  @Override
  public int run(String[] args) throws Exception {
    Preconditions.checkArgument(2 == args.length, "Invalid number of arguments. Expected: "
        + "<kiji-uri> <output-path>");
    final KijiURI tableURI = KijiURI.newBuilder(args[0]).build();
    Preconditions.checkArgument(null != tableURI.getTable(), "Kiji URI must specify a table.");

    mKiji = Kiji.Factory.open(tableURI);
    mTable = mKiji.openTable(tableURI.getTable());

    final KijiDataRequest fullDataRequest = getFullDataRequest(mTable.getLayout());
    final Schema recordSchema = TableSchemaConverter.schemaForLayout(mTable.getLayout());

    // Configure the job to read from the Kiji table and write to Parquet.
    final Job job = new Job(HBaseConfiguration.create(), "Kiji Parquet Export");
    job.getConfiguration().set(KijiConfKeys.KIJI_INPUT_TABLE_URI, tableURI.toString());
    KijiTableInputFormat.configureJob(job, tableURI, fullDataRequest, null, null, null);
    job.setInputFormatClass(KijiTableInputFormat.class);
    job.getConfiguration().set(CONF_SCHEMA_KEY, recordSchema.toString());
    AvroParquetOutputFormat.setSchema(job, recordSchema);
  }

  /**
   * Helper method that, given a table layout, creates a data request designed to get the latest
   * version of every column.
   *
   * @param layout The table layout to produce a full request for.
   * @return a data request for every column, a maximum of one version in each.
   */
  private KijiDataRequest getFullDataRequest(KijiTableLayout layout) {
    KijiDataRequestBuilder builder = KijiDataRequest.builder();
    for (String familyName : layout.getFamilyMap().keySet()) {
      builder.newColumnsDef().addFamily(familyName);
    }
    return builder.build();
  }

  /**
   * The program's entry point.
   *
   * <pre>
   * USAGE:
   *
   *     ParquestExportJob &lt;kiji-uri&gt; &lt;output-path&gt;
   *
   * ARGUMENTS:
   *
   *     kiji-uri: kiji uri of the table to export.
   *
   *     output-path: The path to the output parquet files to generate.
   *
   * </pre>
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new ParquetExportJob(), args));
  }
}
