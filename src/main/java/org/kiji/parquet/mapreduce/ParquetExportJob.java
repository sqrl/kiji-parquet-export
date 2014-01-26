package org.kiji.parquet.mapreduce;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.avro.AvroParquetOutputFormat;

import org.kiji.mapreduce.KijiMapReduceJob;
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

  /** A configuration key for a list of group families in the table. */
  private static final String CONF_GROUP_FAMILIES_KEY = "kiji.parquet.group_families";

  /** A configuration key for a list of map families in the table. */
  private static final String CONF_MAP_FAMILIES_KEY = "kiji.parquet.map_families";

  /** A connection to the Kiji instance to retrieve our table. */
  private Kiji mKiji = null;

  /** A handle on our Kiji table. Necessary to retrieve its layout. */
  private KijiTable mTable = null;

  /**
   * A mapper that goes through every row of the kiji table and writes them into a GenericRecord.
   * Designed to be used with the KijiInputFormat.
   */
  public static class ParquestExportMapper
      extends Mapper<EntityId, KijiRowData, Void, GenericRecord> {
    /** The schema to use for our output.  Will be reconstructed from Configuration. */
    Schema mSchema = null;

    /** An array of groupfamily names in the table. Retrieved from Configuration. */
    String[] mGroupFamilyNames = null;

    /** An array of mapfamily names in the table. Retrieved from Configuration. */
    String[] mMapFamilyNames = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
      mSchema = new Schema.Parser().parse(context.getConfiguration().get(CONF_SCHEMA_KEY));
      mGroupFamilyNames = context.getConfiguration().getStrings(CONF_GROUP_FAMILIES_KEY);
      mMapFamilyNames = context.getConfiguration().getStrings(CONF_MAP_FAMILIES_KEY);
    }

    @Override
    protected void map(EntityId key, KijiRowData value, Mapper.Context context)
        throws IOException, InterruptedException {
      GenericRecordBuilder builder = new GenericRecordBuilder(mSchema);
      // TODO: It would be nice if this translation were kinder.
      builder.set("_rowKey", ByteBuffer.wrap(key.getHBaseRowKey()));
      // Group families
      for (String family : mGroupFamilyNames) {
        for (String qualifier : value.getQualifiers(family)) {
          String field = TableSchemaConverter.fieldForColumn(family, qualifier);
          builder.set(field, value.getMostRecentValue(family, qualifier));
        }
      }
      // Map families
      for (String family : mMapFamilyNames) {
        if (value.containsColumn(family)) {
          LOG.info("*** {} Contains Data ***", family);
          builder.set(family, value.getMostRecentValues(family));
        } else {
          LOG.info("!!! {} Does Not Contain Data !!!", family);
        }
      }

      context.write(null, builder.build());
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Preconditions.checkArgument(2 == args.length, "Invalid number of arguments. Expected: "
        + "<kiji-uri> <output-path>");
    final KijiURI tableURI = KijiURI.newBuilder(args[0]).build();
    Preconditions.checkArgument(null != tableURI.getTable(), "Kiji URI must specify a table.");
    final Path outputPath = new Path(args[1]);

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
    setFamilyNames(mTable.getLayout(), job.getConfiguration());
    job.setOutputFormatClass(AvroParquetOutputFormat.class);
    AvroParquetOutputFormat.setSchema(job, recordSchema);
    AvroParquetOutputFormat.setOutputPath(job, outputPath);
    job.setMapperClass(ParquestExportMapper.class);
    job.setNumReduceTasks(0);

    KijiMapReduceJob kijiMRJob = KijiMapReduceJob.create(job);

    LOG.info("Running job....");
    boolean success = kijiMRJob.run();

    mTable.release();
    mKiji.release();

    return success ? 0 : 1;
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
   * Helper function that, given a table layout, sets the Configuration flags for its group and
   * map families.
   *
   * @param layout The table layout.
   * @param conf The configuration where keys should be set.
   */
  private void setFamilyNames(KijiTableLayout layout, Configuration conf) {
    final List<String> groupFamilies = new ArrayList<String>();
    final List<String> mapFamilies = new ArrayList<String>();

    // TODO: Do this in a more functional style, possibly with guava.
    for (KijiTableLayout.LocalityGroupLayout.FamilyLayout familyLayout : layout.getFamilies()) {
      if (familyLayout.isGroupType()) {
        groupFamilies.add(familyLayout.getName());
      } else {
        mapFamilies.add(familyLayout.getName());
      }
    }

    conf.setStrings(CONF_GROUP_FAMILIES_KEY,
        groupFamilies.toArray(new String[groupFamilies.size()]));
    conf.setStrings(CONF_MAP_FAMILIES_KEY,
        mapFamilies.toArray(new String[mapFamilies.size()]));
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
