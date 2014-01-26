package org.kiji.parquet;

import org.apache.avro.Schema;
import org.junit.Test;

import org.kiji.parquet.TableSchemaConverter;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;

/**
 * Created by sqrl on 12/18/13.
 */
public class TestBasicTableSchemaConvert extends AbstractKijiIntegrationTest {
  @Test
  public void testSimpleConverter() throws Exception {
    KijiTableLayout layout =
        KijiTableLayouts.getTableLayout(KijiTableLayouts.FOODS);
    Schema schema = TableSchemaConverter.schemaForLayout(layout);
    System.out.println(schema.toString(true));
  }
}