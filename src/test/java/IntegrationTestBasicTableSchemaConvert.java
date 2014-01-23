import org.apache.avro.Schema;
import org.junit.Before;
import org.junit.Test;

import org.kiji.parquet.TableSchemaConverter;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.layout.KijiTableLayout;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;

/**
 * Created by sqrl on 12/18/13.
 */
public class IntegrationTestBasicTableSchemaConvert extends AbstractKijiIntegrationTest {
  @Test
  public void testConverter() throws Exception {
    createAndPopulateFooTable();
    Kiji kiji = Kiji.Factory.open(getKijiURI(), getIntegrationHelper().getConf());
    KijiTable kijiTable = kiji.openTable("foo");
    Schema schema = TableSchemaConverter.schemaForLayout(kijiTable.getLayout());
    System.out.println(schema.toString(true));

    kiji.createTable(KijiTableLayout.createFromEffectiveJsonResource());
  }
}
