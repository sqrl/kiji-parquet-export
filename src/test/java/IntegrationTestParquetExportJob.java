import org.junit.Test;
import org.kiji.schema.*;
import org.kiji.schema.layout.KijiTableLayouts;
import org.kiji.schema.testutil.AbstractKijiIntegrationTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by milo on 1/24/14.
 */
public class IntegrationTestParquetExportJob extends AbstractKijiIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestParquetExportJob.class);

  @Test
  public void testEmptyDataRequest() throws Exception {
    createAndPopulateFooTable();

    KijiTable foo = Kiji.Factory.open(getKijiURI()).openTable("foo");
    KijiTableReader reader = foo.openTableReader();

    KijiRowScanner scanner = reader.getScanner(KijiDataRequest.builder().build());
    LOG.info("*** Printing out table Contents ***");
    for (KijiRowData data : scanner) {
      LOG.info(data.toString());
    }
    scanner.close();
    reader.close();
    foo.release();
  }
}
