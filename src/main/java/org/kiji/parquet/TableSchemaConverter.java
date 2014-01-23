package org.kiji.parquet;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import org.kiji.annotations.ApiAudience;
import org.kiji.schema.layout.KijiTableLayout;


@ApiAudience.Private
/**
 * Class containing static methods that convert a table layout into a suitable avro record for
 * parquet-avro-mr.
 *
 * TODO: Make this use a builder pattern instead if it grows great in complexity.
 */
public class TableSchemaConverter {
  /**
   * Static method that creates a schema suitable for an parquet-mr avro writer from a
   * KijiTableLayout.
   *
   * <p>The avro record represents a single row of a kiji table, with a single version of
   * every cell.  It has an additional special record named '_columnName' to hold a representation
   * of the column as bytes.</p>
   *
   * <p>A map type family named 'foo' in the KijiTableLayout is mapped into the parquet avro record
   * (and resultant parquet file) as a field named foo mapping strings (columns in the map family)
   * to the schema type stored in the kiji map family.</p>
   *
   * <p>A group family named 'bar' and a column inside it named 'baz' will be mapped to a field in
   * the parquet record named 'bar:baz' with the type of the 'baz' column.</p>
   *
   * @param layout A KijiTableLayout for the table to convert to a parquet entry.
   * @return A Schema suitable for passing to the parquet--mr-avro output format.
   */
  public static Schema schemaForLayout(KijiTableLayout layout) {
    SchemaBuilder.FieldAssembler<Schema> fieldsBuilder =
        SchemaBuilder.record(layout.getName()).fields();
    fieldsBuilder = fieldsBuilder.name("_columnName").type().bytesType().noDefault();
    for (KijiTableLayout.LocalityGroupLayout.FamilyLayout fam : layout.getFamilies()) {
      if (fam.isMapType()) {
        Schema mapSchema = fam.getDesc().getSchema();
        fieldsBuilder =
            fieldsBuilder.name(fam.getName()).type().nullable().map().values(mapSchema).noDefault();
      } else {
        assert(fam.isGroupType());
        for (KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout col : fam.getColumns()) {
          String fieldName = String.format("%s_%s", fam.getName(), col.getName());
          Schema type = col.getDesc().getSchema();
          fieldsBuilder = fieldsBuilder.name(fieldName).type(type).noDefault();
        }
      }
    }
    return fieldsBuilder.endRecord();
  }
}
