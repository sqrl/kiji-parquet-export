package org.kiji.parquet;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import org.apache.avro.specific.SpecificRecord;
import org.kiji.annotations.ApiAudience;
import org.kiji.schema.KijiIOException;
import org.kiji.schema.avro.AvroSchema;
import org.kiji.schema.avro.CellSchema;
import org.kiji.schema.layout.CellSpec;
import org.kiji.schema.layout.InvalidLayoutException;
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
   * every cell.  It has an additional special record named '_rowKey' to hold a representation
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
    fieldsBuilder = fieldsBuilder.name("_rowKey").type().bytesType().noDefault();
    for (KijiTableLayout.LocalityGroupLayout.FamilyLayout fam : layout.getFamilies()) {
      if (fam.isMapType()) {
        Schema mapSchema = getSchemaFromCellSchema(fam.getDesc().getMapSchema());
        fieldsBuilder =
            fieldsBuilder.name(fam.getName()).type().nullable().map().values(mapSchema).noDefault();
      } else {
        assert(fam.isGroupType());
        for (KijiTableLayout.LocalityGroupLayout.FamilyLayout.ColumnLayout col : fam.getColumns()) {
          String fieldName = String.format("%s_%s", fam.getName(), col.getName());
          Schema type = getSchemaFromCellSchema(col.getDesc().getColumnSchema());
          fieldsBuilder = fieldsBuilder.name(fieldName).type(type).noDefault();
        }
      }
    }
    return fieldsBuilder.endRecord();
  }

  /**
   * Retrieves the relevant Schema from a CellSchema by either parsing it from the relevant String,
   * or retrieving it from the passed in SchemaTable.
   * TODO: This does not currently support Avro records that refer to their default reader by id.
   *
   * @param cellSchema that defines the desired Schema.
   * @return Schema referenced by the CellSchema.
   */
  private static Schema getSchemaFromCellSchema(CellSchema cellSchema) {
    Schema.Parser parser = new Schema.Parser();
    switch (cellSchema.getType()) {
      case INLINE:
        return parser.parse(cellSchema.getValue());
      case AVRO:
        AvroSchema avroSchema = cellSchema.getDefaultReader();
        if (avroSchema.getJson() != null) {
          return parser.parse(avroSchema.getJson());
        }
        throw new KijiIOException("Unable to find Schema for AVRO CellSchema.");
      case CLASS:
        String className = cellSchema.getValue();
        try {
          SpecificRecord clazz = (SpecificRecord) Class.forName(className).newInstance();
          return clazz.getSchema();
        } catch (Exception e) {
          throw new KijiIOException("Unable to find/instantiate class: " + className, e);
        }
      case COUNTER:
        return Schema.create(Schema.Type.LONG);
      case RAW_BYTES:
        return Schema.create(Schema.Type.BYTES);
      default:
        throw new UnsupportedOperationException(
            "CellSchema " + cellSchema.getType() + " unsupported.");
    }
  }
}
