package com.klimber.hiveddbudtf.hive;

import com.google.common.collect.ImmutableList;
import com.klimber.hiveddbudtf.hive.HiveDdbQueryParameters.ColumnMapping;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardConstantStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

public class HiveDdbQueryParametersConverter implements Converter {
    public static final String TABLE_NAME = "tableName";
    public static final String INDEX_NAME = "indexName";
    public static final String HIVE_DDB_COLUMN_MAPPING = "hiveDdbColumnMapping";
    public static final String HIVE_TYPE_MAPPING = "hiveTypeMapping";
    public static final String MAPPING_DIVIDER = ",";
    public static final String MAPPING_SEPARATOR = ":";

    private final StructObjectInspector paramsOI;
    private final StructField tableNameField;
    private final StructField indexNameField;
    private final StructField hiveDdbColumnMappingField;
    private final StructField hiveTypeMappingField;
    @Getter
    private final List<ColumnMapping> hiveDdbColumnMapping;
    @Getter
    private final List<TypeInfo> hiveTypes;

    public HiveDdbQueryParametersConverter(StructObjectInspector paramsOI) throws UDFArgumentException {
        this.paramsOI = paramsOI;

        this.tableNameField = this.paramsOI.getStructFieldRef(TABLE_NAME);
        this.assertPrimitive(this.tableNameField);

        this.indexNameField = this.paramsOI.getStructFieldRef(INDEX_NAME);
        this.assertPrimitive(this.indexNameField);

        this.hiveDdbColumnMappingField = this.paramsOI.getStructFieldRef(HIVE_DDB_COLUMN_MAPPING);
        this.assertPrimitive(this.hiveDdbColumnMappingField);
        this.assertConstant(this.paramsOI, this.hiveDdbColumnMappingField);
        this.hiveDdbColumnMapping = this.getMappings(this.fetchConstant(this.paramsOI, this.hiveDdbColumnMappingField));

        this.hiveTypeMappingField = this.paramsOI.getStructFieldRef(HIVE_TYPE_MAPPING);
        this.assertPrimitive(this.hiveTypeMappingField);
        this.assertConstant(this.paramsOI, this.hiveTypeMappingField);
        this.hiveTypes = this.getTypeInfo(this.fetchConstant(this.paramsOI, this.hiveTypeMappingField));
        if (this.hiveDdbColumnMapping.size() != this.hiveTypes.size()) {
            throw new IllegalArgumentException("Hive column type mappings and hive Ddb column mappings "
                                               + "should contain the same number of columns");
        }
    }

    @Override
    public HiveDdbQueryParameters convert(Object input) {
        String tableName = this.fetchStringField(input, this.tableNameField);
        Objects.requireNonNull(tableName, "Expected field '" + this.tableNameField.getFieldName() + "' to not be null");
        String indexName = this.fetchStringField(input, this.indexNameField);
        return HiveDdbQueryParameters.builder()
                                     .tableName(tableName)
                                     .indexName(indexName)
                                     .hiveDdbColumnMapping(this.hiveDdbColumnMapping)
                                     .hiveTypes(this.hiveTypes)
                                     .build();
    }

    private void assertPrimitive(StructField field) throws UDFArgumentException {
        ObjectInspector fieldOI = field.getFieldObjectInspector();
        if (!(fieldOI instanceof PrimitiveObjectInspector)) {
            String msg = String.format("Expected query parameter field to be primitive (name=%s, found=%s)",
                                       field.getFieldName(), fieldOI.getTypeName());
            throw new UDFArgumentException(msg);
        }
    }

    private void assertConstant(StructObjectInspector structOI, StructField field) throws UDFArgumentException {
        if (structOI instanceof ConstantObjectInspector) {
            return;
        }
        ObjectInspector fieldOI = field.getFieldObjectInspector();
        if (fieldOI instanceof ConstantObjectInspector) {
            return;
        }
        String msg = String.format("Expected query parameter field to be constant (name=%s, found=%s)",
                                   field.getFieldName(), fieldOI.getClass().getSimpleName());
        throw new UDFArgumentException(msg);
    }

    private String fetchConstant(StructObjectInspector structOI, StructField field) {
        String value;
        if (structOI instanceof ConstantObjectInspector) {
            StandardConstantStructObjectInspector constStruct = (StandardConstantStructObjectInspector) structOI;
            Object constValue = constStruct.getWritableConstantValue().get(field.getFieldID());
            value = PrimitiveObjectInspectorUtils.getString(constValue,
                                                            (PrimitiveObjectInspector) field.getFieldObjectInspector());
        } else {
            ConstantObjectInspector fieldOI = (ConstantObjectInspector) field.getFieldObjectInspector();
            value = PrimitiveObjectInspectorUtils.getString(fieldOI.getWritableConstantValue(),
                                                            (PrimitiveObjectInspector) fieldOI);
        }
        Objects.requireNonNull(value, "Expected field '" + field.getFieldName() + "' to not be null");
        return value;
    }

    private List<ColumnMapping> getMappings(String mappingData) {
        ImmutableList.Builder<ColumnMapping> mappingsBuilder = ImmutableList.builder();
        Arrays.stream(mappingData.split(MAPPING_DIVIDER))
              .map(e -> e.split(MAPPING_SEPARATOR))
              .peek(this::validateMapping)
              .map(e -> ColumnMapping.builder().hiveColumn(e[0]).ddbAttName(e[1]).build())
              .forEach(mappingsBuilder::add);
        return mappingsBuilder.build();
    }

    private void validateMapping(String[] e) {
        if (e.length != 2) {
            String msg = String.format("Each hive to DynamoDB column mapping should contain 2 column names "
                                       + "separated by '%s' (found=%d)", MAPPING_SEPARATOR, e.length);
            throw new IllegalArgumentException(msg);
        }
        if (StringUtils.isEmpty(e[0]) || StringUtils.isBlank(e[1])) {
            throw new IllegalArgumentException("Each hive to DynamoDB column mapping should contain at least "
                                               + "one non whitespace character");
        }
    }

    private List<TypeInfo> getTypeInfo(String typeData) {
        return TypeInfoUtils.getTypeInfosFromTypeString(typeData);
    }

    private String fetchStringField(Object input, StructField field) {
        Object fieldData = this.paramsOI.getStructFieldData(input, field);
        PrimitiveObjectInspector tableNameOI = (PrimitiveObjectInspector) field.getFieldObjectInspector();
        return PrimitiveObjectInspectorUtils.getString(fieldData, tableNameOI);
    }
}
