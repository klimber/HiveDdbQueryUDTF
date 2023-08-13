package com.klimber.hiveddbudtf.hive;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import java.util.List;
import java.util.Objects;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.STRUCT;

public class HiveDdbQueryFiltersConverter implements Converter {
    public static final String ATTRIBUTE = "attribute";
    public static final String ATTRIBUTE_TYPE = "attributeType";
    public static final String OPERATOR = "operator";
    public static final String VALUE = "value";

    private final StructObjectInspector filtersOI;
    private final StructObjectInspector[] filterOI;
    private final StructField[] attributeField;
    private final StructField[] attributeTypeField;
    private final StructField[] operatorField;
    private final StructField[] valueField;

    public HiveDdbQueryFiltersConverter(StructObjectInspector filtersOI) throws UDFArgumentException {
        this.filtersOI = filtersOI;
        List<? extends StructField> filters = this.filtersOI.getAllStructFieldRefs();
        this.filterOI = new StructObjectInspector[filters.size()];
        this.attributeField = new StructField[filters.size()];
        this.attributeTypeField = new StructField[filters.size()];
        this.operatorField = new StructField[filters.size()];
        this.valueField = new StructField[filters.size()];
        for (int i = 0; i < filters.size(); i++) {
            StructField filter = filters.get(i);
            ObjectInspector oi = filter.getFieldObjectInspector();
            if (!STRUCT.equals(oi.getCategory())) {
                String msg = String.format("Expected filter list field to be struct (found=%s)",
                                           oi.getTypeName());
                throw new UDFArgumentException(msg);
            }
            this.filterOI[i] = (StructObjectInspector) oi;

            this.attributeField[i] = this.filterOI[i].getStructFieldRef(ATTRIBUTE);
            this.assertPrimitive(this.attributeField[i]);

            this.attributeTypeField[i] = this.filterOI[i].getStructFieldRef(ATTRIBUTE_TYPE);
            this.assertPrimitive(this.attributeTypeField[i]);

            this.operatorField[i] = this.filterOI[i].getStructFieldRef(OPERATOR);
            this.assertPrimitive(this.operatorField[i]);

            this.valueField[i] = this.filterOI[i].getStructFieldRef(VALUE);
            this.assertPrimitive(this.valueField[i]);
        }
    }

    @Override
    public Multimap<String, HiveDdbQueryFilter> convert(Object input) {
        List<Object> filterList = this.filtersOI.getStructFieldsDataAsList(input);
        ImmutableMultimap.Builder<String, HiveDdbQueryFilter> queryMapBuilder = ImmutableMultimap.builder();
        for (int i = 0; i < filterList.size(); i++) {
            Object filterData = filterList.get(i);

            String attribute = this.fetchStringField(filterData, this.filterOI[i], this.attributeField[i]);
            Objects.requireNonNull(attribute, "Expected field 'attribute' to not be null in filter");
            String attributeType = this.fetchStringField(filterData, this.filterOI[i], this.attributeTypeField[i]);
            Objects.requireNonNull(attributeType, "Expected field 'attributeType' to not be null in filter "
                                             + "(attribute=" + attribute + ")");
            String operator = this.fetchStringField(filterData, this.filterOI[i], this.operatorField[i]);
            Objects.requireNonNull(operator, "Expected field 'operator' to not be null in filter "
                                          + "(attribute=" + attribute + ")");
            Object value = this.filterOI[i].getStructFieldData(filterData, this.valueField[i]);
            Objects.requireNonNull(value, "Expected field 'value' to not be null in filter "
                                          + "(attribute=" + attribute + ", operator=" + operator + ")");
            HiveDdbQueryFilter filter = HiveDdbQueryFilter
                    .builder()
                    .attribute(attribute)
                    .attributeType(attributeType)
                    .operator(operator)
                    .value(value)
                    .valueOi(this.valueField[i].getFieldObjectInspector())
                    .build();
            queryMapBuilder.put(filter.getAttribute(), filter);
        }
        return queryMapBuilder.build();
    }

    private void assertPrimitive(StructField field) throws UDFArgumentException {
        ObjectInspector fieldOI = field.getFieldObjectInspector();
        if (!(fieldOI instanceof PrimitiveObjectInspector)) {
            String msg = String.format("Expected query filter field to be primitive (name=%s, found=%s)",
                                       field.getFieldName(), fieldOI.getTypeName());
            throw new UDFArgumentException(msg);
        }
    }

    private String fetchStringField(Object input, StructObjectInspector structOI, StructField field) {
        Object fieldData = structOI.getStructFieldData(input, field);
        PrimitiveObjectInspector tableNameOI = (PrimitiveObjectInspector) field.getFieldObjectInspector();
        return PrimitiveObjectInspectorUtils.getString(fieldData, tableNameOI);
    }
}
