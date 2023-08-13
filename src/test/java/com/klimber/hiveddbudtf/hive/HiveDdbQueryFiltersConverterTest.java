package com.klimber.hiveddbudtf.hive;

import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaLongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HiveDdbQueryFiltersConverterTest {
    private List<String> filterFieldNames;
    private JavaStringObjectInspector stringOI;
    private StructObjectInspector stringFilterOI;

    @BeforeEach
    void setUp() {
        this.filterFieldNames = Arrays.asList("attribute", "attributeType", "operator", "value");
        this.stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        List<ObjectInspector> stringFilterFieldOIs = Arrays.asList(this.stringOI, this.stringOI, this.stringOI,
                                                                   this.stringOI);
        this.stringFilterOI = ObjectInspectorFactory
                .getStandardStructObjectInspector(this.filterFieldNames, stringFilterFieldOIs);
    }

    @Test
    @SneakyThrows
    void convertTest() {
        JavaLongObjectInspector longOI = PrimitiveObjectInspectorFactory.javaLongObjectInspector;
        List<ObjectInspector> numberFilterFieldOIs = Arrays.asList(this.stringOI, this.stringOI, this.stringOI, longOI);
        StructObjectInspector numberFilterOI = ObjectInspectorFactory
                .getStandardStructObjectInspector(this.filterFieldNames, numberFilterFieldOIs);

        List<String> filtersFieldNames = Arrays.asList("col1", "col2", "col3");
        List<ObjectInspector> filtersOIs = Arrays.asList(this.stringFilterOI, this.stringFilterOI, numberFilterOI);

        StandardStructObjectInspector filtersOI = ObjectInspectorFactory
                .getStandardStructObjectInspector(filtersFieldNames, filtersOIs);
        HiveDdbQueryFiltersConverter converter = new HiveDdbQueryFiltersConverter(filtersOI);

        List<Object> filter1Struct = new ArrayList<>();
        filter1Struct.add("myString");
        filter1Struct.add("string");
        filter1Struct.add("greater_than");
        filter1Struct.add("targetValue1");

        List<Object> filter2Struct = new ArrayList<>();
        filter2Struct.add("myString");
        filter2Struct.add("string");
        filter2Struct.add("less_than");
        filter2Struct.add("targetValue2");

        List<Object> filter3Struct = new ArrayList<>();
        filter3Struct.add("myNumber");
        filter3Struct.add("number");
        filter3Struct.add("equals");
        filter3Struct.add(1L);

        List<Object> filterArray = new ArrayList<>();
        filterArray.add(filter1Struct);
        filterArray.add(filter2Struct);
        filterArray.add(filter3Struct);

        Multimap<String, HiveDdbQueryFilter> actual = converter.convert(filterArray);
        List<HiveDdbQueryFilter> stringFilters = new ArrayList<>(actual.get("myString"));
        Assertions.assertEquals(2, stringFilters.size());

        HiveDdbQueryFilter stringFilter1 = stringFilters.get(0);
        Assertions.assertEquals("myString", stringFilter1.getAttribute());
        Assertions.assertEquals("string", stringFilter1.getAttributeType());
        Assertions.assertEquals("greater_than", stringFilter1.getOperator());
        Assertions.assertEquals("targetValue1", stringFilter1.getValue());

        HiveDdbQueryFilter stringFilter2 = stringFilters.get(1);
        Assertions.assertEquals("myString", stringFilter2.getAttribute());
        Assertions.assertEquals("string", stringFilter2.getAttributeType());
        Assertions.assertEquals("less_than", stringFilter2.getOperator());
        Assertions.assertEquals("targetValue2", stringFilter2.getValue());

        List<HiveDdbQueryFilter> numberFilters = new ArrayList<>(actual.get("myNumber"));
        Assertions.assertEquals(1, numberFilters.size());
        HiveDdbQueryFilter numberFilter = numberFilters.get(0);
        Assertions.assertEquals("myNumber", numberFilter.getAttribute());
        Assertions.assertEquals("number", numberFilter.getAttributeType());
        Assertions.assertEquals("equals", numberFilter.getOperator());
        Assertions.assertEquals(1L, numberFilter.getValue());
    }

    @Test
    @SneakyThrows
    void nullAttributeConvertTest() {
        List<String> filtersFieldNames = Collections.singletonList("col1");
        List<ObjectInspector> filtersOIs = Collections.singletonList(this.stringFilterOI);

        StandardStructObjectInspector filtersOI = ObjectInspectorFactory
                .getStandardStructObjectInspector(filtersFieldNames, filtersOIs);
        HiveDdbQueryFiltersConverter converter = new HiveDdbQueryFiltersConverter(filtersOI);

        List<Object> filterStruct = new ArrayList<>();
        filterStruct.add(null);
        filterStruct.add("string");
        filterStruct.add("greater_than");
        filterStruct.add("targetValue1");

        List<Object> filterArray = new ArrayList<>();
        filterArray.add(filterStruct);

        NullPointerException ex = Assertions.assertThrows(NullPointerException.class,
                                                          () -> converter.convert(filterArray));
        Assertions.assertEquals("Expected field 'attribute' to not be null in filter", ex.getMessage());
    }

    @Test
    @SneakyThrows
    void nullAttributeTypeConvertTest() {
        List<String> filtersFieldNames = Collections.singletonList("col1");
        List<ObjectInspector> filtersOIs = Collections.singletonList(this.stringFilterOI);

        StandardStructObjectInspector filtersOI = ObjectInspectorFactory
                .getStandardStructObjectInspector(filtersFieldNames, filtersOIs);
        HiveDdbQueryFiltersConverter converter = new HiveDdbQueryFiltersConverter(filtersOI);

        List<Object> filterStruct = new ArrayList<>();
        filterStruct.add("myString");
        filterStruct.add(null);
        filterStruct.add("greater_than");
        filterStruct.add("targetValue1");

        List<Object> filterArray = new ArrayList<>();
        filterArray.add(filterStruct);

        NullPointerException ex = Assertions.assertThrows(NullPointerException.class,
                                                          () -> converter.convert(filterArray));
        Assertions.assertEquals("Expected field 'attributeType' to not be null in filter (attribute=myString)",
                                ex.getMessage());
    }

    @Test
    @SneakyThrows
    void nullOperatorConvertTest() {
        List<String> filtersFieldNames = Collections.singletonList("col1");
        List<ObjectInspector> filtersOIs = Collections.singletonList(this.stringFilterOI);

        StandardStructObjectInspector filtersOI = ObjectInspectorFactory
                .getStandardStructObjectInspector(filtersFieldNames, filtersOIs);
        HiveDdbQueryFiltersConverter converter = new HiveDdbQueryFiltersConverter(filtersOI);

        List<Object> filterStruct = new ArrayList<>();
        filterStruct.add("myString");
        filterStruct.add("string");
        filterStruct.add(null);
        filterStruct.add("targetValue1");

        List<Object> filterArray = new ArrayList<>();
        filterArray.add(filterStruct);

        NullPointerException ex = Assertions.assertThrows(NullPointerException.class,
                                                          () -> converter.convert(filterArray));
        Assertions.assertEquals("Expected field 'operator' to not be null in filter (attribute=myString)",
                                ex.getMessage());
    }

    @Test
    @SneakyThrows
    void nullValueConvertTest() {
        List<String> filtersFieldNames = Collections.singletonList("col1");
        List<ObjectInspector> filtersOIs = Collections.singletonList(this.stringFilterOI);

        StandardStructObjectInspector filtersOI = ObjectInspectorFactory
                .getStandardStructObjectInspector(filtersFieldNames, filtersOIs);
        HiveDdbQueryFiltersConverter converter = new HiveDdbQueryFiltersConverter(filtersOI);

        List<Object> filterStruct = new ArrayList<>();
        filterStruct.add("myString");
        filterStruct.add("string");
        filterStruct.add("greater_than");
        filterStruct.add(null);

        List<Object> filterArray = new ArrayList<>();
        filterArray.add(filterStruct);

        NullPointerException ex = Assertions.assertThrows(NullPointerException.class,
                                                          () -> converter.convert(filterArray));
        Assertions.assertEquals("Expected field 'value' to not be null in filter "
                                + "(attribute=myString, operator=greater_than)", ex.getMessage());
    }

    @Test
    void wrongFilterElementTypeTest() {
        List<String> fieldNames = Collections.singletonList("col1");
        List<ObjectInspector> fieldOIs = Collections.singletonList(this.stringOI);

        StructObjectInspector structOI = ObjectInspectorFactory
                .getStandardStructObjectInspector(fieldNames, fieldOIs);
        Assertions.assertThrows(UDFArgumentException.class, () -> new HiveDdbQueryFiltersConverter(structOI));
    }

    @Test
    void wrongFilterFieldElementTypeTest() {
        List<String> filterFieldNames = Arrays.asList("attribute", "attributeType", "operator", "value");
        StandardListObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(this.stringOI);
        List<ObjectInspector> invalidFilterFieldOIs = Arrays.asList(this.stringOI, this.stringOI, this.stringOI,
                                                                    listOI);
        StructObjectInspector invalidFilterOI = ObjectInspectorFactory
                .getStandardStructObjectInspector(filterFieldNames, invalidFilterFieldOIs);

        List<String> fieldNames = Collections.singletonList("col1");
        List<ObjectInspector> fieldOIs = Collections.singletonList(invalidFilterOI);

        StructObjectInspector structOI = ObjectInspectorFactory
                .getStandardStructObjectInspector(fieldNames, fieldOIs);
        Assertions.assertThrows(UDFArgumentException.class, () -> new HiveDdbQueryFiltersConverter(structOI));
    }
}
