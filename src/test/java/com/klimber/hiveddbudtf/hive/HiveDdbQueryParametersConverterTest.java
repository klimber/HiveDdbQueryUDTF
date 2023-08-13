package com.klimber.hiveddbudtf.hive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaConstantStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HiveDdbQueryParametersConverterTest {
    private List<String> fieldNames;
    private JavaStringObjectInspector stringOI;
    private ConstantObjectInspector hiveDdbMapping;
    private ConstantObjectInspector hiveTypeMapping;
    private List<ObjectInspector> fieldOIs;

    @BeforeEach
    void setUp() {
        this.fieldNames = Arrays.asList("tableName", "indexName", "hiveDdbColumnMapping", "hiveTypeMapping");
        this.stringOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        this.hiveDdbMapping = new JavaConstantStringObjectInspector("my_column_1:myColumn1");
        this.hiveTypeMapping = new JavaConstantStringObjectInspector("string");
        this.fieldOIs = Arrays.asList(this.stringOI, this.stringOI, this.hiveDdbMapping, this.hiveTypeMapping);
    }

    @Test
    @SneakyThrows
    void convertTest() {
        StructObjectInspector structOI = ObjectInspectorFactory.getStandardStructObjectInspector(this.fieldNames,
                                                                                                 this.fieldOIs);
        HiveDdbQueryParametersConverter converter = new HiveDdbQueryParametersConverter(structOI);

        List<String> input = new ArrayList<>();
        input.add("myTable");
        input.add("myIndex");

        HiveDdbQueryParameters actual = converter.convert(input);
        Assertions.assertEquals("myTable", actual.getTableName());
        Assertions.assertEquals("myIndex", actual.getIndexName());
        Assertions.assertEquals("my_column_1", actual.getHiveDdbColumnMapping().get(0).getHiveColumn());
        Assertions.assertEquals("myColumn1", actual.getHiveDdbColumnMapping().get(0).getDdbAttName());
        Assertions.assertEquals("string", actual.getHiveTypes().get(0).getTypeName());
        Assertions.assertEquals(actual.getHiveDdbColumnMapping(), converter.getHiveDdbColumnMapping());
        Assertions.assertEquals(actual.getHiveTypes(), converter.getHiveTypes());
    }

    @Test
    @SneakyThrows
    void nullTableNameConvertTest() {
        StructObjectInspector structOI = ObjectInspectorFactory.getStandardStructObjectInspector(this.fieldNames,
                                                                                                 this.fieldOIs);
        HiveDdbQueryParametersConverter converter = new HiveDdbQueryParametersConverter(structOI);

        List<String> input = new ArrayList<>();
        input.add(null);
        input.add("myIndex");

        NullPointerException ex = Assertions.assertThrows(NullPointerException.class,
                                                          () -> converter.convert(input));
        Assertions.assertEquals("Expected field 'tablename' to not be null", ex.getMessage());
    }

    @Test
    @SneakyThrows
    void nullIndexNameConvertTest() {
        StructObjectInspector structOI = ObjectInspectorFactory.getStandardStructObjectInspector(this.fieldNames,
                                                                                                 this.fieldOIs);
        HiveDdbQueryParametersConverter converter = new HiveDdbQueryParametersConverter(structOI);

        List<String> input = new ArrayList<>();
        input.add("myTable");
        input.add(null);

        HiveDdbQueryParameters actual = converter.convert(input);
        Assertions.assertEquals("myTable", actual.getTableName());
        Assertions.assertNull(actual.getIndexName());
        Assertions.assertEquals("my_column_1", actual.getHiveDdbColumnMapping().get(0).getHiveColumn());
        Assertions.assertEquals("myColumn1", actual.getHiveDdbColumnMapping().get(0).getDdbAttName());
        Assertions.assertEquals("string", actual.getHiveTypes().get(0).getTypeName());
        Assertions.assertEquals(actual.getHiveDdbColumnMapping(), converter.getHiveDdbColumnMapping());
        Assertions.assertEquals(actual.getHiveTypes(), converter.getHiveTypes());
    }

    @Test
    @SneakyThrows
    void invalidTableOITest() {
        StandardListObjectInspector listOI = ObjectInspectorFactory.getStandardListObjectInspector(this.stringOI);
        List<ObjectInspector> invalidTableOIs = Arrays.asList(listOI, this.stringOI, this.hiveDdbMapping,
                                                              this.hiveTypeMapping);
        StructObjectInspector structOI = ObjectInspectorFactory.getStandardStructObjectInspector(this.fieldNames,
                                                                                                 invalidTableOIs);
        UDFArgumentException ex = Assertions.assertThrows(UDFArgumentException.class,
                                                          () -> new HiveDdbQueryParametersConverter(structOI));
        Assertions.assertTrue(ex.getMessage().contains("Expected query parameter field to be primitive"));
    }

    @Test
    @SneakyThrows
    void mutableHiveMappingOiTest() {
        List<ObjectInspector> mutableHiveMappingOI = Arrays.asList(this.stringOI, this.stringOI, this.stringOI,
                                                                   this.hiveTypeMapping);
        StructObjectInspector structOI = ObjectInspectorFactory.getStandardStructObjectInspector(this.fieldNames,
                                                                                                 mutableHiveMappingOI);
        UDFArgumentException ex = Assertions.assertThrows(UDFArgumentException.class,
                                                          () -> new HiveDdbQueryParametersConverter(structOI));
        Assertions.assertTrue(ex.getMessage().contains("Expected query parameter field to be constant"));
    }

    @Test
    @SneakyThrows
    void emptyHiveMappingValueTest() {
        JavaConstantStringObjectInspector emptyHiveDdbMapping = new JavaConstantStringObjectInspector("");
        List<ObjectInspector> invalidHiveMappingOI = Arrays.asList(this.stringOI, this.stringOI, emptyHiveDdbMapping,
                                                                   this.hiveTypeMapping);
        StructObjectInspector structOI = ObjectInspectorFactory.getStandardStructObjectInspector(this.fieldNames,
                                                                                                 invalidHiveMappingOI);
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                                                              () -> new HiveDdbQueryParametersConverter(structOI));
        Assertions.assertTrue(ex.getMessage().contains("column mapping should contain 2 column names"));
    }

    @Test
    @SneakyThrows
    void invalidHiveMappingValueTest() {
        JavaConstantStringObjectInspector invalidHiveDdbMapping = new JavaConstantStringObjectInspector(
                "my_column_1: ");
        List<ObjectInspector> invalidHiveMappingOI = Arrays.asList(this.stringOI, this.stringOI, invalidHiveDdbMapping,
                                                                   this.hiveTypeMapping);
        StructObjectInspector structOI = ObjectInspectorFactory.getStandardStructObjectInspector(this.fieldNames,
                                                                                                 invalidHiveMappingOI);
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                                                              () -> new HiveDdbQueryParametersConverter(structOI));
        Assertions.assertTrue(ex.getMessage().contains("at least one non whitespace character"));
    }

    @Test
    @SneakyThrows
    void unmatchedHiveTypeMappingValueTest() {
        JavaConstantStringObjectInspector hiveType = new JavaConstantStringObjectInspector(
                "string,binary");
        List<ObjectInspector> hiveTypeOIs = Arrays.asList(this.stringOI, this.stringOI, this.hiveDdbMapping, hiveType);
        StructObjectInspector structOI = ObjectInspectorFactory.getStandardStructObjectInspector(this.fieldNames,
                                                                                                 hiveTypeOIs);
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                                                              () -> new HiveDdbQueryParametersConverter(structOI));
        Assertions.assertTrue(ex.getMessage().contains("should contain the same number of columns"));
    }

    @Test
    @SneakyThrows
    void nullHiveDdbMappingValueTest() {
        JavaConstantStringObjectInspector hiveDdbMapping = new JavaConstantStringObjectInspector(null);
        List<ObjectInspector> fieldOIs = Arrays.asList(this.stringOI, this.stringOI, hiveDdbMapping,
                                                          this.hiveTypeMapping);
        StructObjectInspector structOI = ObjectInspectorFactory.getStandardStructObjectInspector(this.fieldNames,
                                                                                                 fieldOIs);
        NullPointerException ex = Assertions.assertThrows(NullPointerException.class,
                                                          () -> new HiveDdbQueryParametersConverter(structOI));
        Assertions.assertEquals("Expected field 'hiveddbcolumnmapping' to not be null", ex.getMessage());
    }

    @Test
    @SneakyThrows
    void nullHiveTypeMappingValueTest() {
        JavaConstantStringObjectInspector hiveType = new JavaConstantStringObjectInspector(null);
        List<ObjectInspector> hiveTypeOIs = Arrays.asList(this.stringOI, this.stringOI, this.hiveDdbMapping, hiveType);
        StructObjectInspector structOI = ObjectInspectorFactory.getStandardStructObjectInspector(this.fieldNames,
                                                                                                 hiveTypeOIs);
        NullPointerException ex = Assertions.assertThrows(NullPointerException.class,
                                                          () -> new HiveDdbQueryParametersConverter(structOI));
        Assertions.assertEquals("Expected field 'hivetypemapping' to not be null", ex.getMessage());
    }
}
