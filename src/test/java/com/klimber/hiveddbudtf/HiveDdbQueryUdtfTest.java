package com.klimber.hiveddbudtf;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.klarna.hiverunner.HiveRunnerExtension;
import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import com.klimber.hiveddbudtf.client.ddb.DynamoDbClientWrapper;
import com.klimber.hiveddbudtf.hive.HiveDdbQueryFilter;
import com.klimber.hiveddbudtf.hive.HiveDdbQueryParameters;
import java.io.File;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

@ExtendWith({HiveRunnerExtension.class})
class HiveDdbQueryUdtfTest {
    @HiveSQL(files = {"HiveDdbUdtfTest/queries/initialize_udtf.sql",
            "HiveDdbUdtfTest/queries/initialize_query_keys.sql"})
    private HiveShell shell;
    private AutoCloseable closeable;
    @Captor
    private ArgumentCaptor<HiveDdbQueryParameters> paramsCaptor;
    @Captor
    private ArgumentCaptor<Multimap<String, HiveDdbQueryFilter>> filtersCaptor;

    @BeforeEach
    void setUp() {
        this.closeable = MockitoAnnotations.openMocks(this);
    }

    @AfterEach
    @SneakyThrows
    void close() {
        this.closeable.close();
    }

    @Test
    @SneakyThrows
    void dataFormatsExceptBinaryTest() {
        MockedHiveDdbQueryUdtf.MOCK_SUPPLIER = () -> this.mockDynamoDB(this.dataFormatsNoBinaryMockResult());

        URL ddbQueryPath = ClassLoader.getSystemResource("HiveDdbUdtfTest/queries/ddb_query_no_binary.sql");
        URL expectedRows = ClassLoader.getSystemResource("HiveDdbUdtfTest/outputs/no_binary_results.txt");
        URL expectedDescribe = ClassLoader.getSystemResource("HiveDdbUdtfTest/outputs/no_binary_describe.txt");

        this.shell.executeQuery(Paths.get(ddbQueryPath.toURI()));
        List<String> results = this.shell.executeQuery("SELECT * FROM result");
        String result = String.join("\n", results);
        Assertions.assertEquals(FileUtils.readFileToString(new File(expectedRows.toURI())), result);
        results = this.shell.executeQuery("DESCRIBE result");
        result = String.join("\n", results);
        Assertions.assertEquals(FileUtils.readFileToString(new File(expectedDescribe.toURI())), result);
    }

    @Test
    @SneakyThrows
    void binaryDataFormatsTest() {
        MockedHiveDdbQueryUdtf.MOCK_SUPPLIER = () -> this.mockDynamoDB(this.binaryDataFormatsMockResult());

        URL ddbQueryPath = ClassLoader.getSystemResource("HiveDdbUdtfTest/queries/ddb_query_binary.sql");
        URL expectedRows = ClassLoader.getSystemResource("HiveDdbUdtfTest/outputs/binary_results.txt");
        URL expectedDescribe = ClassLoader.getSystemResource("HiveDdbUdtfTest/outputs/binary_describe.txt");

        this.shell.executeQuery(Paths.get(ddbQueryPath.toURI()));
        List<String> results = this.shell.executeQuery("SELECT DECODE(field_binary, 'UTF-8'), " 
                                                       + "DECODE(bset, 'UTF-8') " 
                                                       + "FROM result " 
                                                       + "LATERAL VIEW EXPLODE(field_binary_set) bSet as bSet");
        String result = String.join("\n", results);
        Assertions.assertEquals(FileUtils.readFileToString(new File(expectedRows.toURI())), result);
        results = this.shell.executeQuery("DESCRIBE result");
        result = String.join("\n", results);
        Assertions.assertEquals(FileUtils.readFileToString(new File(expectedDescribe.toURI())), result);
    }

    @Test
    @SneakyThrows
    void nestedMapOfStringsTest() {
        // While this is undesired behavior, I'm testing it to know when it changes
        MockedHiveDdbQueryUdtf.MOCK_SUPPLIER = () -> this.mockDynamoDB(this.nestedMapOfStringsMockResult());

        URL ddbQueryPath = ClassLoader.getSystemResource("HiveDdbUdtfTest/queries/ddb_query_nested_map_of_strings.sql");

        IllegalArgumentException ex =
                Assertions.assertThrows(IllegalArgumentException.class,
                                        () -> this.shell.executeQuery(Paths.get(ddbQueryPath.toURI())));
        Assertions.assertTrue(ex.getMessage().contains("DynamoDBItemType does not support this operation"));
    }

    @Test
    @SneakyThrows
    void unsupportedDynamoDBTypeConversionTest() {
        MockedHiveDdbQueryUdtf.MOCK_SUPPLIER = () -> this.mockDynamoDB(this.binaryDataFormatsMockResult());

        URL ddbQueryPath = ClassLoader.getSystemResource("HiveDdbUdtfTest/queries/ddb_query_unsupported_ddb_type.sql");

        IllegalArgumentException ex =
                Assertions.assertThrows(IllegalArgumentException.class,
                                        () -> this.shell.executeQuery(Paths.get(ddbQueryPath.toURI())));
        String expectedMessage = "Hive type 'string' does not support DynamoDB type 'B' (ddbAttributeName=fieldBinary)";
        Assertions.assertTrue(ex.getMessage().contains(expectedMessage));
    }

    @Test
    @SneakyThrows
    void missingParametersTest() {
        MockedHiveDdbQueryUdtf.MOCK_SUPPLIER = () -> Mockito.mock(DynamoDbClientWrapper.class);

        IllegalArgumentException ex =
                Assertions.assertThrows(IllegalArgumentException.class,
                                        () -> this.shell.executeQuery("SELECT ddb_query()"));
        String expectedMessage = "DDB_QUERY requires 2 arguments (found=0)";
        Assertions.assertTrue(ex.getMessage().contains(expectedMessage));
    }

    @Test
    @SneakyThrows
    void wrongTypeParamsTest() {
        MockedHiveDdbQueryUdtf.MOCK_SUPPLIER = () -> Mockito.mock(DynamoDbClientWrapper.class);

        IllegalArgumentException ex =
                Assertions.assertThrows(IllegalArgumentException.class,
                                        () -> this.shell.executeQuery("SELECT ddb_query('params', 'filters')"));
        String expectedMessage = "DDB_QUERY first argument should be a struct (found=PRIMITIVE)";
        Assertions.assertTrue(ex.getMessage().contains(expectedMessage));
    }

    @Test
    @SneakyThrows
    void wrongTypeFiltersTest() {
        MockedHiveDdbQueryUdtf.MOCK_SUPPLIER = () -> Mockito.mock(DynamoDbClientWrapper.class);

        String hiveSql = "SELECT ddb_query(named_struct('tableName', 'EventHistory'," 
                         + "'indexName', null," 
                         + "'hiveDdbColumnMapping', 'field:attribute'," 
                         + "'hiveTypeMapping', 'string'), 'filters')";
        IllegalArgumentException ex =
                Assertions.assertThrows(IllegalArgumentException.class,
                                        () -> this.shell.executeQuery(hiveSql));
        String expectedMessage = "DDB_QUERY second argument should be a struct (found=PRIMITIVE)";
        Assertions.assertTrue(ex.getMessage().contains(expectedMessage));
    }

    private DynamoDbClientWrapper mockDynamoDB(Stream<Map<String, AttributeValue>> mockResults) {
        DynamoDbClientWrapper ddbMock = Mockito.mock(DynamoDbClientWrapper.class);

        Mockito.doReturn(mockResults)
               .when(ddbMock)
               .queryTable(this.paramsCaptor.capture(), this.filtersCaptor.capture());
        return ddbMock;
    }

    private Stream<Map<String, AttributeValue>> dataFormatsNoBinaryMockResult() {
        Map<String, AttributeValue> row1 =
                ImmutableMap.<String, AttributeValue>builder()
                            .put("fieldString", new AttributeValue("string1"))
                            .put("fieldBigInt", new AttributeValue().withN("1"))
                            .put("fieldDouble", new AttributeValue().withN("1.11"))
                            .put("fieldBoolean", new AttributeValue().withBOOL(true))
                            .put("fieldListString", new AttributeValue()
                                    .withL(new AttributeValue("1"), new AttributeValue("2")))
                            .put("fieldStruct", new AttributeValue()
                                    .addMEntry("fieldString", new AttributeValue("Val1"))
                                    .addMEntry("fieldBigInt", new AttributeValue().withN("1")))
                            .put("fieldMapOfNumbers", new AttributeValue()
                                    .addMEntry("one", new AttributeValue().withN("1.000"))
                                    .addMEntry("two", new AttributeValue().withN("2.000")))
                            .put("fieldMapOfStrings", new AttributeValue()
                                    .addMEntry("one", new AttributeValue("One"))
                                    .addMEntry("two", new AttributeValue("Two")))
                            .put("fieldStringSet", new AttributeValue()
                                    .withSS("stringSetV1", "stringSetV2"))
                            .put("fieldNumberSet", new AttributeValue()
                                    .withNS("1", "1.11"))
                            .put("fieldNull", new AttributeValue().withNULL(true))
                            .build();

        Map<String, AttributeValue> row2 =
                ImmutableMap.<String, AttributeValue>builder()
                            .put("fieldString", new AttributeValue("string2"))
                            .put("fieldBigInt", new AttributeValue().withN("2"))
                            .put("fieldDouble", new AttributeValue().withN("2.22"))
                            .put("fieldBoolean", new AttributeValue().withBOOL(false))
                            .put("fieldListString", new AttributeValue()
                                    .withL(new AttributeValue("3"), new AttributeValue("4")))
                            .put("fieldStruct", new AttributeValue()
                                    .addMEntry("fieldString", new AttributeValue("Val2"))
                                    .addMEntry("fieldBigInt", new AttributeValue().withN("2"))
                                    .addMEntry("fieldIgnored", new AttributeValue("Val3")))
                            .put("fieldMapOfNumbers", new AttributeValue()
                                    .addMEntry("three", new AttributeValue().withN("3.000"))
                                    .addMEntry("four", new AttributeValue().withN("4.000")))
                            .put("fieldMapOfStrings", new AttributeValue()
                                    .addMEntry("three", new AttributeValue("Three"))
                                    .addMEntry("four", new AttributeValue("Four")))
                            .put("fieldStringSet", new AttributeValue()
                                    .withSS("stringSetV3", "stringSetV4"))
                            .put("fieldNumberSet", new AttributeValue()
                                    .withNS("2", "2.22"))
                            // not putting fieldNull here to test missing attribute behavior
                            .build();

        return Stream.of(row1, row2);
    }

    private Stream<Map<String, AttributeValue>> binaryDataFormatsMockResult() {
        Map<String, AttributeValue> row1 =
                ImmutableMap.of("fieldBinary", new AttributeValue()
                        .withB(ByteBuffer.wrap("bytes1".getBytes(StandardCharsets.UTF_8))),
                                "fieldBinarySet", new AttributeValue()
                                        .withBS(ByteBuffer.wrap("binarySetV1".getBytes(StandardCharsets.UTF_8)),
                                                ByteBuffer.wrap("binarySetV2".getBytes(StandardCharsets.UTF_8))));
        Map<String, AttributeValue> row2 =
                ImmutableMap.of("fieldBinary", new AttributeValue()
                        .withB(ByteBuffer.wrap("bytes2".getBytes(StandardCharsets.UTF_8))),
                                "fieldBinarySet", new AttributeValue()
                                        .withBS(ByteBuffer.wrap("binarySetV3".getBytes(StandardCharsets.UTF_8)),
                                                ByteBuffer.wrap("binarySetV4".getBytes(StandardCharsets.UTF_8))));
        return Stream.of(row1, row2);
    }

    private Stream<Map<String, AttributeValue>> nestedMapOfStringsMockResult() {
        Map<String, AttributeValue> row1 =
                ImmutableMap.of("fieldNestedMapOfStrings", new AttributeValue()
                        .addMEntry("nestedMap", new AttributeValue()
                                .addMEntry("nestedStringField",
                                           new AttributeValue("nestedStringValue"))));
        return Stream.of(row1);
    }
}