package com.klimber.hiveddbudtf.client.ddb;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.klimber.hiveddbudtf.hive.HiveDdbQueryFilter;
import com.klimber.hiveddbudtf.hive.HiveDdbQueryParameters;
import com.klimber.hiveddbudtf.hive.HiveDdbQueryParameters.ColumnMapping;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;

class DynamoDbClientWrapperImplTest {
    private AmazonDynamoDB client;
    private DynamoDbClientWrapperImpl wrapper;
    private String tableName;
    private String partitionKeyAtt;
    private String pkValue;
    private String hiveColumn;
    private String ddbAttName;

    @BeforeEach
    void setUp() {
        this.client = Mockito.mock(AmazonDynamoDB.class);
        this.wrapper = new DynamoDbClientWrapperImpl(Mockito.mock(AWSCredentialsProvider.class), this.client);
        this.tableName = UUID.randomUUID().toString();
        this.partitionKeyAtt = UUID.randomUUID().toString();
        this.pkValue = UUID.randomUUID().toString();
        this.hiveColumn = UUID.randomUUID().toString();
        this.ddbAttName = UUID.randomUUID().toString();
    }

    @Test
    void queryTableTest() {
        HiveDdbQueryParameters params = this.getSampleParams().build();
        Multimap<String, HiveDdbQueryFilter> filters = ImmutableMultimap.of(this.partitionKeyAtt, this.getPkFilter());

        KeySchemaElement keySchemaElement = new KeySchemaElement()
                .withAttributeName(this.partitionKeyAtt)
                .withKeyType(KeyType.HASH);
        Collection<KeySchemaElement> keySchema = ImmutableList.of(keySchemaElement);
        TableDescription table = new TableDescription().withKeySchema(keySchema);
        DescribeTableResult describeTableRes = new DescribeTableResult().withTable(table);
        Mockito.doReturn(describeTableRes).when(this.client).describeTable(this.tableName);

        QueryRequest firstRequest = new QueryRequest()
                .withTableName(this.tableName)
                .withExpressionAttributeNames(ImmutableMap.of("#pk", this.partitionKeyAtt,
                                                              "#p0", this.ddbAttName))
                .withExpressionAttributeValues(ImmutableMap.of(":pkValue", new AttributeValue(this.pkValue)))
                .withKeyConditionExpression("#pk = :pkValue")
                .withProjectionExpression("#p0");

        Map<String, AttributeValue> record1 = ImmutableMap.of(this.ddbAttName, new AttributeValue("firstRowValue"));
        Map<String, AttributeValue> lastKey = ImmutableMap.of(this.partitionKeyAtt, new AttributeValue(this.pkValue));
        QueryResult firstPage = new QueryResult().withItems(ImmutableList.of(record1)).withLastEvaluatedKey(lastKey);
        Mockito.doReturn(firstPage)
               .when(this.client)
               .query(firstRequest);

        QueryRequest secondRequest = firstRequest.clone().withExclusiveStartKey(firstPage.getLastEvaluatedKey());
        Map<String, AttributeValue> record2 = ImmutableMap.of(this.ddbAttName, new AttributeValue("secondRowValue"));
        QueryResult secondPage = new QueryResult().withItems(ImmutableList.of(record2));
        Mockito.doReturn(secondPage)
               .when(this.client)
               .query(secondRequest);

        List<Map<String, AttributeValue>> actual = this.wrapper.queryTable(params, filters)
                                                               .collect(Collectors.toList());
        List<Map<String, AttributeValue>> expected = ImmutableList.of(record1, record2);
        Assertions.assertEquals(expected, actual);
    }

    @Test
    void queryTableWithIndexNameTest() {
        String indexName = UUID.randomUUID().toString();
        HiveDdbQueryParameters params = this.getSampleParams()
                                            .indexName(indexName)
                                            .build();
        Multimap<String, HiveDdbQueryFilter> filters = ImmutableMultimap.of(this.partitionKeyAtt, this.getPkFilter());

        KeySchemaElement keySchemaElement = new KeySchemaElement()
                .withAttributeName(this.partitionKeyAtt)
                .withKeyType(KeyType.HASH);
        Collection<KeySchemaElement> keySchema = ImmutableList.of(keySchemaElement);
        LocalSecondaryIndexDescription localIndex = new LocalSecondaryIndexDescription()
                .withIndexName(indexName)
                .withKeySchema(keySchema);
        TableDescription table = new TableDescription()
                .withGlobalSecondaryIndexes(ImmutableList.of())
                .withLocalSecondaryIndexes(ImmutableList.of(localIndex));
        DescribeTableResult describeTableRes = new DescribeTableResult().withTable(table);
        Mockito.doReturn(describeTableRes).when(this.client).describeTable(this.tableName);

        QueryRequest firstRequest = new QueryRequest()
                .withTableName(this.tableName)
                .withIndexName(indexName)
                .withExpressionAttributeNames(ImmutableMap.of("#pk", this.partitionKeyAtt,
                                                              "#p0", this.ddbAttName))
                .withExpressionAttributeValues(ImmutableMap.of(":pkValue", new AttributeValue(this.pkValue)))
                .withKeyConditionExpression("#pk = :pkValue")
                .withProjectionExpression("#p0");

        Map<String, AttributeValue> record1 = ImmutableMap.of(this.ddbAttName, new AttributeValue("firstRowValue"));
        Map<String, AttributeValue> lastKey = ImmutableMap.of(this.partitionKeyAtt, new AttributeValue(this.pkValue));
        QueryResult firstPage = new QueryResult().withItems(ImmutableList.of(record1)).withLastEvaluatedKey(lastKey);
        Mockito.doReturn(firstPage)
               .when(this.client)
               .query(firstRequest);

        QueryRequest secondRequest = firstRequest.clone().withExclusiveStartKey(firstPage.getLastEvaluatedKey());
        Map<String, AttributeValue> record2 = ImmutableMap.of(this.ddbAttName, new AttributeValue("secondRowValue"));
        QueryResult secondPage = new QueryResult().withItems(ImmutableList.of(record2));
        Mockito.doReturn(secondPage)
               .when(this.client)
               .query(secondRequest);

        List<Map<String, AttributeValue>> actual = this.wrapper.queryTable(params, filters)
                                                               .collect(Collectors.toList());
        List<Map<String, AttributeValue>> expected = ImmutableList.of(record1, record2);
        Assertions.assertEquals(expected, actual);
    }

    @Test
    void describeTableFailureTest() {
        HiveDdbQueryParameters params = this.getSampleParams().build();
        Mockito.doThrow(SdkClientException.class).when(this.client).describeTable(this.tableName);

        DynamoDbClientWrapperException ex = Assertions.assertThrows(DynamoDbClientWrapperException.class,
                                                                    () -> this.wrapper.queryTable(params, null));
        Assertions.assertTrue(ex.getMessage().contains("Failed to load table description."));
    }

    private HiveDdbQueryFilter getPkFilter() {
        return HiveDdbQueryFilter.builder()
                                 .attribute(this.partitionKeyAtt)
                                 .attributeType("S")
                                 .operator("EQ")
                                 .value(this.pkValue)
                                 .valueOi(javaStringObjectInspector)
                                 .build();
    }

    private HiveDdbQueryParameters.HiveDdbQueryParametersBuilder getSampleParams() {
        return HiveDdbQueryParameters.builder()
                                     .tableName(this.tableName)
                                     .hiveDdbColumnMapping(ImmutableList.of(
                                             ColumnMapping.builder()
                                                          .hiveColumn(this.hiveColumn)
                                                          .ddbAttName(this.ddbAttName)
                                                          .build()))
                                     .hiveTypes(ImmutableList.of(
                                             TypeInfoFactory.getPrimitiveTypeInfo("string")));
    }
}