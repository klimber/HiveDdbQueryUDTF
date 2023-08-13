package com.klimber.hiveddbudtf.client.ddb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import java.nio.ByteBuffer;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBBinarySetType;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBBinaryType;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBBooleanType;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBListType;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBMapType;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBNullType;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBNumberSetType;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBNumberType;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBStringSetType;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBStringType;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static java.nio.charset.StandardCharsets.UTF_8;

class DynamoDbTypeFinderTest {

    @Test
    void forAttributeValueStringTest() {
        AttributeValue attValue = new AttributeValue("String");
        HiveDynamoDBType hiveDynamoDBType = DynamoDbTypeFinder.forAttributeValue(attValue);
        Assertions.assertTrue(hiveDynamoDBType instanceof HiveDynamoDBStringType);
    }

    @Test
    void forAttributeValueStringSetTest() {
        AttributeValue attValue = new AttributeValue().withSS("One", "Two");
        HiveDynamoDBType hiveDynamoDBType = DynamoDbTypeFinder.forAttributeValue(attValue);
        Assertions.assertTrue(hiveDynamoDBType instanceof HiveDynamoDBStringSetType);
    }

    @Test
    void forAttributeValueNumberTest() {
        AttributeValue attValue = new AttributeValue().withN("1");
        HiveDynamoDBType hiveDynamoDBType = DynamoDbTypeFinder.forAttributeValue(attValue);
        Assertions.assertTrue(hiveDynamoDBType instanceof HiveDynamoDBNumberType);
    }

    @Test
    void forAttributeValueNumberSetTest() {
        AttributeValue attValue = new AttributeValue().withNS("1", "2");
        HiveDynamoDBType hiveDynamoDBType = DynamoDbTypeFinder.forAttributeValue(attValue);
        Assertions.assertTrue(hiveDynamoDBType instanceof HiveDynamoDBNumberSetType);
    }

    @Test
    void forAttributeValueBinaryTest() {
        AttributeValue attValue = new AttributeValue().withB(ByteBuffer.wrap("String".getBytes(UTF_8)));
        HiveDynamoDBType hiveDynamoDBType = DynamoDbTypeFinder.forAttributeValue(attValue);
        Assertions.assertTrue(hiveDynamoDBType instanceof HiveDynamoDBBinaryType);
    }

    @Test
    void forAttributeValueBinarySetTest() {
        AttributeValue attValue = new AttributeValue().withBS(ByteBuffer.wrap("One".getBytes(UTF_8)),
                                                              ByteBuffer.wrap("Two".getBytes(UTF_8)));
        HiveDynamoDBType hiveDynamoDBType = DynamoDbTypeFinder.forAttributeValue(attValue);
        Assertions.assertTrue(hiveDynamoDBType instanceof HiveDynamoDBBinarySetType);
    }

    @Test
    void forAttributeValueNullTest() {
        AttributeValue attValue = new AttributeValue().withNULL(Boolean.TRUE);
        HiveDynamoDBType hiveDynamoDBType = DynamoDbTypeFinder.forAttributeValue(attValue);
        Assertions.assertTrue(hiveDynamoDBType instanceof HiveDynamoDBNullType);
    }

    @Test
    void forAttributeValueCallWithNullTest() {
        HiveDynamoDBType hiveDynamoDBType = DynamoDbTypeFinder.forAttributeValue(null);
        Assertions.assertTrue(hiveDynamoDBType instanceof HiveDynamoDBNullType);
    }

    @Test
    void forAttributeValueBoolTest() {
        AttributeValue attValue = new AttributeValue().withBOOL(Boolean.TRUE);
        HiveDynamoDBType hiveDynamoDBType = DynamoDbTypeFinder.forAttributeValue(attValue);
        Assertions.assertTrue(hiveDynamoDBType instanceof HiveDynamoDBBooleanType);
    }

    @Test
    void forAttributeValueMapTest() {
        AttributeValue attValue = new AttributeValue().addMEntry("key", new AttributeValue("value"));
        HiveDynamoDBType hiveDynamoDBType = DynamoDbTypeFinder.forAttributeValue(attValue);
        Assertions.assertTrue(hiveDynamoDBType instanceof HiveDynamoDBMapType);
    }

    @Test
    void forAttributeValueListTest() {
        AttributeValue attValue = new AttributeValue()
                .withL(new AttributeValue("One"), new AttributeValue("Two"));
        HiveDynamoDBType hiveDynamoDBType = DynamoDbTypeFinder.forAttributeValue(attValue);
        Assertions.assertTrue(hiveDynamoDBType instanceof HiveDynamoDBListType);
    }

    @Test
    void forAttributeValueInvalidTest() {
        AttributeValue attValue = new AttributeValue();
        RuntimeException ex = Assertions.assertThrows(RuntimeException.class,
                                                      () -> DynamoDbTypeFinder.forAttributeValue(attValue));
        Assertions.assertTrue(ex.getMessage().contains("Should be impossible to reach here."));
    }
}