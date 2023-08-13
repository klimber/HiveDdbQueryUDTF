package com.klimber.hiveddbudtf.client.ddb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import lombok.experimental.UtilityClass;
import org.apache.hadoop.dynamodb.type.DynamoDBTypeConstants;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBType;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBTypeFactory;

/**
 * AWS SDKv2 has a <a
 * href="https://sdk.amazonaws.com/java/api/latest/software/amazon/awssdk/services/dynamodb/model/AttributeValue
 * .html#type()">type</a> method to help discover which kind of attribute was returned from the API. This method does
 * not exist on SDKv1, so we have to rely on checking null fields.
 */
@UtilityClass
public class DynamoDbTypeFinder {

    /**
     * Gets the appropriate {@link HiveDynamoDBType} for a given {@link AttributeValue}.
     * 
     * @param value a DynamoDB {@link AttributeValue}
     * @return the {@link HiveDynamoDBType} for that {@link AttributeValue}
     */
    public static HiveDynamoDBType forAttributeValue(AttributeValue value) {
        if (value == null)
            return HiveDynamoDBTypeFactory.getTypeObjectFromDynamoDBType(DynamoDBTypeConstants.NULL);
        if (value.getS() != null)
            return HiveDynamoDBTypeFactory.getTypeObjectFromDynamoDBType(DynamoDBTypeConstants.STRING);
        if (value.getN() != null)
            return HiveDynamoDBTypeFactory.getTypeObjectFromDynamoDBType(DynamoDBTypeConstants.NUMBER);
        if (value.getB() != null)
            return HiveDynamoDBTypeFactory.getTypeObjectFromDynamoDBType(DynamoDBTypeConstants.BINARY);
        if (value.getSS() != null)
            return HiveDynamoDBTypeFactory.getTypeObjectFromDynamoDBType(DynamoDBTypeConstants.STRING_SET);
        if (value.getNS() != null)
            return HiveDynamoDBTypeFactory.getTypeObjectFromDynamoDBType(DynamoDBTypeConstants.NUMBER_SET);
        if (value.getBS() != null)
            return HiveDynamoDBTypeFactory.getTypeObjectFromDynamoDBType(DynamoDBTypeConstants.BINARY_SET);
        if (value.getM() != null)
            return HiveDynamoDBTypeFactory.getTypeObjectFromDynamoDBType(DynamoDBTypeConstants.MAP);
        if (value.getL() != null)
            return HiveDynamoDBTypeFactory.getTypeObjectFromDynamoDBType(DynamoDBTypeConstants.LIST);
        if (value.getNULL() != null)
            return HiveDynamoDBTypeFactory.getTypeObjectFromDynamoDBType(DynamoDBTypeConstants.NULL);
        if (value.getBOOL() != null)
            return HiveDynamoDBTypeFactory.getTypeObjectFromDynamoDBType(DynamoDBTypeConstants.BOOLEAN);
        throw new RuntimeException("Should be impossible to reach here.");
    }
}