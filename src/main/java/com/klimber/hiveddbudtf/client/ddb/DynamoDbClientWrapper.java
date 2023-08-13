package com.klimber.hiveddbudtf.client.ddb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.Multimap;
import com.klimber.hiveddbudtf.hive.HiveDdbQueryFilter;
import com.klimber.hiveddbudtf.hive.HiveDdbQueryParameters;
import java.util.Map;
import java.util.stream.Stream;

public interface DynamoDbClientWrapper {
    Stream<Map<String, AttributeValue>> queryTable(HiveDdbQueryParameters params,
                                                   Multimap<String, HiveDdbQueryFilter> filters);
}
