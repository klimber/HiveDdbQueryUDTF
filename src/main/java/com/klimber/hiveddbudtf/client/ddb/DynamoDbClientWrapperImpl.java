package com.klimber.hiveddbudtf.client.ddb;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.klimber.hiveddbudtf.hive.HiveDdbQueryFilter;
import com.klimber.hiveddbudtf.hive.HiveDdbQueryParameters;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.RequiredArgsConstructor;

public class DynamoDbClientWrapperImpl implements DynamoDbClientWrapper {
    private final AWSCredentialsProvider credentials;
    private AmazonDynamoDB client;
    private final Cache<String, TableDescription> tableCache = CacheBuilder.newBuilder().build();

    public DynamoDbClientWrapperImpl(AWSCredentialsProvider credentials) {
        this.credentials = credentials;
    }

    DynamoDbClientWrapperImpl(AWSCredentialsProvider credentials, AmazonDynamoDB client) {
        this.credentials = credentials;
        this.client = client;
    } 

    @Override
    public Stream<Map<String, AttributeValue>> queryTable(HiveDdbQueryParameters params,
                                                          Multimap<String, HiveDdbQueryFilter> filters) {
        if(Objects.isNull(this.client)) {
            this.client = AmazonDynamoDBClientBuilder.standard()
                                                     .withCredentials(this.credentials)
                                                     .build();
        }
        String tableName = params.getTableName();
        String indexName = params.getIndexName();
        Map<KeyType, String> keyAttributes = this.findKeyAttributes(tableName, indexName);

        DdbFilterHelper filterHelper = new DdbFilterHelper(filters, keyAttributes);

        Map<String, String> exprAttNames = new HashMap<>();
        for (int i = 0; i < params.getHiveDdbColumnMapping().size(); i++) {
            String attName = params.getHiveDdbColumnMapping().get(i).getDdbAttName();
            String alias = "#p" + i;
            exprAttNames.put(alias, attName);
        }
        String projectionExpr = String.join(", ", exprAttNames.keySet());

        exprAttNames.putAll(filterHelper.getExprAttNames());

        QueryRequest queryReq = new QueryRequest()
                .withTableName(tableName)
                .withIndexName(indexName)
                .withProjectionExpression(projectionExpr)
                .withKeyConditionExpression(filterHelper.getKeyConditionExpr())
                .withFilterExpression(filterHelper.getFilterExpr())
                .withExpressionAttributeNames(exprAttNames)
                .withExpressionAttributeValues(filterHelper.getExprAttValues());

        QueryResultIterator resultIterator = new QueryResultIterator(this.client, queryReq);
        Spliterator<QueryResult> queryResultSpliterator = Spliterators.spliteratorUnknownSize(resultIterator, 0);
        Stream<QueryResult> queryResults = StreamSupport.stream(() -> queryResultSpliterator,
                                                                queryResultSpliterator.characteristics(),
                                                                false);
        return queryResults.flatMap(q -> q.getItems().stream());
    }

    private Map<KeyType, String> findKeyAttributes(String tableName, String indexName) {
        TableDescription tableDesc = this.describeTable(tableName);
        Stream<KeySchemaElement> keySchemaElements = this.getKeySchemaElements(tableDesc, indexName);
        return keySchemaElements.collect(Collectors.toMap(e -> KeyType.fromValue(e.getKeyType()),
                                                          KeySchemaElement::getAttributeName));
    }

    private TableDescription describeTable(String tableName) {
        try {
            return this.tableCache.get(tableName, () -> this.callDescribeTable(tableName));
        } catch (ExecutionException | UncheckedExecutionException e) {
            throw new DynamoDbClientWrapperException("Failed to load table description.", e.getCause());
        }
    }

    private TableDescription callDescribeTable(String tableName) {
        return this.client.describeTable(tableName).getTable();
    }

    private Stream<KeySchemaElement> getKeySchemaElements(TableDescription tableDesc, String indexName) {
        if (Objects.isNull(indexName)) {
            return tableDesc.getKeySchema().stream();
        } else {
            Stream<KeySchemaElement> globalKeys = tableDesc.getGlobalSecondaryIndexes().stream()
                                                           .filter(g -> indexName.equals(g.getIndexName()))
                                                           .map(GlobalSecondaryIndexDescription::getKeySchema)
                                                           .flatMap(List::stream);
            Stream<KeySchemaElement> localKeys = tableDesc.getLocalSecondaryIndexes().stream()
                                                          .filter(l -> indexName.equals(l.getIndexName()))
                                                          .map(LocalSecondaryIndexDescription::getKeySchema)
                                                          .flatMap(List::stream);
            return Stream.concat(globalKeys, localKeys);
        }
    }

    @RequiredArgsConstructor
    private static class QueryResultIterator implements Iterator<QueryResult> {
        private final AmazonDynamoDB client;
        private final QueryRequest request;
        private boolean hasNext = true;

        @Override
        public boolean hasNext() {
            return this.hasNext;
        }

        @Override
        public QueryResult next() {
            QueryResult next = this.client.query(this.request);
            this.request.setExclusiveStartKey(next.getLastEvaluatedKey());
            this.hasNext = next.getLastEvaluatedKey() != null;
            return next;
        }
    }
}