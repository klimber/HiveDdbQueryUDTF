package com.klimber.hiveddbudtf.client.ddb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.klimber.hiveddbudtf.hive.HiveDdbQueryFilter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBTypeFactory;

@Getter
class DdbFilterHelper {
    private final Map<String, String> exprAttNames;
    private final Map<String, AttributeValue> exprAttValues;
    private final String keyConditionExpr;
    private final String filterExpr;

    DdbFilterHelper(Multimap<String, HiveDdbQueryFilter> filters, Map<KeyType, String> keyAttributes) {
        Map<String, String> exprAttNamesBuilder = new HashMap<>();
        Map<String, AttributeValue> exprAttValuesBuilder = new HashMap<>();
        List<String> keyConditions = new ArrayList<>();
        List<String> filterConditions = new ArrayList<>();
        keyConditions.add(parseHashKey(filters, keyAttributes, exprAttNamesBuilder, exprAttValuesBuilder));
        parseSortKey(filters, keyAttributes, exprAttNamesBuilder, exprAttValuesBuilder).ifPresent(keyConditions::add);

        int i = 0;
        for (String att : filters.keySet()) {
            if (exprAttNamesBuilder.containsValue(att)) {
                continue;
            }
            i++;
            String attAlias = "a" + i;
            exprAttNamesBuilder.put( "#" + attAlias, att);
            Collection<HiveDdbQueryFilter> attFilters = filters.get(att);
            int j = 0;
            for (HiveDdbQueryFilter attFilter : attFilters) {
                j++;
                String valueAlias = attAlias + "v" + j;
                AttributeValue attValue = HiveDynamoDBTypeFactory
                        .getTypeObjectFromDynamoDBType(attFilter.getAttributeType())
                        .getDynamoDBData(attFilter.getValue(), attFilter.getValueOi(), true);
                exprAttValuesBuilder.put(":" + valueAlias, attValue);
                Operators operator = Operators.valueOf(attFilter.getOperator().toUpperCase());
                filterConditions.add(operator.getCondition("#" + attAlias, ":" + valueAlias));
            }
        }
        this.exprAttNames = ImmutableMap.copyOf(exprAttNamesBuilder);
        this.exprAttValues = ImmutableMap.copyOf(exprAttValuesBuilder);
        this.keyConditionExpr = String.join(" AND ", keyConditions);
        this.filterExpr = filterConditions.isEmpty() ? null : String.join(" AND ", filterConditions);
    }

    private static String parseHashKey(Multimap<String, HiveDdbQueryFilter> filters, Map<KeyType, String> keyAttributes,
                                       Map<String, String> exprAttNamesBuilder,
                                       Map<String, AttributeValue> exprAttValuesBuilder) {
        Collection<HiveDdbQueryFilter> hashFilters = filters.get(keyAttributes.get(KeyType.HASH));
        if (hashFilters.size() != 1) {
            String msg = String.format("There should be one filter on the hash key attribute "
                                       + "(found=%d, filters=%s)", hashFilters.size(), hashFilters);
            throw new IllegalArgumentException(msg);
        }
        HiveDdbQueryFilter hashKeyFilter = hashFilters.iterator().next();
        Operators hashOperator = Operators.valueOf(hashKeyFilter.getOperator().toUpperCase());
        if (!Operators.EQ.equals(hashOperator)) {
            String msg = String.format("Hash key attribute should use EQUALS operator"
                                       + "(found=%s, filters=%s)", hashOperator, hashKeyFilter);
            throw new IllegalArgumentException(msg);
        }
        exprAttNamesBuilder.put("#pk", hashKeyFilter.getAttribute());
        AttributeValue attValue = HiveDynamoDBTypeFactory
                .getTypeObjectFromDynamoDBType(hashKeyFilter.getAttributeType())
                .getDynamoDBData(hashKeyFilter.getValue(), hashKeyFilter.getValueOi(), true);
        exprAttValuesBuilder.put(":pkValue", attValue);
        return hashOperator.getCondition("#pk", ":pkValue");
    }

    private static Optional<String> parseSortKey(Multimap<String, HiveDdbQueryFilter> filters, Map<KeyType, String> keyAttributes,
                                                 Map<String, String> exprAttNamesBuilder,
                                                 Map<String, AttributeValue> exprAttValuesBuilder) {
        Collection<HiveDdbQueryFilter> sortKeyFilters = filters.get(keyAttributes.get(KeyType.RANGE));
        if (sortKeyFilters.isEmpty()) {
            return Optional.empty();
        }
        if (sortKeyFilters.size() > 1) {
            String msg = String.format("There should be at most one filter on the sort key attribute "
                                       + "(found=%d, filters=%s)", sortKeyFilters.size(), sortKeyFilters);
            throw new IllegalArgumentException(msg);
        }
        HiveDdbQueryFilter sortKeyFilter = sortKeyFilters.iterator().next();
        Operators sortKeyOperator = Operators.valueOf(sortKeyFilter.getOperator().toUpperCase());
        exprAttNamesBuilder.put("#sk", sortKeyFilter.getAttribute());
        AttributeValue attValue = HiveDynamoDBTypeFactory
                .getTypeObjectFromDynamoDBType(sortKeyFilter.getAttributeType())
                .getDynamoDBData(sortKeyFilter.getValue(), sortKeyFilter.getValueOi(), true);
        exprAttValuesBuilder.put(":skValue", attValue);
        return Optional.of(sortKeyOperator.getCondition("#sk", ":skValue"));
    }

    @RequiredArgsConstructor
    enum Operators {
        EQ(new ComparatorOperator("=")),
        LT(new ComparatorOperator("<")),
        LE(new ComparatorOperator("<=")),
        GT(new ComparatorOperator(">")),
        GE(new ComparatorOperator(">="));

        private final Operator op;

        String getCondition(String attAlias, String valueAlias) {
            return this.op.getCondition(attAlias, valueAlias);
        }

        private interface Operator {
            String getCondition(String attAlias, String valueAlias);
        }

        @RequiredArgsConstructor
        private static class ComparatorOperator implements Operator {
            private final String op;

            @Override
            public String getCondition(String attAlias, String valueAlias) {
                return attAlias + " " + this.op + " " + valueAlias;
            }
        }
    }
}
