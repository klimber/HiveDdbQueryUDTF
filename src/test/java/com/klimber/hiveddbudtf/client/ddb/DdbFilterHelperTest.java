package com.klimber.hiveddbudtf.client.ddb;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.klimber.hiveddbudtf.hive.HiveDdbQueryFilter;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.*;

class DdbFilterHelperTest {
    private String pkAtt;
    private String pkAttType;
    private String pkAttValue;
    private HiveDdbQueryFilter pkFilter;
    private String skAtt;
    private String skAttType;
    private String skAttValue;
    private HiveDdbQueryFilter skFilter;

    @BeforeEach
    void setUp() {
        this.pkAtt = "PartitionKey";
        this.pkAttType = "S";
        this.pkAttValue = "TargetPkValue";
        this.pkFilter = HiveDdbQueryFilter.builder()
                                          .attribute(this.pkAtt)
                                          .attributeType(this.pkAttType)
                                          .operator("EQ")
                                          .value(this.pkAttValue)
                                          .valueOi(javaStringObjectInspector)
                                          .build();
        this.skAtt = "RangeKey";
        this.skAttType = "S";
        this.skAttValue = "TargetSkValue";
        this.skFilter = HiveDdbQueryFilter.builder()
                                          .attribute(this.skAtt)
                                          .attributeType(this.skAttType)
                                          .operator("EQ")
                                          .value(this.skAttValue)
                                          .valueOi(javaStringObjectInspector)
                                          .build();
    }
    
    @Test
    void noFilterTest() {
        Multimap<String, HiveDdbQueryFilter> filters = ImmutableMultimap.of();
        Map<KeyType, String> keyAttributes = ImmutableMap.of(KeyType.HASH, this.pkAtt);
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                                                              () -> new DdbFilterHelper(filters, keyAttributes));
        Assertions.assertTrue(ex.getMessage().contains("There should be one filter on the hash key attribute"));
    }

    @Test
    void noPkFilterTest() {
        HiveDdbQueryFilter filter = HiveDdbQueryFilter.builder()
                                                      .attribute("SomeField")
                                                      .attributeType("S")
                                                      .operator("EQ")
                                                      .value("SomeValue")
                                                      .valueOi(javaStringObjectInspector)
                                                      .build();
        Multimap<String, HiveDdbQueryFilter> filters = ImmutableMultimap.of("SomeField", filter);
        Map<KeyType, String> keyAttributes = ImmutableMap.of(KeyType.HASH, this.pkAtt);
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                                                              () -> new DdbFilterHelper(filters, keyAttributes));
        Assertions.assertTrue(ex.getMessage().contains("There should be one filter on the hash key attribute"));
    }

    @Test
    void pkFilterTest() {
        Multimap<String, HiveDdbQueryFilter> filters = ImmutableMultimap.of(this.pkAtt, this.pkFilter);
        Map<KeyType, String> keyAttributes = ImmutableMap.of(KeyType.HASH, this.pkAtt,
                                                             KeyType.RANGE, this.skAtt);
        DdbFilterHelper helper = new DdbFilterHelper(filters, keyAttributes);

        Assertions.assertNull(helper.getFilterExpr());
        Assertions.assertEquals("#pk = :pkValue", helper.getKeyConditionExpr());
        Map<String, String> expectedAttNames = ImmutableMap.of("#pk", this.pkAtt);
        Assertions.assertEquals(expectedAttNames, helper.getExprAttNames());
        Map<String, AttributeValue> expectedAttValues = ImmutableMap.of(":pkValue",
                                                                        new AttributeValue(this.pkAttValue));
        Assertions.assertEquals(expectedAttValues, helper.getExprAttValues());
    }

    @Test
    void pkWrongOperatorFilterTest() {
        HiveDdbQueryFilter filter = HiveDdbQueryFilter.builder()
                                                      .attribute(this.pkAtt)
                                                      .attributeType("S")
                                                      .operator("GT")
                                                      .value("SomeValue")
                                                      .valueOi(javaStringObjectInspector)
                                                      .build();
        Multimap<String, HiveDdbQueryFilter> filters = ImmutableMultimap.of(this.pkAtt, filter);
        Map<KeyType, String> keyAttributes = ImmutableMap.of(KeyType.HASH, this.pkAtt,
                                                             KeyType.RANGE, this.skAtt);
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                                                              () -> new DdbFilterHelper(filters, keyAttributes));
        Assertions.assertTrue(ex.getMessage().contains("Hash key attribute should use EQUALS operator"));
    }

    @Test
    void multiplePkFilterTest() {
        HiveDdbQueryFilter filter = HiveDdbQueryFilter.builder()
                                                      .attribute(this.pkAtt)
                                                      .attributeType("S")
                                                      .operator("EQ")
                                                      .value("SomeValue")
                                                      .valueOi(javaStringObjectInspector)
                                                      .build();
        Multimap<String, HiveDdbQueryFilter> filters = ImmutableMultimap.of(this.pkAtt, this.pkFilter,
                                                                            this.pkAtt, filter);
        Map<KeyType, String> keyAttributes = ImmutableMap.of(KeyType.HASH, this.pkAtt,
                                                             KeyType.RANGE, this.skAtt);
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                                                              () -> new DdbFilterHelper(filters, keyAttributes));
        Assertions.assertTrue(ex.getMessage().contains("There should be one filter on the hash key attribute"));
    }

    @Test
    void pkAndSkFilterTest() {
        Multimap<String, HiveDdbQueryFilter> filters = ImmutableMultimap.of(this.pkAtt, this.pkFilter,
                                                                            this.skAtt, this.skFilter);
        Map<KeyType, String> keyAttributes = ImmutableMap.of(KeyType.HASH, this.pkAtt,
                                                             KeyType.RANGE, this.skAtt);
        DdbFilterHelper helper = new DdbFilterHelper(filters, keyAttributes);

        Assertions.assertNull(helper.getFilterExpr());
        Assertions.assertEquals("#pk = :pkValue AND #sk = :skValue", helper.getKeyConditionExpr());
        Map<String, String> expectedAttNames = ImmutableMap.of("#pk", this.pkAtt,
                                                               "#sk", this.skAtt);
        Assertions.assertEquals(expectedAttNames, helper.getExprAttNames());
        Map<String, AttributeValue> expectedAttValues = ImmutableMap.of(":pkValue",
                                                                        new AttributeValue(this.pkAttValue),
                                                                        ":skValue",
                                                                        new AttributeValue(this.skAttValue));
        Assertions.assertEquals(expectedAttValues, helper.getExprAttValues());
    }

    @Test
    void pkAndMultipleSkFilterTest() {
        HiveDdbQueryFilter filter = HiveDdbQueryFilter.builder()
                                                      .attribute(this.skAtt)
                                                      .attributeType("S")
                                                      .operator("EQ")
                                                      .value("SomeValue")
                                                      .valueOi(javaStringObjectInspector)
                                                      .build();
        Multimap<String, HiveDdbQueryFilter> filters = ImmutableMultimap.of(this.pkAtt, this.pkFilter,
                                                                            this.skAtt, this.skFilter,
                                                                            this.skAtt, filter);
        Map<KeyType, String> keyAttributes = ImmutableMap.of(KeyType.HASH, this.pkAtt,
                                                             KeyType.RANGE, this.skAtt);
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                                                              () -> new DdbFilterHelper(filters, keyAttributes));
        Assertions.assertTrue(ex.getMessage().contains("There should be at most one filter on the sort key attribute"));
    }

    @Test
    void multipleFilterTest() {
        HiveDdbQueryFilter f1v1 = HiveDdbQueryFilter.builder()
                                                    .attribute("FieldOne")
                                                    .attributeType("S")
                                                    .operator("GT")
                                                    .value("FieldOneValue")
                                                    .valueOi(javaStringObjectInspector)
                                                    .build();
        HiveDdbQueryFilter f1v2 = HiveDdbQueryFilter.builder()
                                                    .attribute("FieldOne")
                                                    .attributeType("S")
                                                    .operator("LT")
                                                    .value("FieldOneValueOther")
                                                    .valueOi(javaStringObjectInspector)
                                                    .build();
        HiveDdbQueryFilter f2v1 = HiveDdbQueryFilter.builder()
                                                    .attribute("FieldTwo")
                                                    .attributeType("S")
                                                    .operator("GE")
                                                    .value("FieldTwoValue")
                                                    .valueOi(javaStringObjectInspector)
                                                    .build();
        HiveDdbQueryFilter f2v2 = HiveDdbQueryFilter.builder()
                                                    .attribute("FieldTwo")
                                                    .attributeType("S")
                                                    .operator("LE")
                                                    .value("FieldTwoValueOther")
                                                    .valueOi(javaStringObjectInspector)
                                                    .build();
        Multimap<String, HiveDdbQueryFilter> filters = ImmutableMultimap.<String, HiveDdbQueryFilter>builder()
                                                                        .put(this.pkAtt, this.pkFilter)
                                                                        .put(this.skAtt, this.skFilter)
                                                                        .put("FieldOne", f1v1)
                                                                        .put("FieldOne", f1v2)
                                                                        .put("FieldTwo", f2v1)
                                                                        .put("FieldTwo", f2v2)
                                                                        .build();
        Map<KeyType, String> keyAttributes = ImmutableMap.of(KeyType.HASH, this.pkAtt,
                                                             KeyType.RANGE, this.skAtt);
        DdbFilterHelper helper = new DdbFilterHelper(filters, keyAttributes);

        Assertions.assertEquals("#a1 > :a1v1 AND #a1 < :a1v2 AND "
                                + "#a2 >= :a2v1 AND #a2 <= :a2v2", helper.getFilterExpr());
        Assertions.assertEquals("#pk = :pkValue AND #sk = :skValue", helper.getKeyConditionExpr());
        Map<String, String> expectedAttNames = ImmutableMap.of("#pk", this.pkAtt,
                                                               "#sk", this.skAtt,
                                                               "#a1", "FieldOne",
                                                               "#a2", "FieldTwo");
        Assertions.assertEquals(expectedAttNames, helper.getExprAttNames());
        Map<String, AttributeValue> expectedAttValues = ImmutableMap.<String, AttributeValue>builder()
                                                                    .put(":pkValue",
                                                                         new AttributeValue(this.pkAttValue))
                                                                    .put(":skValue",
                                                                         new AttributeValue(this.skAttValue))
                                                                    .put(":a1v1",
                                                                         new AttributeValue("FieldOneValue"))
                                                                    .put(":a1v2",
                                                                         new AttributeValue("FieldOneValueOther"))
                                                                    .put(":a2v1",
                                                                         new AttributeValue("FieldTwoValue"))
                                                                    .put(":a2v2",
                                                                         new AttributeValue("FieldTwoValueOther"))
                                                                    .build();
        Assertions.assertEquals(expectedAttValues, helper.getExprAttValues());
    }
}