package com.klimber.hiveddbudtf.hive;

import java.util.List;
import lombok.Builder;
import lombok.Value;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

@Value
@Builder
public class HiveDdbQueryParameters {
    String tableName;
    String indexName;
    List<ColumnMapping> hiveDdbColumnMapping;
    List<TypeInfo> hiveTypes;
    
    @Value
    @Builder
    public static class ColumnMapping {
        String hiveColumn;
        String ddbAttName;
    }
}
