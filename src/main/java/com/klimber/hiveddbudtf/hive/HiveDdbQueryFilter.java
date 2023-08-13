package com.klimber.hiveddbudtf.hive;

import lombok.Builder;
import lombok.Value;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

@Value
@Builder
public class HiveDdbQueryFilter {
    String attribute;
    String attributeType;
    String operator;
    Object value;
    ObjectInspector valueOi;
}
