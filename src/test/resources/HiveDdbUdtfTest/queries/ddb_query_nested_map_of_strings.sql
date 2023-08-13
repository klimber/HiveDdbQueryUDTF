create table result as
select 
    ddb_query(
        named_struct(
            'tableName', 'EventHistory',
            'indexName', null,
            'hiveDdbColumnMapping', 'field_nested_map_of_strings:fieldNestedMapOfStrings',
            'hiveTypeMapping', 'map<string,map<string,string>>'
        ),
        struct(
            named_struct(
                'attribute', 'entityId',
                'attributeType', 'S',
                'operator', 'EQ',
                'value', entity_id
            )
        )
    )
from query_keys;
