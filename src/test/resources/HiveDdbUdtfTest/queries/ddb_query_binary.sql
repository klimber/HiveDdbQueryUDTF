create table result as
select 
    ddb_query(
        named_struct(
            'tableName', 'EventHistory',
            'indexName', null,
            'hiveDdbColumnMapping', 'field_binary:fieldBinary,field_binary_set:fieldBinarySet',
            'hiveTypeMapping', 'binary,array<binary>'
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
