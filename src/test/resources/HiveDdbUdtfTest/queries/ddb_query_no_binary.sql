create table result as
select 
    ddb_query(
        named_struct(
            'tableName', 'EventHistory',
            'indexName', null,
            'hiveDdbColumnMapping', 'field_string:fieldString,field_bigint:fieldBigInt,field_double:fieldDouble,field_boolean:fieldBoolean,field_list_string:fieldListString,field_struct:fieldStruct,field_map_of_numbers:fieldMapOfNumbers,field_map_of_strings:fieldMapOfStrings,field_string_set:fieldStringSet,field_number_set:fieldNumberSet,field_null:fieldNull',
            'hiveTypeMapping', 'string,bigint,double,boolean,array<string>,struct<fieldString:string,fieldBigInt:bigint>,map<string,double>,map<string,string>,array<string>,array<double>,string'
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
