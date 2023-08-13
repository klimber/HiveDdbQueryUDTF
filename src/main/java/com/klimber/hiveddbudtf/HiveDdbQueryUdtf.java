package com.klimber.hiveddbudtf;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.collect.Multimap;
import com.klimber.hiveddbudtf.client.ddb.CredentialsProvider;
import com.klimber.hiveddbudtf.client.ddb.DynamoDbClientWrapper;
import com.klimber.hiveddbudtf.client.ddb.DynamoDbClientWrapperImpl;
import com.klimber.hiveddbudtf.client.ddb.DynamoDbTypeFinder;
import com.klimber.hiveddbudtf.hive.HiveDdbQueryFilter;
import com.klimber.hiveddbudtf.hive.HiveDdbQueryFiltersConverter;
import com.klimber.hiveddbudtf.hive.HiveDdbQueryParameters;
import com.klimber.hiveddbudtf.hive.HiveDdbQueryParameters.ColumnMapping;
import com.klimber.hiveddbudtf.hive.HiveDdbQueryParametersConverter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBNullType;
import org.apache.hadoop.hive.dynamodb.type.HiveDynamoDBType;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.STRUCT;

@Description(name = "ddb_query",
        value = "_FUNC_(a, b) - Queries DynamoDB using the parameters from struct a"
                + " and the filters from struct b."
                + " The resulting items are returned as multiple rows and columns")
public class HiveDdbQueryUdtf extends GenericUDTF {
    private transient DynamoDbClientWrapper ddbWrapper;
    private transient HiveDdbQueryParametersConverter queryParamsConverter;
    private transient HiveDdbQueryFiltersConverter queryFiltersConverter;
    private AWSCredentialsProvider awsCredProvider;

    public HiveDdbQueryUdtf() {
    }

    // For tests
    HiveDdbQueryUdtf(DynamoDbClientWrapper mockWrapper) {
        this.ddbWrapper = mockWrapper;
    }

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        List<? extends StructField> inputFields = argOIs.getAllStructFieldRefs();
        if (inputFields.size() != 2) {
            throw new UDFArgumentLengthException("DDB_QUERY requires 2 arguments "
                                                 + "(found=" + inputFields.size() + ")");
        }

        ObjectInspector fieldOI = inputFields.get(0).getFieldObjectInspector();
        if (!STRUCT.equals(fieldOI.getCategory())) {
            throw new UDFArgumentException("DDB_QUERY first argument should be a struct "
                                           + "(found=" + fieldOI.getCategory() + ")");
        }
        this.queryParamsConverter = new HiveDdbQueryParametersConverter((StructObjectInspector) fieldOI);

        fieldOI = inputFields.get(1).getFieldObjectInspector();
        if (!STRUCT.equals(fieldOI.getCategory())) {
            throw new UDFArgumentException("DDB_QUERY second argument should be a struct "
                                           + "(found=" + fieldOI.getCategory() + ")");
        }
        this.queryFiltersConverter = new HiveDdbQueryFiltersConverter((StructObjectInspector) fieldOI);

        List<String> fieldNames = this.queryParamsConverter
                .getHiveDdbColumnMapping().stream()
                .map(ColumnMapping::getHiveColumn)
                .collect(Collectors.toList());
        List<ObjectInspector> fieldOIs = this.queryParamsConverter
                .getHiveTypes().stream()
                .map(TypeInfoUtils::getStandardJavaObjectInspectorFromTypeInfo)
                .collect(Collectors.toList());

        /*
         * SessionState won't be available at EMR nodes, so initialize
         * anything that requires it during query planning.
         */
        if (Objects.isNull(this.awsCredProvider) && Objects.nonNull(SessionState.get())) {
            this.awsCredProvider = new CredentialsProvider(SessionState.get().getConf());
        }

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        /*
         * Delay DDB client initialization to process because UDTF will be serialized
         * at plan creation and deserialized on EMR nodes.
         */
        if (Objects.isNull(this.ddbWrapper)) {
            this.ddbWrapper = new DynamoDbClientWrapperImpl(this.awsCredProvider);
        }
        HiveDdbQueryParameters params = this.queryParamsConverter.convert(args[0]);
        Multimap<String, HiveDdbQueryFilter> filters = this.queryFiltersConverter.convert(args[1]);
        Stream<Map<String, AttributeValue>> queryResults = this.ddbWrapper.queryTable(params, filters);

        Iterator<Object[]> recordIterator = queryResults.map(record -> this.toHiveData(record, params)).iterator();

        while (recordIterator.hasNext()) {
            this.forward(recordIterator.next());
        }
    }

    private Object[] toHiveData(Map<String, AttributeValue> record, HiveDdbQueryParameters params) {
        Object[] row = new Object[params.getHiveDdbColumnMapping().size()];
        for (int i = 0; i < params.getHiveDdbColumnMapping().size(); i++) {
            String ddbAttName = params.getHiveDdbColumnMapping().get(i).getDdbAttName();
            TypeInfo type = params.getHiveTypes().get(i);
            ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(type);
            AttributeValue attValue = record.get(ddbAttName);
            HiveDynamoDBType hiveDdbType = DynamoDbTypeFinder.forAttributeValue(attValue);
            if (!(hiveDdbType instanceof HiveDynamoDBNullType) && !hiveDdbType.supportsHiveType(type)) {
                String msg = String.format("Hive type '%s' does not support DynamoDB type '%s' (ddbAttributeName=%s)",
                                           type, hiveDdbType.getDynamoDBType(), ddbAttName);
                throw new IllegalArgumentException(msg);
            }
            row[i] = hiveDdbType.getHiveData(attValue, oi);
        }
        return row;
    }

    @Override
    public void close() {
    }
}