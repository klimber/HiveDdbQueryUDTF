package com.klimber.hiveddbudtf;

import com.klimber.hiveddbudtf.client.ddb.DynamoDbClientWrapper;
import java.util.function.Supplier;
import org.mockito.Mockito;

public class MockedHiveDdbQueryUdtf extends HiveDdbQueryUdtf {
    static Supplier<DynamoDbClientWrapper> MOCK_SUPPLIER = () -> Mockito.mock(DynamoDbClientWrapper.class);

    MockedHiveDdbQueryUdtf() {
        super(MOCK_SUPPLIER.get());
    }
}
