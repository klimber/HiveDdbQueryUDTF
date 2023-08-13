package com.klimber.hiveddbudtf.client.ddb;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import java.util.UUID;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.apache.hadoop.dynamodb.DynamoDBConstants.CUSTOM_CREDENTIALS_PROVIDER_CONF;

class CredentialsProviderTest {
    private static AWSCredentialsProvider mockProvider;

    @BeforeEach
    void setUp() {
        mockProvider = Mockito.mock(AWSCredentialsProvider.class);
    }

    @Test
    void createWithDefaultProvider() {
        HiveConf conf = new HiveConf();
        CredentialsProvider provider = new CredentialsProvider(conf);
        Assertions.assertNotNull(provider);
    }

    @Test
    void getCredentials() {
        BasicAWSCredentials credentials = new BasicAWSCredentials(UUID.randomUUID().toString(),
                                                                  UUID.randomUUID().toString());
        Mockito.doReturn(credentials)
               .when(mockProvider)
               .getCredentials();

        HiveConf conf = new HiveConf();
        conf.set(CUSTOM_CREDENTIALS_PROVIDER_CONF,
                 "com.klimber.hiveddbudtf.client.ddb.CredentialsProviderTest$DummyCustomProvider");
        CredentialsProvider provider = new CredentialsProvider(conf);
        provider.getCredentials();
        Mockito.verify(mockProvider, Mockito.times(1)).getCredentials();
        Mockito.verify(mockProvider, Mockito.times(1)).refresh(); // For setConf
    }

    @Test
    void refresh() {
        HiveConf conf = new HiveConf();
        conf.set(CUSTOM_CREDENTIALS_PROVIDER_CONF,
                 "com.klimber.hiveddbudtf.client.ddb.CredentialsProviderTest$DummyCustomProvider");
        CredentialsProvider provider = new CredentialsProvider(conf);
        provider.refresh();
        Mockito.verify(mockProvider, Mockito.times(2)).refresh(); // 2 due to setConf
    }

    @Test
    void classNotFoundTest() {
        HiveConf conf = new HiveConf();
        conf.set(CUSTOM_CREDENTIALS_PROVIDER_CONF,
                 "com.klimber.hiveddbudtf.client.ddb.CredentialsProviderTest$SomeCustomProvider");
        RuntimeException ex = Assertions.assertThrows(RuntimeException.class,
                                                      () -> new CredentialsProvider(conf));
        Assertions.assertTrue(ex.getCause() instanceof ClassNotFoundException);
    }

    @Test
    void classCastExceptionTest() {
        HiveConf conf = new HiveConf();
        conf.set(CUSTOM_CREDENTIALS_PROVIDER_CONF,
                 "com.klimber.hiveddbudtf.client.ddb.CredentialsProviderTest");
        RuntimeException ex = Assertions.assertThrows(RuntimeException.class,
                                                      () -> new CredentialsProvider(conf));
        Assertions.assertTrue(ex.getCause() instanceof ClassCastException);
    }

    static class DummyCustomProvider implements AWSCredentialsProvider, Configurable {

        @Override
        public AWSCredentials getCredentials() {
            return mockProvider.getCredentials();
        }

        @Override
        public void refresh() {
            mockProvider.refresh();
        }

        @Override
        public void setConf(Configuration configuration) {
            mockProvider.refresh();
        }

        @Override
        public Configuration getConf() {
            return null;
        }
    }
}