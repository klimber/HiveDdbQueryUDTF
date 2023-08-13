package com.klimber.hiveddbudtf.client.ddb;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import static org.apache.hadoop.dynamodb.DynamoDBConstants.CUSTOM_CREDENTIALS_PROVIDER_CONF;

public class CredentialsProvider implements AWSCredentialsProvider {
    private final AWSCredentialsProvider delegate;

    public CredentialsProvider(Configuration conf) {
        this.delegate = this.getAWSCredentialsProvider(conf);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AWSCredentials getCredentials() {
        return this.delegate.getCredentials();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void refresh() {
        this.delegate.refresh();
    }

    private AWSCredentialsProvider getAWSCredentialsProvider(Configuration conf) {
        List<AWSCredentialsProvider> providers = new ArrayList<>();

        // try to load custom credential provider, fail if a provider is specified but cannot be
        // initialized
        String providerClass = conf.get(CUSTOM_CREDENTIALS_PROVIDER_CONF);
        if (!Strings.isNullOrEmpty(providerClass)) {
            try {
                providers.add(
                        (AWSCredentialsProvider) ReflectionUtils.newInstance(Class.forName(providerClass), conf)
                );
            } catch (ClassNotFoundException notFoundEx) {
                String msg = String.format("Unable to find custom credential provider " 
                                           + "(%s=%s)", CUSTOM_CREDENTIALS_PROVIDER_CONF, providerClass);
                throw new RuntimeException(msg, notFoundEx);
            } catch (ClassCastException castEx) {
                String msg = String.format("Custom credential provider should implement '%s'",
                                           AWSCredentialsProvider.class.getName());
                throw new RuntimeException(msg, castEx);
            }
        }

        // Fallback to EMR Instance Profile credentials
        providers.add(InstanceProfileCredentialsProvider.getInstance());
        AWSCredentialsProviderChain providerChain = new AWSCredentialsProviderChain(
                providers.toArray(new AWSCredentialsProvider[0]));
        providerChain.setReuseLastProvider(true);
        return providerChain;
    }
}