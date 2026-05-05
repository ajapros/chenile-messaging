package org.chenile.pubsub.azure.configuration.storage;

import com.azure.core.credential.AzureSasCredential;
import com.azure.storage.blob.*;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AzureStorageChenileConfiguration {

    private static final Logger LOGGER = LoggerFactory.getLogger(AzureStorageChenileConfiguration.class);

    @Value("${chenile.storage.blob.endpoint}")
    private String storageBlobEndpoint;

    @Value("${chenile.storage.blob.container}")
    private String storageBlobContainer;

    @Bean
    public BlobContainerClientBuilder blobContainerClientBuilder(BlobStorageProperties properties) {

        BlobContainerClientBuilder builder = new BlobContainerClientBuilder()
                .endpoint(properties.getEndpoint())
                .containerName(properties.getContainer());

        if ("key".equalsIgnoreCase(properties.getCredentialType())) {
            StorageSharedKeyCredential credential =
                    new StorageSharedKeyCredential(properties.getAccountName(), properties.getAccountKey());
            builder.credential(credential);
        } else if ("sas".equalsIgnoreCase(properties.getCredentialType())) {
            AzureSasCredential credential = new AzureSasCredential(properties.getSasToken());
            builder.credential(credential);
        } else {
            throw new IllegalArgumentException("Unsupported blob credential type: " + properties.getCredentialType());
        }

        return builder;
    }


    @Bean
    BlobContainerAsyncClient blobContainerAsyncClient(BlobContainerClientBuilder blobContainerClientBuilder) {
        return blobContainerClientBuilder.buildAsyncClient();
    }


}
