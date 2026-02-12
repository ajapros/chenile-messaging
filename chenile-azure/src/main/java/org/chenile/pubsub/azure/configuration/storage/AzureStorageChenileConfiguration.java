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

    @Value("${spring.chenile.storage.blob.endpoint}")
    private String storageBlobEndpoint;

    @Value("${spring.chenile.storage.blob.container}")
    private String storageBlobContainer;

//    @Bean
//    BlobContainerClientBuilder blobContainerClientBuilder() {
//
//        String accountName = "devstoreaccount1";
//        String accountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
//
//        StorageSharedKeyCredential credential =
//                new StorageSharedKeyCredential(accountName, accountKey);
//
//        AzureSasCredential sasCredential =
//                new AzureSasCredential("?sv=2024-11-04&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2026-05-31T13:15:37Z&st=2026-02-09T05:00:37Z&spr=https,http&sig=nlBT9DLuGoXtyywdsY2ZCR1gPMbdeX3%2Bs%2F8beo%2FUcQw%3D");
//
//
//        return new BlobContainerClientBuilder().credential(credential)
//                .endpoint(storageBlobEndpoint)
//                .containerName(storageBlobContainer);
//    }

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