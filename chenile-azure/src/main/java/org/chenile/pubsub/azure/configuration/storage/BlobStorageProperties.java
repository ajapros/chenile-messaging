package org.chenile.pubsub.azure.configuration.storage;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "chenile.storage.blob")
public class BlobStorageProperties {

    private String endpoint;
    private String container;
    private String credentialType; // "key" or "sas"

    private String accountName;
    private String accountKey;

    private String sasToken;

    // Getters & Setters

    public String getEndpoint() { return endpoint; }
    public void setEndpoint(String endpoint) { this.endpoint = endpoint; }

    public String getContainer() { return container; }
    public void setContainer(String container) { this.container = container; }

    public String getCredentialType() { return credentialType; }
    public void setCredentialType(String credentialType) { this.credentialType = credentialType; }

    public String getAccountName() { return accountName; }
    public void setAccountName(String accountName) { this.accountName = accountName; }

    public String getAccountKey() { return accountKey; }
    public void setAccountKey(String accountKey) { this.accountKey = accountKey; }

    public String getSasToken() { return sasToken; }
    public void setSasToken(String sasToken) { this.sasToken = sasToken; }
}
