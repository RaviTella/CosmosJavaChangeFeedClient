package com.example.CosmosJavaChangeFeedClient;

import com.azure.cosmos.*;
import com.azure.cosmos.models.ChangeFeedProcessorOptions;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.ThroughputProperties;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Service;
import reactor.core.scheduler.Schedulers;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Service
public class CosmosDB {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private static final String FEED_CONTAINER ="FEED_CONTAINER";
    private static final String LEASE_CONTAINER ="LEASE_CONTAINER";

    private Map<String, CosmosAsyncContainer> containerNameToContainer = new HashMap<>();;
    private CosmosAsyncClient client;
    private CosmosAsyncDatabase database;
    @Autowired
    @Qualifier("gremlinDataSourcePropertiesBean")
    CosmosProperties gremlinDataSourceProperties;

    @Autowired
    @Qualifier("sqlDataSourcePropertiesBean")
    CosmosProperties sqlDataSourceProperties;

    @Bean("gremlinDataSourcePropertiesBean")
    @ConfigurationProperties(prefix = "azure.cosmos.gremlin")
    public CosmosProperties gremlin() {
        return new CosmosProperties();
    }

    @Bean("sqlDataSourcePropertiesBean")
    @ConfigurationProperties(prefix = "azure.cosmos.sql")
    public CosmosProperties sql() {
        return new CosmosProperties();
    }


    @PostConstruct
    private void start(){
        cosmosSetup();
        startChangeFeedClient();
    }

    private void cosmosSetup() {
                CosmosAsyncContainer gremlinCosmosAsyncContainer = cosmosCreateResources( gremlinDataSourceProperties.getContainer());
                containerNameToContainer.put(FEED_CONTAINER, gremlinCosmosAsyncContainer);
                CosmosAsyncContainer sqlCosmosAsyncContainer = cosmosCreateResources( sqlDataSourceProperties.getContainer());
                containerNameToContainer.put(LEASE_CONTAINER, sqlCosmosAsyncContainer);
    }

    private CosmosAsyncContainer cosmosCreateResources(String containerName) {
        CosmosContainerProperties containerProperties;
        CosmosAsyncClient cosmosAsyncClient;
        String databaseName;
        if (containerName.contains("-lease")) {
            containerProperties = new CosmosContainerProperties(containerName,"/id");
            cosmosAsyncClient = buildAndGetClient(sqlDataSourceProperties.getUri(),sqlDataSourceProperties.getKey());
            databaseName = sqlDataSourceProperties.getDatabase();
        } else{
            containerProperties = new CosmosContainerProperties(containerName, "/partitionkey");
            cosmosAsyncClient = buildAndGetClient(gremlinDataSourceProperties.getUri(),gremlinDataSourceProperties.getKey());
            databaseName = gremlinDataSourceProperties.getDatabase();
        }

        return  cosmosAsyncClient
                .createDatabaseIfNotExists(databaseName)
                .flatMap(databaseResponse -> {
                    database = client.getDatabase(databaseResponse
                            .getProperties()
                            .getId());
                    return database
                            .createContainerIfNotExists(containerProperties, ThroughputProperties.createManualThroughput(400));
                })
                .map(containerResponse -> {
                    return database.getContainer(containerResponse
                            .getProperties()
                            .getId());
                })
                .block();
    }


    private CosmosAsyncClient buildAndGetClient(String endpoint, String key) {
            client = new CosmosClientBuilder()
                    .endpoint(endpoint)
                    .key(key)
                    .consistencyLevel(ConsistencyLevel.SESSION)
                    .contentResponseOnWriteEnabled(true)
                    .buildAsyncClient();
            return client;
    }

    public CosmosAsyncContainer getContainer(String name) {
        return this.containerNameToContainer.get(name);
    }


    public void startChangeFeedClient() {
        com.azure.cosmos.ChangeFeedProcessor changeFeedProcessorInstance = getChangeFeedProcessor("GraphHost_1", getContainer(FEED_CONTAINER), getContainer(LEASE_CONTAINER));
        changeFeedProcessorInstance
                .start()
                .subscribeOn(Schedulers.elastic())
                .subscribe();
    }


    public ChangeFeedProcessor getChangeFeedProcessor(String hostName, CosmosAsyncContainer feedContainer, CosmosAsyncContainer leaseContainer) {
        ChangeFeedProcessorOptions changeFeedOptions = new ChangeFeedProcessorOptions();
        changeFeedOptions.setFeedPollDelay(Duration.ofSeconds(20));
        changeFeedOptions.setStartFromBeginning(true);
        return new ChangeFeedProcessorBuilder()
                .options(changeFeedOptions)
                .hostName(hostName)
                .feedContainer(feedContainer)
                .leaseContainer(leaseContainer)
                .handleChanges((List<JsonNode> docs) -> {
                    for (JsonNode document : docs) {
                        logger.info(document.toPrettyString());
                    }

                })
                .buildChangeFeedProcessor();
    }
}