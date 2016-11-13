package es.creation.client;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;

/**
 * Created by khiemtt on 11/13/16.
 */
@Component
public class EsClient {
    private static Logger logger = LoggerFactory.getLogger(EsClient.class);

    TransportClient client;

    public EsClient() throws UnknownHostException {
        try {
            client = new TransportClient()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        } catch (UnknownHostException e) {
            logger.error(e.getMessage(), e);
            throw e;
        }
    }

    @PostConstruct
    public void init() {
        createIndex("test-index", "test-type", getMappings());

        registerShutdownHook();
    }

    public void createIndex(String indexName, String indexType, String jsonMappings) {
        boolean isExisted =  client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists();
        if (!isExisted){
            CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate(indexName)
                    .addMapping(indexType, jsonMappings);
            CreateIndexResponse response = createIndexRequestBuilder.execute().actionGet();

            if(response.isAcknowledged()) {
                logger.info("Created index successfully");
            } else {
                logger.error("Unable to create index");
                throw new RuntimeException("Unable to create index");
            }
        } else {
            logger.info("Index is already existed");
        }
    }

    public String insert(String json) {

        IndexResponse response = client.prepareIndex("test-index", "test-type")
                .setSource(json)
                .execute()
                .actionGet();

        String _id = response.getId();
        long _version = response.getVersion();
        boolean created = response.isCreated();

        logger.info("Insert successfully. Response: [id={}, version={}, created={}]", _id, _version, created);

        return _id;
    }

    public String get(String id) {
        GetResponse response = client.prepareGet("test-index", "test-type", id)
                .execute()
                .actionGet();

        String _id = response.getId();
        String data = response.getSourceAsString();

        logger.info("Read successfully. Response: [id={}, data={}]", _id, data);

        return data;
    }

    private String getMappings() {
        String result = "";
        ClassLoader classLoader = getClass().getClassLoader();
        try {
            result = IOUtils.toString(classLoader.getResourceAsStream("mappings.json"), Charset.forName("utf-8"));
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }

        return result;
    }

    public void close() {
        client.close();
    }

    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    close();
                    logger.info("Stop EsClient gracefully.");
                } catch (Exception e) {
                    logger.error("Error when registering shutdown hook.", e);
                }
            }
        }));
    }
}
