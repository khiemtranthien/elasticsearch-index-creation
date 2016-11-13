package es.creation;

import es.creation.client.EsClient;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

/**
 * Created by khiemtt on 11/13/16.
 */
@SpringBootApplication
public class App {
    public static void main(String[] args) {
        ApplicationContext ctx = SpringApplication.run(App.class, args);

        EsClient esClient = (EsClient) ctx.getBean(EsClient.class);

        String json = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";

        String id = esClient.insert(json);

        esClient.get(id);
    }
}
