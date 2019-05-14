package com.kafka.tutorial.tutorial3.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

/**
 * Created by Ghazi Naceur on 14/05/2019
 * Email: ghazi.ennacer@gmail.com
 */
public class ElasticsearchConsumer {

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = createClient();
        IndexRequest request = new IndexRequest("twitter", "tweets");
        request.source("test");
        IndexResponse response = client.index(request, RequestOptions.DEFAULT);
        System.out.println(response.getId());

        client.close();
    }

    private static RestHighLevelClient createClient() {
        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200));
        return new RestHighLevelClient(builder);
    }
}
