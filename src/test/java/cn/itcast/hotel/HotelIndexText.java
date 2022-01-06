package cn.itcast.hotel;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static cn.itcast.hotel.constants.HotelConstants.MAPPING_TEMPLATE;

public class HotelIndexText {
    private RestHighLevelClient client;

    @BeforeEach
    void setUp() {
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://101.35.51.125:9200")
        ));
    }

    @AfterEach
    void tearDown() throws IOException {
        this.client.close();
    }

    @Test
    public void testInit() {
        System.out.println(client);
    }

    /**
     * 创建索引库
     */
    @Test
    public void createHotelIndex() throws IOException {
        // 1.创建Request对象
        CreateIndexRequest req = new CreateIndexRequest("hotel");
        // 2.准备请求参数：DSL语句
        req.source(MAPPING_TEMPLATE, XContentType.JSON);

        // 3.发出请求
        client.indices().create(req, RequestOptions.DEFAULT);
    }

    @Test
    public void testDeleteHotelIndex() throws IOException {
        // 1.创建Request对象
        DeleteIndexRequest req = new DeleteIndexRequest("hotel");

        // 2.发出请求
        client.indices().delete(req, RequestOptions.DEFAULT);
    }
}
