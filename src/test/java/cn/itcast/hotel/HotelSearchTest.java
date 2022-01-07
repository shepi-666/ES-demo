package cn.itcast.hotel;

import cn.itcast.hotel.pojo.HotelDoc;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static cn.itcast.hotel.constants.HotelConstants.MAPPING_TEMPLATE;

public class HotelSearchTest {
    private RestHighLevelClient client;

    /**
     * matchAll：查询所有的数据
     * @throws IOException
     */
    @Test
    public void testMatchAll() throws IOException {
        // 准备request
        SearchRequest req = new SearchRequest("hotel");

        // 准备dsl参数
        req.source().query(QueryBuilders.matchAllQuery());

        // 发送请求
        SearchResponse resp = client.search(req, RequestOptions.DEFAULT);

        // 解析结果
        SearchHits hits = resp.getHits();

        // 查询所有的条数
        long total = hits.getTotalHits().value;

        // 查询所有的结果
        SearchHit[] lists = hits.getHits();
        for (SearchHit hit : lists) {
            // 得到source
            String json = hit.getSourceAsString();

            // 将JSON数据反序列化
            HotelDoc doc = JSON.parseObject(json, HotelDoc.class);
            System.out.println(doc);
        }
    }

    /**
     * match：全文检索
     * @throws IOException
     */
    @Test
    public void testMatch() throws IOException {
        // 准备request
        SearchRequest req = new SearchRequest("hotel");

        // 准备dsl参数
        req.source().query(QueryBuilders.matchQuery("condition", "希尔顿"));

        // 发送请求
        SearchResponse resp = client.search(req, RequestOptions.DEFAULT);

        // 解析结果
        SearchHits hits = resp.getHits();

        // 查询所有的条数
        long total = hits.getTotalHits().value;
        System.out.println(total);

        // 查询所有的结果
        SearchHit[] lists = hits.getHits();
        for (SearchHit hit : lists) {
            // 得到source
            String json = hit.getSourceAsString();

            // 将JSON数据反序列化
            HotelDoc doc = JSON.parseObject(json, HotelDoc.class);
            System.out.println(doc);
        }
    }

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


}
