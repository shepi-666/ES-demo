package cn.itcast.hotel;

import cn.itcast.hotel.pojo.HotelDoc;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

public class HotelAggregateTest {
    private RestHighLevelClient client;

    @Test
    public void testAggregate() throws IOException {
        // 1 准备req
        SearchRequest req = new SearchRequest("hotel");

        // 2 准备DSL
        // 2.1 设置size
        req.source().size(0);
        // 2.2 聚合
        req.source().aggregation(AggregationBuilders
                .terms("bandAgg")
                .field("brand")
                .size(20) // 显示的个数
        );


        // 3 发出请求
        SearchResponse resp = client.search(req, RequestOptions.DEFAULT);

        // 4 解析结果
        getResult(resp);
    }

    /**
     * 聚合结果的解析
     */
    public void getResult(SearchResponse resp) {
        // 解析聚合结果
        Aggregations aggregations = resp.getAggregations();

        // 根据聚合名称获取聚合结果
        Terms brandTerms = aggregations.get("bandAgg");

        // 获取桶子
        List<? extends Terms.Bucket> buckets = brandTerms.getBuckets();

        // 遍历桶子
        for (Terms.Bucket bucket : buckets) {
            String brandName = bucket.getKeyAsString();
            long count = bucket.getDocCount();
            System.out.println("品牌: " + brandName + "\t\t数量: " + count);
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
