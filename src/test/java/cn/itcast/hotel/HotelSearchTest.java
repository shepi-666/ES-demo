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
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

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

        handleResponse(resp);
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

        handleResponse(resp);
    }

    /**
     * 精确查询
     * @throws IOException
     */
    @Test
    public void testBoolean() throws IOException {
        // 准备request
        SearchRequest req = new SearchRequest("hotel");

        // 获取boolQuery对象
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        // 添加term
        boolQuery.must(QueryBuilders.termQuery("city", "北京"));
        boolQuery.filter(QueryBuilders.rangeQuery("price").lte(500));

        req.source().query(boolQuery);


        // 发送请求
        SearchResponse resp = client.search(req, RequestOptions.DEFAULT);

        handleResponse(resp);
    }

    /**
     * 分页和排序查询
     * @throws IOException
     */
    @Test
    public void testPageAndSort() throws IOException {
        // 准备request
        SearchRequest req = new SearchRequest("hotel");

        SearchSourceBuilder ssb = req.source();
        ssb.query(QueryBuilders.matchAllQuery());
        // 排序
        ssb.sort("price", SortOrder.ASC);
        // 分页
        ssb.from(0).size(5);


        // 发送请求
        SearchResponse resp = client.search(req, RequestOptions.DEFAULT);

        handleResponse(resp);
    }


    /**
     * 抽取处理查询结果的步骤，ctrl + alt + m
     * @param resp
     */
    private void handleResponse(SearchResponse resp) {
        // 解析结果
        SearchHits hits = resp.getHits();

        // 查询所有的条数
        long total = hits.getTotalHits().value;
        System.out.println("命中条数：" + total);

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

    @Test
    public void testSuggest() throws IOException {
        SearchRequest req = new SearchRequest("hotel");
        req.source().suggest(new SuggestBuilder().addSuggestion(
                "suggestions",
                SuggestBuilders.completionSuggestion("suggestion")
                .prefix("h")
                .skipDuplicates(true)
                .size(10)
        ));

        SearchResponse resp = client.search(req, RequestOptions.DEFAULT);


        Suggest suggest = resp.getSuggest();
        // 根据补全查询名称获取补全结果
        CompletionSuggestion suggestions = suggest.getSuggestion("suggestions");
        // 获取options
        List<CompletionSuggestion.Entry.Option> options = suggestions.getOptions();
        // 遍历
        for (CompletionSuggestion.Entry.Option option : options) {
            String text = option.getText().toString();
            System.out.println(text);
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
