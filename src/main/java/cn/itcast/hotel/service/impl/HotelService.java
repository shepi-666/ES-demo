package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {
    @Resource
    private RestHighLevelClient client;

    @Override
    public PageResult search(RequestParams params) {
        try {
            // 1 准备request
            SearchRequest req = new SearchRequest("hotel");

            QueryBuilder qb = getQueryBuilder(params);


            req.source().query(qb);
            // 2.2 分页
            Integer page = params.getPage();
            Integer size = params.getSize();
            req.source().from((page - 1) * size).size(size);

            // 2.3 添加排序功能
            String location = params.getLocation();
            if (location != null && !"".equals(location)) {
                req.source().sort(SortBuilders
                        .geoDistanceSort("location", new GeoPoint(location))
                        .order(SortOrder.ASC)
                        .unit(DistanceUnit.KILOMETERS)
                );
            }

            // 3 发送请求
            SearchResponse resp = client.search(req, RequestOptions.DEFAULT);

            return handleResponse(resp);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, List<String>> filters(RequestParams params) {
        // 首先确定我们需要聚合的关键词
        List<String> keywords = new ArrayList<>();
        keywords.add("city");
        keywords.add("brand");
        keywords.add("starName");

        // 首先获取req
        SearchRequest req = new SearchRequest("hotel");
        SearchSourceBuilder builder = req.source();
        // 聚合查询信息
        QueryBuilder queryBuilder = getQueryBuilder(params);
        builder.query(queryBuilder);

        Map<String, List<String>> res = new HashMap<>();
        for (String keyword : keywords) {
            getAggregation(keyword, builder);
        }

        // 发送请求
        try {
            SearchResponse resp = client.search(req, RequestOptions.DEFAULT);
            // 对结果集进行处理
            getResult(resp, keywords, res);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return res;
    }

    private SearchSourceBuilder getAggregation(String keyword, SearchSourceBuilder builder) {
        // 2 准备DSL
        // 2.1 设置size
        builder.size(0);
        // 2.2 聚合
        builder.aggregation(AggregationBuilders
                .terms(keyword + "Agg")
                .field(keyword)
                .size(100) // 显示的个数
        );
        return builder;
    }

    /**
     * 聚合结果解析
     */
    public void getResult(SearchResponse resp, List<String> keywords, Map<String, List<String>> res) {
        // 解析聚合结果
        Aggregations aggregations = resp.getAggregations();

        for (String keyword : keywords) {
            // 根据聚合名称获取聚合结果
            Terms brandTerms = aggregations.get(keyword + "Agg");

            // 获取桶子
            List<? extends Terms.Bucket> buckets = brandTerms.getBuckets();

            List<String> temp = new ArrayList<>();
            // 遍历桶子
            for (Terms.Bucket bucket : buckets) {
                String terms = bucket.getKeyAsString();
                temp.add(terms);
            }

            res.put(keyword, temp);
        }


    }

    /**
     * 获取queryBuilder的方法
     * @param params
     * @return
     */
    private QueryBuilder getQueryBuilder(RequestParams params) {
        // 2 准备dsl
        // 2.1 query
        // 构建booleanquery
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        // must：关键字搜索
        String key = params.getKey();
        // 健壮性的判断
        if (key == null || "".equals(key)) {
            boolQuery.must(QueryBuilders.matchAllQuery());
        } else {
            boolQuery.must(QueryBuilders.matchQuery("condition", key));
        }
        // 条件过滤
        // 城市条件
        if (params.getCity() != null &&  !"".equals(params.getCity())) {
            // 将查询条件放在filter中防止算分
            boolQuery.filter(QueryBuilders.termQuery("city", params.getCity()));
        }

        // 品牌条件
        if (params.getBrand() != null &&  !"".equals(params.getBrand())) {
            // 将查询条件放在filter中防止算分
            boolQuery.filter(QueryBuilders.termQuery("brand", params.getBrand()));
        }

        // 星级查询
        if (params.getStarName() != null &&  !"".equals(params.getStarName())) {
            // 将查询条件放在filter中防止算分
            boolQuery.filter(QueryBuilders.termQuery("starName", params.getStarName()));
        }

        // 价格
        if (params.getMinPrice() != null && params.getMaxPrice() != null) {
            boolQuery.filter(QueryBuilders.rangeQuery("price")
                    .gte(params.getMinPrice()).lte(params.getMaxPrice()));
        }

        // 算分查询
        FunctionScoreQueryBuilder fsqb = QueryBuilders.functionScoreQuery(
                boolQuery, // 原始查询，相关性算分的查询
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                                // 一个functionScore元素
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                        QueryBuilders.termQuery("isAd", true),
                                        // 算分函数
                                        ScoreFunctionBuilders.weightFactorFunction(10)
                                )
                        });


        return fsqb;
    }

    /**
     * 抽取处理查询结果的步骤，ctrl + alt + m
     * @param resp
     */
    private PageResult handleResponse(SearchResponse resp) {
        // 解析结果
        SearchHits hits = resp.getHits();

        // 查询所有的条数
        long total = hits.getTotalHits().value;

        // 查询所有的结果
        SearchHit[] lists = hits.getHits();
        List<HotelDoc> hotels = new ArrayList<>();
        for (SearchHit hit : lists) {
            // 得到source
            String json = hit.getSourceAsString();

            // 将JSON数据反序列化
            HotelDoc doc = JSON.parseObject(json, HotelDoc.class);

            // 获取排序值
            Object[] values = hit.getSortValues();
            if (values.length > 0) {
                Object value = values[0];
                doc.setDistance(value);
            }
            hotels.add(doc);
        }
        return new PageResult(total, hotels);
    }
}
