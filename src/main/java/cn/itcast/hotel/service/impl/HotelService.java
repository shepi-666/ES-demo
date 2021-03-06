package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
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
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.xcontent.XContentType;
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
            // 1 ??????request
            SearchRequest req = new SearchRequest("hotel");

            QueryBuilder qb = getQueryBuilder(params);


            req.source().query(qb);
            // 2.2 ??????
            Integer page = params.getPage();
            Integer size = params.getSize();
            req.source().from((page - 1) * size).size(size);

            // 2.3 ??????????????????
            String location = params.getLocation();
            if (location != null && !"".equals(location)) {
                req.source().sort(SortBuilders
                        .geoDistanceSort("location", new GeoPoint(location))
                        .order(SortOrder.ASC)
                        .unit(DistanceUnit.KILOMETERS)
                );
            }

            // 3 ????????????
            SearchResponse resp = client.search(req, RequestOptions.DEFAULT);

            return handleResponse(resp);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, List<String>> filters(RequestParams params) {
        // ??????????????????????????????????????????
        List<String> keywords = new ArrayList<>();
        keywords.add("city");
        keywords.add("brand");
        keywords.add("starName");

        // ????????????req
        SearchRequest req = new SearchRequest("hotel");
        SearchSourceBuilder builder = req.source();
        // ??????????????????
        QueryBuilder queryBuilder = getQueryBuilder(params);
        builder.query(queryBuilder);

        Map<String, List<String>> res = new HashMap<>();
        for (String keyword : keywords) {
            getAggregation(keyword, builder);
        }

        // ????????????
        try {
            SearchResponse resp = client.search(req, RequestOptions.DEFAULT);
            // ????????????????????????
            getResult(resp, keywords, res);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return res;
    }

    @Override
    public List<String> getSuggestions(String prefix) {

        List<String> res = new ArrayList<>();
        SearchRequest req = new SearchRequest("hotel");
        req.source().suggest(new SuggestBuilder().addSuggestion(
                "suggestions",
                SuggestBuilders.completionSuggestion("suggestion")
                        .prefix(prefix)
                        .skipDuplicates(true)
                        .size(10)
        ));

        try {
            SearchResponse resp = client.search(req, RequestOptions.DEFAULT);


            Suggest suggest = resp.getSuggest();
            // ??????????????????????????????????????????
            CompletionSuggestion suggestions = suggest.getSuggestion("suggestions");
            // ??????options
            List<CompletionSuggestion.Entry.Option> options = suggestions.getOptions();
            // ??????
            for (CompletionSuggestion.Entry.Option option : options) {
                String text = option.getText().toString();
                res.add(text);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return res;

    }

    @Override
    public void insert(Hotel hotel) {
        try {
            HotelDoc hotelDoc = new HotelDoc(hotel);

            // req??????
            IndexRequest req = new IndexRequest("hotel").id(hotel.getId().toString());
            // ??????json??????
            req.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
            // ????????????
            client.index(req, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void deleteById(Long id) {
        try {
            DeleteRequest req = new DeleteRequest("hotel", id.toString());
            client.delete(req, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private SearchSourceBuilder getAggregation(String keyword, SearchSourceBuilder builder) {
        // 2 ??????DSL
        // 2.1 ??????size
        builder.size(0);
        // 2.2 ??????
        builder.aggregation(AggregationBuilders
                .terms(keyword + "Agg")
                .field(keyword)
                .size(100) // ???????????????
        );
        return builder;
    }

    /**
     * ??????????????????
     */
    public void getResult(SearchResponse resp, List<String> keywords, Map<String, List<String>> res) {
        // ??????????????????
        Aggregations aggregations = resp.getAggregations();

        for (String keyword : keywords) {
            // ????????????????????????????????????
            Terms brandTerms = aggregations.get(keyword + "Agg");

            // ????????????
            List<? extends Terms.Bucket> buckets = brandTerms.getBuckets();

            List<String> temp = new ArrayList<>();
            // ????????????
            for (Terms.Bucket bucket : buckets) {
                String terms = bucket.getKeyAsString();
                temp.add(terms);
            }

            res.put(keyword, temp);
        }


    }

    /**
     * ??????queryBuilder?????????
     * @param params
     * @return
     */
    private QueryBuilder getQueryBuilder(RequestParams params) {
        // 2 ??????dsl
        // 2.1 query
        // ??????booleanquery
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        // must??????????????????
        String key = params.getKey();
        // ??????????????????
        if (key == null || "".equals(key)) {
            boolQuery.must(QueryBuilders.matchAllQuery());
        } else {
            boolQuery.must(QueryBuilders.matchQuery("condition", key));
        }
        // ????????????
        // ????????????
        if (params.getCity() != null &&  !"".equals(params.getCity())) {
            // ?????????????????????filter???????????????
            boolQuery.filter(QueryBuilders.termQuery("city", params.getCity()));
        }

        // ????????????
        if (params.getBrand() != null &&  !"".equals(params.getBrand())) {
            // ?????????????????????filter???????????????
            boolQuery.filter(QueryBuilders.termQuery("brand", params.getBrand()));
        }

        // ????????????
        if (params.getStarName() != null &&  !"".equals(params.getStarName())) {
            // ?????????????????????filter???????????????
            boolQuery.filter(QueryBuilders.termQuery("starName", params.getStarName()));
        }

        // ??????
        if (params.getMinPrice() != null && params.getMaxPrice() != null) {
            boolQuery.filter(QueryBuilders.rangeQuery("price")
                    .gte(params.getMinPrice()).lte(params.getMaxPrice()));
        }

        // ????????????
        FunctionScoreQueryBuilder fsqb = QueryBuilders.functionScoreQuery(
                boolQuery, // ???????????????????????????????????????
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                                // ??????functionScore??????
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                        QueryBuilders.termQuery("isAd", true),
                                        // ????????????
                                        ScoreFunctionBuilders.weightFactorFunction(10)
                                )
                        });


        return fsqb;
    }

    /**
     * ????????????????????????????????????ctrl + alt + m
     * @param resp
     */
    private PageResult handleResponse(SearchResponse resp) {
        // ????????????
        SearchHits hits = resp.getHits();

        // ?????????????????????
        long total = hits.getTotalHits().value;

        // ?????????????????????
        SearchHit[] lists = hits.getHits();
        List<HotelDoc> hotels = new ArrayList<>();
        for (SearchHit hit : lists) {
            // ??????source
            String json = hit.getSourceAsString();

            // ???JSON??????????????????
            HotelDoc doc = JSON.parseObject(json, HotelDoc.class);

            // ???????????????
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
