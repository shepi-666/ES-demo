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
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {
    @Resource
    private RestHighLevelClient client;

    @Override
    public PageResult search(RequestParams params) {
        try {
            // 1 准备request
            SearchRequest req = new SearchRequest("hotel");

            // 2 准备dsl
            // 2.1 关键字搜索
            String key = params.getKey();
            // 健壮性的判断
            if (key == null || "".equals(key)) {
                req.source().query(QueryBuilders.matchAllQuery());
            } else {
                req.source().query(QueryBuilders.matchQuery("condition", key));
            }

            // 2.2 分页
            Integer page = params.getPage();
            Integer size = params.getSize();
            req.source().from((page - 1) * size).size(size);

            // 3 发送请求
            SearchResponse resp = client.search(req, RequestOptions.DEFAULT);

            return handleResponse(resp);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
            hotels.add(doc);
        }
        return new PageResult(total, hotels);
    }
}
