package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.io.IOException;

import static cn.itcast.hotel.constants.HotelConstants.MAPPING_TEMPLATE;

@SpringBootTest
public class HotelDocumentTest {
    @Resource
    private IHotelService hotelService;

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

    /**
     * 添加文档
     * @throws IOException
     */
    @Test
    public void testAddDoc() throws IOException {
        // 查询酒店数据
        Hotel hotel = hotelService.getById(56977L);
        // 将数据转换为文档类型
        HotelDoc hotelDoc = new HotelDoc(hotel);

        // 1.准备request对象
        IndexRequest req = new IndexRequest("hotel").id(hotelDoc.getId().toString());

        // 2.准备json文档
        req.source(JSON.toJSONString(hotelDoc), XContentType.JSON);

        // 3.发送请求
        client.index(req, RequestOptions.DEFAULT);
    }

    /**
     * 查询文档
     */
    @Test
    public void testFindById() throws IOException {
        // 1 准备request
        GetRequest req = new GetRequest("hotel", "56977");

        // 2 发送请求，得到响应
        GetResponse resp = client.get(req, RequestOptions.DEFAULT);

        // 3 解析响应结果
        String json = resp.getSourceAsString();

        // 实现反序列化
        HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
        System.out.println(hotelDoc);

    }

    @Test
    public void testDeleteDoc() throws IOException {
        // 1 准备request
        DeleteRequest req = new DeleteRequest("hotel", "56977");

        // 2 发送请求，得到响应
        client.delete(req, RequestOptions.DEFAULT);

    }
}
