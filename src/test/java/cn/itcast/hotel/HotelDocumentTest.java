package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
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
import java.util.List;

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

    /**
     * 删除文档
     * @throws IOException
     */
    @Test
    public void testDeleteDoc() throws IOException {
        // 1 准备request
        DeleteRequest req = new DeleteRequest("hotel", "56977");

        // 2 发送请求，得到响应
        client.delete(req, RequestOptions.DEFAULT);

    }

    /**
     * 更新文档
     */
    @Test
    public void testUpdateDoc() throws IOException {
        // 1 准备Request
        UpdateRequest req = new UpdateRequest("hotel", "60487");

        // 2 准备请求参数
        req.doc(
                "isAd", true
        );

        // 3 发送请求
        client.update(req,RequestOptions.DEFAULT);
    }

    /**
     * 批量操作文档
     * @throws IOException
     */
    @Test
    public void testBulkRequest() throws IOException {
        // 批量查询酒店数据
        List<Hotel> hotels = hotelService.list();

        // 1 创建request
        BulkRequest req = new BulkRequest();

        // 2 准备参数，添加多个新增的Request
        for (Hotel hotel : hotels) {
            HotelDoc hotelDoc = new HotelDoc(hotel);
            // 创建新增文档的req对象
            req.add(new IndexRequest("hotel").
                    id(hotelDoc.getId().toString()).
                    source(JSON.toJSONString(hotelDoc), XContentType.JSON));
        }

        // 3 发送请求
        client.bulk(req, RequestOptions.DEFAULT);
    }
}
