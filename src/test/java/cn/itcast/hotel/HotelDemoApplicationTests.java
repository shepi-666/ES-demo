package cn.itcast.hotel;

import cn.itcast.hotel.service.IHotelService;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

@SpringBootTest
class HotelDemoApplicationTests {

    @Test
    void contextLoads() {
    }

    @Resource
    private IHotelService service;

    /*@Test
    public void testAgg() {
        Map<String, List<String>> filters = service.filters(params);
        System.out.println(filters);
    }*/

}
