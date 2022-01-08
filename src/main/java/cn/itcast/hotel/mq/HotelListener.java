package cn.itcast.hotel.mq;

import cn.itcast.hotel.constants.MqConstants;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

@Component
public class HotelListener {

    @Resource
    private IHotelService service;

    /**
     * 监听酒店新增或者修改的业务
     * @param hotel
     */
    @RabbitListener(queues = MqConstants.HOTEL_INSERT_QUEUE)
    public void ListenHotelInsertOrUpdate(String hotel) {
        Hotel hotel1 = JSON.parseObject(hotel, Hotel.class);
        service.insert(hotel1);
    }

    /**
     * 监听酒店删除的业务
     * @param id
     */
    @RabbitListener(queues = MqConstants.HOTEL_DELETE_QUEUE)
    public void ListenHotelDelete(Long id) {
        service.deleteById(id);
    }

}
