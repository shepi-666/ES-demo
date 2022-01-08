package cn.itcast.hotel.controller;

import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/hotel")
public class HotelController {

    @Resource
    private IHotelService hotelService;

    @ResponseBody
    @PostMapping("/list")
    public PageResult search(@RequestBody RequestParams params) {
        return hotelService.search(params);
    }

    @ResponseBody
    @PostMapping("/filters")
    public Map<String, List<String>> getAgg(@RequestBody RequestParams params) {
        return hotelService.filters(params);
    }

    @ResponseBody
    @GetMapping("/suggestion")
    public List<String> getSuggestion(@RequestParam("key") String prefix) {
        return hotelService.getSuggestions(prefix);
    }

}
