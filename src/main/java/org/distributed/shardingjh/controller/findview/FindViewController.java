package org.distributed.shardingjh.controller.findview;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class FindViewController {

    @GetMapping("/test")
    public String test(){
        log.info("Spring boot shardingJH is running!");
        return "Spring boot shardingJH is running!";
    }

}
