package org.distributed.shardingjh.controller.findview;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class FindViewController {

    @GetMapping("/home")
    public String home(){
        return "Spring boot shardingJH is running!";
    }

}
