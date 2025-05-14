package org.distributed.shardingjh.controller.usercontroller;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.model.User;
import org.distributed.shardingjh.service.UserService;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
public class UserController {

    @Resource
    UserService userService;

    @RequestMapping(value = "/user/save", method = RequestMethod.POST)
    public String save(@RequestBody User user) {
        userService.saveUser(user);
        return "Saved";
    }

    @RequestMapping(value = "/user/get/{id}", method = RequestMethod.GET)
    public User get(@PathVariable Long id) {
        return userService.findById(id);
    }
}
