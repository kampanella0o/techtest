package com.maksym.techtest.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class IndexController {

    @GetMapping("/")
    public String index(){
        return "Bucket listener is launched! Upload avro files to the bucket.";
    }

}
