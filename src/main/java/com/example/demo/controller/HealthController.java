package com.example.demo.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class HealthController {

    @GetMapping("/health/{name}/{age}")
    public String sayHello(@PathVariable String name, @PathVariable int age) {
        System.out.println("App health is green ");
        return "Hello, " + name + "! You are " + age + " years old.";
    }
}
