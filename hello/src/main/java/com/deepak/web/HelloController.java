package com.deepak.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HelloController {

    final Logger logger = LoggerFactory.getLogger(HelloController.class);

    @GetMapping("/api/v1/hello/{name}")
    public ResponseEntity<String> hello(@PathVariable String name) {
        logger.info("Calling hello endpoint with param " + name);
        return ResponseEntity.ok("Hello, " + name);
    }
}
