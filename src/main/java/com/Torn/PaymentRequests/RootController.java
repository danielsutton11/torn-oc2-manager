package com.Torn.PaymentRequests;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RootController {

    @GetMapping("/")
    public ResponseEntity<String> root() {
        return ResponseEntity.ok("OC2 Payment Service is running!");
    }

    @GetMapping("/health")
    public ResponseEntity<String> simpleHealth() {
        return ResponseEntity.ok("OK");
    }
}