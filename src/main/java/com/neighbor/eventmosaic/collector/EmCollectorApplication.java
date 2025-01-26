package com.neighbor.eventmosaic.collector;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@SpringBootApplication
@EnableDiscoveryClient
public class EmCollectorApplication {

	public static void main(String[] args) {
		SpringApplication.run(EmCollectorApplication.class, args);
	}

}
