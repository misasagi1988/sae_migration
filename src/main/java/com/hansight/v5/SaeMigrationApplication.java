package com.hansight.v5;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SaeMigrationApplication {
    private final static Logger LOG = LoggerFactory.getLogger(SaeMigrationApplication.class);
	public static void main(String[] args) {
        LOG.info("start migration service...");
		SpringApplication.run(SaeMigrationApplication.class, args);
	}
}
