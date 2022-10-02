package com.developersertac.configurable_binance_fetch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class ConfigurableBinanceFetchApplication {

	public static void main(String[] args) {

		SpringApplication.run(ConfigurableBinanceFetchApplication.class, args);
	}

}
