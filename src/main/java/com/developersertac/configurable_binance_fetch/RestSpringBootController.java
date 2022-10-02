package com.developersertac.configurable_binance_fetch;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import lombok.RequiredArgsConstructor;

@RestController
@RequiredArgsConstructor
public class RestSpringBootController {
    
	@Autowired
	MongoOperations mongoOps;
	
	DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");

	@GetMapping("/kline-statistics")
	public String klineStatistics(@RequestParam String symbol, @RequestParam String interval) {
		
		String description = "";
		String collectionName = symbol + "-" + interval;

		String latestOpenPrice = getDocumentWithMaxValueOf(collectionName, "openTime", Sort.Direction.DESC).getOpen().toEngineeringString();
		String lastOpenTime = dateFormat.format(new Date(getDocumentWithMaxValueOf(collectionName, "openTime", Sort.Direction.DESC).getOpenTime()));
		String firstOpenTime = dateFormat.format(new Date(getDocumentWithMaxValueOf(collectionName, "openTime", Sort.Direction.ASC).getOpenTime()));
		String minCloseValue = getDocumentWithMaxValueOf(collectionName, "close", Sort.Direction.ASC).getClose().toEngineeringString();
		String maxCloseValue = getDocumentWithMaxValueOf(collectionName, "close", Sort.Direction.DESC).getClose().toEngineeringString();
		String klineCount = String.valueOf(mongoOps.getCollection(collectionName).countDocuments());

		description += "<h2>" + symbol + " Parity Statistics for " + interval + " interval</h2>";
		description += "<ul>";
		description += "<li><b>Latest open price:</b> " + latestOpenPrice + "</li>";
		description += "<li><b>Last open time:</b> " + lastOpenTime + "</li>";
		description += "<li><b>First open time:</b> " + firstOpenTime + "</li>";
		description += "<li><b>Minimum close value:</b> " + minCloseValue + "</li>";
		description += "<li><b>Maximum close value:</b> " + maxCloseValue + "</li>";
		description += "<li><b>Kline count:</b> " + klineCount + "</li>";
		description += "</ul>";
		
		return description;
	}
	
	public BinanceKline getDocumentWithMaxValueOf(String collectionName, String key, Direction sort) {
		
		if (mongoOps.getCollection(collectionName).countDocuments() > 0) {
		
			return mongoOps.findOne(
					new Query().with(Sort.by(sort, key)).limit(1), 
					BinanceKline.class, 
					collectionName);
			
		}
		
		return null;
		
	}
}
