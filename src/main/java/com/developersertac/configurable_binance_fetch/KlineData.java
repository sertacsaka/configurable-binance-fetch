package com.developersertac.configurable_binance_fetch;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.Period;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.result.DeleteResult;

@Component
public class KlineData {
	
	@Autowired
	MongoClient mongoClient;
	
	@Autowired
	MongoOperations mongoOps;

	@Autowired
	ApplicationConfiguration appConf;
	
	DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");

	String baseEndpoint;
	String klinesEndpoint;
	String timeEndpoint;
	String exchangeinfoEndpoint;
	int binanceLimit;
    
    String cronExpression;
    boolean firstFetchAllInsertLater;
    boolean loggingOn;
    boolean deleteBeforeInsert;
	
	String parityBase;
	
	Long exchangeTimeDifference;
	
    public KlineData() {
    	
    	this.dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    	
	}
    
    public void logln(String log) {
    	
    	if (this.loggingOn) System.out.println(log);
    	
    }
    
    public void log(String log) {
    	
    	if (this.loggingOn) System.out.print(log);
    	
    }

	public String getKlineArrayJsonString(String symbol, Interval interval, Long startTime, Long endTime) {
		
		String uri = this.baseEndpoint + this.klinesEndpoint + "?symbol=" + symbol + "&interval=" + interval.getCode() + "&limit=" + binanceLimit;
		
		if (startTime != 0) 
		{
			uri += "&startTime=" + Long.toString(startTime);
			
			if (endTime != 0) 
			{
				uri += "&endTime=" + Long.toString(endTime);
			}
		}

		return (new RestTemplate()).getForObject(uri, String.class);
	}
	
	public List<BinanceKline> convertArrayJsonStringToKlineList(String json) {
		
		if (!json.isBlank() && !json.isEmpty()) {

			try {
				
				List<BinanceKline> binanceKlineList = new ArrayList<BinanceKline>();
				
				for(String[] s : (new ObjectMapper()).readValue(json, new TypeReference<List<String[]>>(){})) 
					binanceKlineList.add(new BinanceKline(s));
				
				return binanceKlineList;
				
			} catch (JsonMappingException e) {
				e.printStackTrace();
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
			
		}
		
		return null;
	}
	
	public List<BinanceKline> getKlineList(String symbol, Interval interval, Long startTime, Long endTime) {
		
		return convertArrayJsonStringToKlineList(getKlineArrayJsonString(symbol, interval, startTime, endTime));
	}

	public boolean klineExists(String symbol, Interval interval, Long openTime) {
		
		List<BinanceKline> kline = getKlineList(symbol, interval, openTime, openTime + interval.getUtsMs());

		if ( kline.isEmpty() || ( !kline.isEmpty() && kline.size() == 0 ) ) return false;
		
		return true;
	}
	
	public Long getServerTime(boolean fromServer) {
		
		if (fromServer)
			return (new JSONObject((new RestTemplate()).getForObject(this.baseEndpoint + this.timeEndpoint, String.class))).getLong("serverTime");

		return System.currentTimeMillis() + this.exchangeTimeDifference;
		
	};
	
	public Long openTimeOfUncompletedKline(Interval interval) {
		return Constants.truncByInterval(getServerTime(false), interval); 
	};
	
	public String concatenateTwoKlineArrayJsonStrings(String klineArrayJsonString1, String klineArrayJsonString2) {

		if (klineArrayJsonString1.isEmpty() && !klineArrayJsonString2.isEmpty()) return klineArrayJsonString2;
		if (!klineArrayJsonString1.isEmpty() && klineArrayJsonString2.isEmpty()) return klineArrayJsonString1;
		if (klineArrayJsonString1.isEmpty() && klineArrayJsonString2.isEmpty()) return "";
		
		return klineArrayJsonString1.substring(0, klineArrayJsonString1.length() - 1) + "," + klineArrayJsonString2.substring(1);
		
	}

	public List<BinanceKline> fetchKlineList(String symbol, Interval interval, Long startTime) {

		logln("\t\tFetch kline array list from startTime: " + startTime + " = " + dateFormat.format(startTime));
		
		String klines = "";
		Long stepBlockSize = this.binanceLimit * interval.getUtsMs();
		Long lastCompletedKlineOpenTime = 0L;
		Long newUncompletedKlineOpenTime = 0L;
		Long uncompletedKlineOpenTime = 0L;
		Long openTime = startTime;
		Long endTime = 0L;
		int apiCallCount = 0;

		do {
			
			newUncompletedKlineOpenTime = openTimeOfUncompletedKline(interval);
			
			if (uncompletedKlineOpenTime < newUncompletedKlineOpenTime) {
				
				uncompletedKlineOpenTime = newUncompletedKlineOpenTime;
				lastCompletedKlineOpenTime = uncompletedKlineOpenTime - interval.getUtsMs();

				while (openTime < lastCompletedKlineOpenTime) {

					endTime = openTime + stepBlockSize;

					if (endTime >= lastCompletedKlineOpenTime) 
						endTime = lastCompletedKlineOpenTime;

					klines = concatenateTwoKlineArrayJsonStrings(klines, getKlineArrayJsonString(symbol, interval, openTime, endTime));
				
					openTime = endTime;
					
					apiCallCount++;
					
				}
				
			}
			else
				break;
			
		} while (true);
		
		logln("\t\t\tDone! API called " + apiCallCount + " times.");
		
		return convertArrayJsonStringToKlineList(klines);
	}

	public void fetchAndInsertKlines(String symbol, Interval interval, Long startTime, String collectionName) {

		logln("\t\tFetch and insert klines from startTime: " + startTime + " = " + dateFormat.format(startTime));
		
		List<BinanceKline> klineList = null;
		Long stepBlockSize = this.binanceLimit * interval.getUtsMs();
		Long lastCompletedKlineOpenTime = 0L;
		Long uncompletedKlineOpenTime = 0L;
		Long openTime = startTime;
		Long endTime = 0L;
		int apiCallCount = 0;
		
		if (startTime >= openTimeOfUncompletedKline(interval)) {
			
			logln("\t\tNo new completed klines to insert");
			
		}
		else {
			
			if (this.deleteBeforeInsert) {

				DeleteResult deleteResult = mongoOps.remove(new Query(Criteria.where("openTime").gte(startTime)), collectionName);
	
				logln("\t" + deleteResult.getDeletedCount() + " documents deleted.");
			
			}

			do {

				uncompletedKlineOpenTime = openTimeOfUncompletedKline(interval);
				lastCompletedKlineOpenTime = uncompletedKlineOpenTime - interval.getUtsMs();
				
				do {

					endTime = openTime + stepBlockSize;

					if (endTime > lastCompletedKlineOpenTime) 
						endTime = lastCompletedKlineOpenTime;

					log("\t\t\tFrom " + openTime + " (" + dateFormat.format(openTime) + ") ");
					logln("to " + endTime + " (" + dateFormat.format(endTime) + ") ");

					klineList = convertArrayJsonStringToKlineList(getKlineArrayJsonString(symbol, interval, openTime, endTime));
					
					Collection<BinanceKline> insertedObjects = mongoOps.insert(klineList, collectionName);
					
					if (insertedObjects.isEmpty())
						
						logln("\tEmpty array of inserted objects for " + klineList.size() + " documents");
					
					else
						
						logln("\t" + insertedObjects.size() + " objects inserted");
				
					openTime = endTime;
					
					if (this.loggingOn) apiCallCount++;
					
				} while (openTime < lastCompletedKlineOpenTime);
				
			} while (uncompletedKlineOpenTime < openTimeOfUncompletedKline(interval));
			
		}

		logln("\t\tDocuments count: " + mongoOps.getCollection(collectionName).countDocuments());
		
		if (this.loggingOn) {
			
			Long maxOpenTime = getDocumentWithMaxValueOf(collectionName, "openTime").getOpenTime();
		
			logln("\t\t\tLatest openTime in DB: " + maxOpenTime + " = " + dateFormat.format(maxOpenTime));
			
		}
		
		logln("\t\tDone! API called " + apiCallCount + " times.");
	}
	
	public BinanceKline getDocumentWithMaxValueOf(String collectionName, String key) {
		
		if (mongoOps.getCollection(collectionName).countDocuments() > 0) {
		
			return mongoOps.findOne(
					new Query().with(Sort.by(Sort.Direction.DESC, key)).limit(1), 
					BinanceKline.class, 
					collectionName);
			
		}
		
		return null;
		
	}
	
	public void insertKlineList(List<BinanceKline> klineList, String collectionName, Long startTime) {

		if (klineList != null && !klineList.isEmpty() && klineList.size() > 0) {
			
			if (this.deleteBeforeInsert) {

				DeleteResult deleteResult = mongoOps.remove(new Query(Criteria.where("openTime").gte(startTime)), collectionName);
	
				logln("\t" + deleteResult.getDeletedCount() + " documents deleted.");
			
			}
			
			Collection<BinanceKline> insertedObjects = mongoOps.insert(klineList, collectionName);
			
			if (insertedObjects.isEmpty())
				
				logln("\tEmpty array of inserted objects for " + klineList.size() + " documents into collection " + collectionName);
			
			else
				
				logln("\t" + insertedObjects.size() + " objects inserted into collection " + collectionName);

			logln("\t\tDocuments count: " + mongoOps.getCollection(collectionName).countDocuments());
			
			if (this.loggingOn) {
				
				Long maxOpenTime = getDocumentWithMaxValueOf(collectionName, "openTime").getOpenTime();
			
				logln("\t\t\tLatest openTime in DB: " + maxOpenTime + " = " + dateFormat.format(maxOpenTime));
				
			}
		}
		
	}
	
	public long getEarliestOpenTime(String symbol, Interval interval, Long fromOpenTime, Long toOpenTime, boolean fromExists) {
		
	    Long inFrom = 0L;
	    Long inTo = 0L;
	    Long outFrom = 0L;
	    Long outTo = 0L;
	    Long middleUtsMs = 0L;
	    boolean inFromExists = false;
	    boolean outFromExists = false;
	    Interval intervalToUse;
	    
	    if (interval.getCode() == "3d") intervalToUse = Interval._1d; else intervalToUse = interval;

	    if ( fromOpenTime == 0L )
	    {
			inFrom = Constants.truncByInterval(Instant.now().getEpochSecond() * 1000, intervalToUse);
			inTo = inFrom;
			inFromExists = true;
	    }
	    else
	    {
	    	inFrom = fromOpenTime;
			inTo = toOpenTime;
			inFromExists = fromExists;
	    }
		
		if (inFromExists) {
			outFrom = Constants.truncByInterval(inFrom - Constants.uts3y, intervalToUse);
			
			if (klineExists(symbol, intervalToUse, outFrom))
			{
				outTo = outFrom;
				outFromExists = true;
			}
			else
			{
				outTo = inFrom;
				outFromExists = false;
			}
		}
		else
		{
			if (inFrom == inTo || inTo - inFrom == intervalToUse.getUtsMs())
				return inTo;
			else if (intervalToUse.getCode() == "1M" && Period
					.between((new Date(inFrom)).toInstant().atZone(ZoneId.of("UTC")).toLocalDate().withDayOfMonth(1),
							(new Date(inTo)).toInstant().atZone(ZoneId.of("UTC")).toLocalDate().withDayOfMonth(1))
					.getMonths() == 1)
				return inTo;
            
            middleUtsMs = Constants.truncByInterval((inFrom + inTo) / 2, intervalToUse);
            		
    		if (klineExists(symbol, intervalToUse, middleUtsMs))
    		{
    			outFrom = inFrom;
				outTo = middleUtsMs;
				outFromExists = false;
    		}
            else
            {
            	outFrom = middleUtsMs;
    			outTo = inTo;
				outFromExists = false;
            }
		}
		
		return getEarliestOpenTime(symbol, intervalToUse, outFrom, outTo, outFromExists);
	}
	
	public void importKlines(String symbol, Interval interval) {
		
		logln("-----------------------\nImport into: " + symbol + "-" + interval.getCode() + "\n-----------------------");
		
		String collectionName = symbol + "-" + interval.getCode();
		
		Long startTime = 0L;
		
		Long documentCount = mongoOps.getCollection(collectionName).countDocuments();
		
		logln("\tExisting document count: " + documentCount);
		
		if (documentCount > 0) {

			startTime = getDocumentWithMaxValueOf(collectionName, "openTime").getOpenTime();
			
			logln("\tLatest openTime in DB: " + startTime + " = " + dateFormat.format(startTime));
			
			startTime += interval.getUtsMs();
			
		}
		else {
			
			logln("\tNo documents exist");
			
			startTime = getEarliestOpenTime(symbol, interval, 0L, 0L, false);
			
		}
		
		if (firstFetchAllInsertLater) 
			insertKlineList(fetchKlineList(symbol, interval, startTime), collectionName, startTime);
		else 
			fetchAndInsertKlines(symbol, interval, startTime, collectionName);
		
		mongoOps.indexOps(collectionName).ensureIndex(new Index().on("openTime", Direction.ASC));
		
		logln("");
		
	}
	
	public Interval getIntervalFromCode(String code) {
		Interval interval = Interval._1M;
		
		for (Interval i : Interval.values()) { 
		    if (i.getCode().equals(code)) return i; 
		}
		
		return interval;
	}
	
	public List<String> getAllParities() {
		
		List<String> allParities = new ArrayList<String>();
		
		JSONObject jsonObject = new JSONObject((new RestTemplate()).getForObject(this.baseEndpoint + this.exchangeinfoEndpoint, String.class));

		JSONArray symbols = new JSONArray(jsonObject.get("symbols").toString());
		
		for(int i = 0; i < symbols.length(); i++) {

			if (symbols.getJSONObject(i).get("quoteAsset").toString().equals(parityBase) &&
					symbols.getJSONObject(i).get("status").toString().equals("TRADING")) {
				
				if ((new JSONArray(symbols.getJSONObject(i).get("permissions").toString())).toList().contains("SPOT")) {
					
					allParities.add(symbols.getJSONObject(i).get("baseAsset").toString());
					
				}
				
			}
			
		}

		return allParities; 
		
	}
	
	public List<String> getAllIntervals() {
		
		List<String> allIntervals = new ArrayList<String>();
	
		for (Interval i : Interval.values()) { 
			allIntervals.add(i.getCode());
		}
		
		return allIntervals;
		
	}
	
	public void deleteDocumentWithMaxOpenTimeFromAllCollections() {
		
		Long maxOpenTime;
		
		for(Object c : mongoOps.getCollectionNames().toArray()) {
			
			maxOpenTime = getDocumentWithMaxValueOf(c.toString(), "openTime").getOpenTime();
			
			DeleteResult deleteResult = mongoOps.remove(new Query(Criteria.where("openTime").is(maxOpenTime)), c.toString());
			
			logln(c.toString() + ": " + deleteResult.getDeletedCount() + " documents deleted.");
			
		}
		
	}
	
	public void cloneDataBase(String sourceDBName, String targetDBName) {
		
		for (String collection : mongoClient.getDatabase(sourceDBName).listCollectionNames()) {

			 mongoClient.getDatabase(sourceDBName).getCollection(collection).renameCollection(new MongoNamespace(targetDBName, collection));
			
		}
		
	}
	
	@Scheduled(cron = "${cron.expression}")
	public void updateDatabase() {
		
		loggingOn = appConf.flow().isLoggingOn();
    	firstFetchAllInsertLater = appConf.flow().isFirstFetchAllInsertLater();
		deleteBeforeInsert = appConf.flow().isDeleteBeforeInsert();

		baseEndpoint = appConf.binance().getBaseEndpoint();
		klinesEndpoint = appConf.binance().getKlinesEndpoint();
		timeEndpoint = appConf.binance().getTimeEndpoint();
		exchangeinfoEndpoint = appConf.binance().getExchangeinfoEndpoint();
		binanceLimit = appConf.binance().getLimit();
		
		String paritySelector = appConf.parity().getSelector();

		parityBase = appConf.parity().getBase();
		List<String> paritySymbols = appConf.parity().getSymbols();
		List<String> parityIntervals = appConf.parity().getIntervals();
		List<String> parityBlacklist = appConf.parity().getBlacklist();
		
		List<List<String>> parityInfo = appConf.parity().getInfo();
		
		List<String> paritySymbolList = null;
		List<String> parityIntervalList = null;
		
		exchangeTimeDifference = getServerTime(true) - System.currentTimeMillis();

		System.out.println("System Time: " + System.currentTimeMillis());
		System.out.println("Server Time: " + getServerTime(false));
		
		long duration = System.currentTimeMillis();
			
		if (paritySelector.equals("1")) {
			
			paritySymbolList = (paritySymbols.get(0).equals("all")) ? getAllParities() : paritySymbols;
			parityIntervalList = (parityIntervals.get(0).equals("all")) ? getAllIntervals() : parityIntervals;
			
			for(String symbol : paritySymbolList) {
				
				if (!parityBlacklist.contains(symbol)) {
				
					for(String code : parityIntervalList) {
						
						importKlines(symbol + parityBase, getIntervalFromCode(code));
						
					}
					
				}
			}
			
		}
		
		if (paritySelector.equals("2")) {
			
			for(List<String> info : parityInfo) {
				
				for(int i = 1; i < info.size(); i++) {
					
					importKlines(info.get(0), getIntervalFromCode(info.get(i)));
					
				}
			}
			
		}
		
		duration = System.currentTimeMillis() - duration;
		
		System.out.println("");
		System.out.println("Import completed in " + (duration/(1000*60)) + " minutes " + ((duration/1000)%60) + " seconds " + (duration%1000) + " milliseconds");
		System.out.println("");
		
	}
}
