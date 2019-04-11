package com.its.common;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

/**
 * @author lijiahui
*/
public class Tools {
	static SimpleDateFormat UTCFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	public static InfluxDB connectInfluxDB(String url,String user,String password){
		final InfluxDB influxDB = InfluxDBFactory.connect(url,user,password);
		influxDB.enableBatch(10000, 5000, TimeUnit.MILLISECONDS);
		return influxDB;
	}
	public static Point.Builder insertToInfluxDBPoint(String mesurement,Long milliSecond,Map<String, String> tagsMap,Map<String, Object> fieldsMap){		
		final Point.Builder point = Point.measurement(mesurement);
		point.fields(fieldsMap);
		point.tag(tagsMap);
		point.time(milliSecond, TimeUnit.NANOSECONDS);
		return point;
	}
	public static void insertToInfluxDB(InfluxDB influxDB,String database,BatchPoints batchPoints){		
		if(!influxDB.databaseExists(database)){
			influxDB.createDatabase(database);
		}
		influxDB.write(batchPoints);
		influxDB.flush();
	}
	public static long getMilliSecondFromUTCTime(String utcTime){		
		try { 
			return UTCFormat.parse(utcTime).getTime();
		} catch (java.text.ParseException e) {
			e.printStackTrace();
			return 0;
		}
	}
}
