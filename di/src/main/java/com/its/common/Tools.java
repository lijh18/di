package com.its.common;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;


public class Tools {
	static SimpleDateFormat UTCFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	public static InfluxDB ConnectInfluxDB(String url,String user,String password){
		InfluxDB influxDB = InfluxDBFactory.connect(url,user,password);
		return influxDB;
	}
	public static Point.Builder insertToInfluxDBPoint(String mesurement,Long MilliSecond,Map<String, String> tagsMap,Map<String, Object> fieldsMap){		
		Point.Builder point = Point.measurement(mesurement);
		point.fields(fieldsMap);
		point.tag(tagsMap);
		point.time(MilliSecond, TimeUnit.NANOSECONDS);
		return point;
	}
	public static void insertToInfluxDB(InfluxDB influxDB,String database,BatchPoints batchPoints){		
		if(!influxDB.databaseExists(database)){
			influxDB.createDatabase(database);
		}
		influxDB.write(batchPoints);
	}
	public static long getMilliSecondFromUTCTime(String UTCTime){		
		try { 
			return UTCFormat.parse(UTCTime).getTime();
		} catch (java.text.ParseException e) {
			e.printStackTrace();
			return 0;
		}
	}
}
