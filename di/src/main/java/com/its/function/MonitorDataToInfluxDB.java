package com.its.function;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import com.its.common.Kafka;
import com.its.common.Tools;
import net.sf.json.JSONObject;
/**
 * @author lijiahui
*/
public class MonitorDataToInfluxDB {
	static Properties dbproperties=null;
	static List<String> tagsList =Arrays.asList("instance","job");
	static Tools tools=new Tools();
	static String utcTime="";
	static InfluxDB influxDB= null;
	static List<Integer> exclusive_monitor_kpi_hashcode=new ArrayList<Integer>() ;
	static{
        dbproperties = new Properties();
        try {
			dbproperties.load(MonitorDataToInfluxDB.class.getClassLoader().getResourceAsStream("common.properties"));
		} catch (IOException e) {
			e.printStackTrace();
			
		}
        influxDB=Tools.connectInfluxDB(dbproperties.getProperty("influxDB_url"), dbproperties.getProperty("influxDB_user"), dbproperties.getProperty("influxDB_password"));
        String [] exclusiveMonitorKpi=dbproperties.getProperty("exclusive_monitor_kpi").split(",");
        for (int i=0;i<exclusiveMonitorKpi.length;i++){
        	exclusive_monitor_kpi_hashcode.add(exclusiveMonitorKpi[i].hashCode());
        }
	}
	public static void getMonitorValueFromKafka(String groupName,String topicName){
		KafkaConsumer<String,String> consumer=Kafka.createConsumer(UUID.randomUUID().toString());
		consumer.subscribe(Arrays.asList(topicName));
		int consumerRecordNum=0;
		int breakNum=0;		
		Point.Builder point=null;
		long initialTime=System.currentTimeMillis();
		Map<String, String> tagsMap = new HashMap<String, String>();
		Map<String, Object> fieldsMap = new HashMap<String, Object>();
		while (true) {
			BatchPoints batchPoints = BatchPoints.database(dbproperties.getProperty("influxDB_database")).consistency(InfluxDB.ConsistencyLevel.ALL).build();
			ConsumerRecords<String, String> records = consumer.poll(1000);
			for (ConsumerRecord<String, String> record : records){
				point=generateInfluxDBPoint(tagsMap,fieldsMap,record);
				if(point ==null){
					continue;
				}
				consumerRecordNum++;
				batchPoints.point(point.build());
				if(consumerRecordNum>=10000){
					//System.out.println(System.currentTimeMillis());	
					Tools.insertToInfluxDB(influxDB,dbproperties.getProperty("influxDB_database"),batchPoints);			
					consumerRecordNum=0;
				}
				fieldsMap.clear();
				tagsMap.clear();
				breakNum++;
			}
			if(breakNum%100000<=500){
				System.out.println("消费"+breakNum+"条数据，共耗时"+(System.currentTimeMillis()-initialTime)/1000+"s");
			}
	    }         
	}
	public static boolean filter(String monitorKPI){
		for(int i=0;i<exclusive_monitor_kpi_hashcode.size();i++){
			if(monitorKPI.hashCode()==exclusive_monitor_kpi_hashcode.get(i)){
				return true;
			}	
		}
		return false;
	}	
	public static Point.Builder generateInfluxDBPoint(Map<String, String> tagsMap,Map<String, Object> fieldsMap,ConsumerRecord<String, String> record){
		JSONObject jsonobject=JSONObject.fromObject(record.value());
		JSONObject tagsObject=(JSONObject)jsonobject.get("labels");
		String monitorKPIName=jsonobject.get("name").toString();
		boolean ifExclusiveLoad=filter(monitorKPIName);
		if(ifExclusiveLoad==false){	
			@SuppressWarnings("unchecked")
			Iterator<String> tagsIterator = tagsObject.keys();
			String fieldsKey="";
			String tagsvalue="";
			String tagsKey=""  ;
			/*generate tagsKay and filedsKey*/
			while(tagsIterator.hasNext()){
				tagsKey=tagsIterator.next().toString();
				tagsvalue=tagsObject.getString(tagsKey);
				if(tagsList.contains(tagsKey)){
					tagsMap.put(tagsKey, tagsvalue);
				}
				else{
					if(!"__name__".equals(tagsKey)){
						fieldsKey="_"+fieldsKey+tagsvalue;
					}
				}
			}
			/*generate fields value*/
			Double fieldsValue=Double.valueOf(jsonobject.get("value").toString().replace("?","0").replace("NaN","0"));
			fieldsMap.put(jsonobject.get("name").toString()+fieldsKey, fieldsValue);
			
			/*generate timestamp*/
			setUTCTime(jsonobject.get("timestamp").toString());
			
			/*insert into influxdb point*/
			Point.Builder point=Tools.insertToInfluxDBPoint(dbproperties.getProperty("influxDB_mesurement"),Tools.getMilliSecondFromUTCTime(utcTime),tagsMap,fieldsMap);
			return point;
		}
		return null;
	}	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		getMonitorValueFromKafka("monitorDataGroup","OS");
	}
	public static String getutcTime() {
		return utcTime;
	}
	public static void setUTCTime(String utcTime) {
		MonitorDataToInfluxDB.utcTime = utcTime;
	}	
}
