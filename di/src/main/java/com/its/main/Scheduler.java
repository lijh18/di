package com.its.main;

import java.io.IOException;
import java.sql.SQLException;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import com.its.common.Tools;
import com.its.function.Alarm;
import com.its.function.DeleteMiddleTable;
import com.its.function.ETL_StorageDataFromUnisphere;
import com.its.function.HC_GenerateECCHCReport;
import com.its.function.ETL_DBPerformance;
import com.its.function.ETL_DBPerformanceToHDFS;
import com.its.function.ETL_K2Storage;
import com.its.function.ETL_OSDataFromSaltStack;
import com.its.function.SqlserverJobExecuteDelayedAlarm;

public class Scheduler implements Job{
    public void execute(JobExecutionContext context) throws JobExecutionException {
        if("ECC-Report-ETL.every10Min".equals(context.getJobDetail().getKey().toString())){
        	new HC_GenerateECCHCReport().loadDataFromCommand("sapcontrol -nr 1 -function EnqGetLockTable|wc -l","totalLockEntry","10.96.14.161");
        }
        if("ECC-Report-ETL.every1Hour".equals(context.getJobDetail().getKey().toString())){
        	new HC_GenerateECCHCReport().loadDataFromCommand("sapcontrol -nr 1 -function GetSystemInstanceList|grep -i abap|wc -l","instancesNums","10.96.14.161");
        } 
        if("ECC-Report-ETL.generateReportAndSendEmail".equals(context.getJobDetail().getKey().toString())){
    		try {
				try {
					new HC_GenerateECCHCReport().generateECCReport();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		new HC_GenerateECCHCReport().sendECCReportToEmail();
        }         
        if("K2Storage.min".equals(context.getJobDetail().getKey().toString())){
        	ETL_K2Storage.loadK2StorageData("performance");
        	try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	ETL_K2Storage.loadK2StorageData("capacity");
        }
        if("OS.min".equals(context.getJobDetail().getKey().toString())){
        	ETL_OSDataFromSaltStack.getOSData();
        }
        if(context.getJobDetail().getKey().toString().startsWith("DatabasePerformance")){
        	System.out.println("JOB��ʼ�����ˣ�ʱ��Ϊ"+new Tools().getDate());
        	ETL_DBPerformance.startRun();
        }        
        if(context.getJobDetail().getKey().toString().startsWith("Alarm")){
        	try {
				new Alarm().getAlarm();
			} catch (Exception e) {
				e.printStackTrace();
			} 
        }
        if("sqlserverPublishAlarm.min".equals(context.getJobDetail().getKey().toString())){
        	try {
				new Alarm().getAlarm();
			} catch (Exception e) {
				e.printStackTrace();
			}
        }
        if("sqlserverReportExcuteEelayedAlarm.day".equals(context.getJobDetail().getKey().toString())){
        	new SqlserverJobExecuteDelayedAlarm().getAlarm();
        }
        if("DBPerformanceDataToHDFS.erery1Hour".equals(context.getJobDetail().getKey().toString())){
        	new ETL_DBPerformanceToHDFS().startRun();
        } 
        if("CleanITOAMiddleData.erery1Hour".equals(context.getJobDetail().getKey().toString())){
        	DeleteMiddleTable.startRun();
        }
        if("VMAXStorageData.erery10Min".equals(context.getJobDetail().getKey().toString())){
        	try {
				ETL_StorageDataFromUnisphere.startRun();
			} catch (IOException | SQLException | InterruptedException e) {
				e.printStackTrace();
			}
        }        
        if(context.getJobDetail().getKey().toString().startsWith("DBPerformanceToMysql")){
        	ETL_DBPerformance.startRun();
        }        
    }
    public static void main(String [] args) throws SQLException, IOException{
    	new HC_GenerateECCHCReport().generateECCReport();
    }
} 