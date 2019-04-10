package com.its.main;

import java.util.List;

import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.ScheduleBuilder;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import com.its.common.Tools;

public class Quartz{
	static List<String> jobList;
	static List<String> jobgroupList;
	static List<Integer> frequencyList;
	static List<String> commandList;
	static String cycle;
	static String etlSource;
	//����������
    public Scheduler getScheduler() throws SchedulerException{
        SchedulerFactory schedulerFactory = new StdSchedulerFactory();
        return schedulerFactory.getScheduler();
    }
    public void schedulerECCJob() throws SchedulerException{
        JobDetail j_min = JobBuilder.newJob(com.its.main.Scheduler.class).withIdentity("every10Min", "ECC-Report-ETL").build();  
        Trigger t_min = TriggerBuilder.newTrigger().withIdentity("every10Min", "ECC-Report")
                            .withSchedule(CronScheduleBuilder.cronSchedule("00 0/10 * * * ?")) 
                            .build();
        Scheduler s_min = getScheduler();
        s_min.scheduleJob(j_min, t_min);
        s_min.start();

        JobDetail j_hour = JobBuilder.newJob(com.its.main.Scheduler.class).withIdentity("every1Hour", "ECC-Report-ETL").build();
        Trigger t_hour = TriggerBuilder.newTrigger().withIdentity("every1Hour", "ECC-Report")
                            .withSchedule(CronScheduleBuilder.cronSchedule("00 0/10 * * * ?"))
                            .build();
        Scheduler s_hours = getScheduler();
        s_hours.scheduleJob(j_hour, t_hour);
        s_hours.start();   
        
        JobDetail j_email = JobBuilder.newJob(com.its.main.Scheduler.class).withIdentity("generateReportAndSendEmail", "ECC-Report-ETL").build();
        Trigger t_email = TriggerBuilder.newTrigger().withIdentity("generateReportAndSendEmail", "ECC-Report")
                            .withSchedule(CronScheduleBuilder.cronSchedule("00 05 09 * * ?")) 
                            .build();
        Scheduler s_email = getScheduler();
        s_email.scheduleJob(j_email, t_email);
        s_email.start();   
        //.withSchedule(CronScheduleBuilder.cronSchedule("00 05 09 * * ?")) 
    }
    public void schedulerK2StorageJob() throws SchedulerException{
        JobDetail j_min = JobBuilder.newJob(com.its.main.Scheduler.class).withIdentity("min", "K2Storage").build();  
        Trigger t_min = TriggerBuilder.newTrigger().withIdentity("min", "K2Storage")
                            .withSchedule(CronScheduleBuilder.cronSchedule("00 0/5 * * * ?"))
                            .build();
        Scheduler s_min = getScheduler();
        s_min.scheduleJob(j_min, t_min);
        s_min.start();      
    }

    public void schedulerDatabaseDataJob(String cycle,String etlSource) throws SchedulerException{
    	Quartz.setCycle(cycle);
    	Quartz.setEtlSource(etlSource);
        String cycleLevel=new Tools().getStringFromString(cycle);
        Long times=new Tools().getDigitalFromString(cycle);
        CronScheduleBuilder cron=null;
    	if(cycleLevel.equalsIgnoreCase("min")){
    		cron=CronScheduleBuilder.cronSchedule("00 0/"+times+" * * * ?");
    	}else if(cycleLevel.equalsIgnoreCase("hour")){
    		cron=CronScheduleBuilder.cronSchedule("00 06 */"+times+" * * ?");
    	}else if(cycleLevel.equalsIgnoreCase("day")){
    		cron=CronScheduleBuilder.cronSchedule("00 00 08 */"+times+" * ?");
    	}else if(cycleLevel.equalsIgnoreCase("month")){
    		cron=CronScheduleBuilder.cronSchedule("00 00 08 01 */"+times+" ?");
    	}
    	System.out.println(cron);
    	
        JobDetail job = JobBuilder.newJob(com.its.main.Scheduler.class).withIdentity(cycle, "DBPerformanceToMysql").build();  
        Trigger trigger = TriggerBuilder.newTrigger().withIdentity(cycle, "Database")
                            .withSchedule(cron)
                            .build();
        Scheduler scheduler = getScheduler();
        scheduler.scheduleJob(job, trigger);
        scheduler.start();
    }    
    public void schedulerOSDataJob() throws SchedulerException{
        JobDetail job = JobBuilder.newJob(com.its.main.Scheduler.class).withIdentity("min", "OS").build();
        Trigger trigger = TriggerBuilder.newTrigger().withIdentity("min", "OS")
                            .withSchedule(CronScheduleBuilder.cronSchedule("00 0/5 * * * ?"))
                            .build();
        Scheduler scheduler = getScheduler();
        scheduler.scheduleJob(job, trigger);
        scheduler.start();      
    }  
    public void schedulerAlarmJob(String cycle) throws SchedulerException{
        String cycleLevel=new Tools().getStringFromString(cycle);
        Long times=new Tools().getDigitalFromString(cycle);
        if(cycleLevel.equalsIgnoreCase("min")){
        	cycleLevel="minute";
        	Quartz.setCycle(times+" "+cycleLevel);
        	System.out.println(times+" "+cycleLevel);
        }
        CronScheduleBuilder cron=null;
    	if(cycleLevel.equalsIgnoreCase("minute")){
    		cron=CronScheduleBuilder.cronSchedule("00 0/"+times+" * * * ?");
    	}else if(cycleLevel.equalsIgnoreCase("hour")){
    		cron=CronScheduleBuilder.cronSchedule("00 06 0/"+times+" * * ?");
    	}else if(cycleLevel.equalsIgnoreCase("day")){
    		cron=CronScheduleBuilder.cronSchedule("00 00 08 0/"+times+" * ?");
    	}else if(cycleLevel.equalsIgnoreCase("month")){
    		cron=CronScheduleBuilder.cronSchedule("00 00 08 01 0/"+times+" ?");
    	}        
    	JobDetail job = JobBuilder.newJob(com.its.main.Scheduler.class).withIdentity(cycle, "Alarm").build();  
        Trigger trigger = TriggerBuilder.newTrigger().withIdentity(cycle, "bakAlarm")
                            .withSchedule(cron)
                            .build();
        Scheduler scheduler = getScheduler();
        scheduler.scheduleJob(job, trigger);
        scheduler.start();
    }
    public void schedulersqlserverPublishAlarmJob() throws SchedulerException{
        JobDetail job = JobBuilder.newJob(com.its.main.Scheduler.class).withIdentity("min", "sqlserverPublishAlarm").build();  
        Trigger trigger = TriggerBuilder.newTrigger().withIdentity("min", "sqlserverPublishAlarm")
                            .withSchedule(CronScheduleBuilder.cronSchedule("00 0/30 * * * ?"))
                            .build();
        Scheduler scheduler = getScheduler();
        scheduler.scheduleJob(job, trigger);
        scheduler.start();      
    } 
    public void schedulersqlserverReportExcuteEelayedAlarm() throws SchedulerException{
        JobDetail job = JobBuilder.newJob(com.its.main.Scheduler.class).withIdentity("day", "sqlserverReportExcuteEelayedAlarm").build();  
        Trigger trigger = TriggerBuilder.newTrigger().withIdentity("day", "sqlserverReportExcuteEelayedAlarm")
                            .withSchedule(CronScheduleBuilder.cronSchedule("00 00 03 * * ?"))
                            .build();
        Scheduler scheduler = getScheduler();
        scheduler.scheduleJob(job, trigger);
        scheduler.start();      
    }
    public void schedulerDBPerformanceDataToHDFS() throws SchedulerException{
        JobDetail job = JobBuilder.newJob(com.its.main.Scheduler.class).withIdentity("erery1Hour", "DBPerformanceDataToHDFS").build();  
        Trigger trigger = TriggerBuilder.newTrigger().withIdentity("erery1hour", "DBPerformanceDataToHDFS")
                            .withSchedule(CronScheduleBuilder.cronSchedule("00 08,28,48 * * * ?"))
                            .build();
        Scheduler scheduler = getScheduler();
        scheduler.scheduleJob(job, trigger);
        scheduler.start();      
    } 
    public void schedulerCleanITOAMiddleData() throws SchedulerException{
        JobDetail job = JobBuilder.newJob(com.its.main.Scheduler.class).withIdentity("erery1Hour", "CleanITOAMiddleData").build();  
        Trigger trigger = TriggerBuilder.newTrigger().withIdentity("erery1Hour", "CleanITOAMiddleData")
                            .withSchedule(CronScheduleBuilder.cronSchedule("00 06 * * * ?"))
                            .build();
        Scheduler scheduler = getScheduler();
        scheduler.scheduleJob(job, trigger);
        scheduler.start();      
    } 
    public void schedulerVMAXStorageData() throws SchedulerException{
        JobDetail job = JobBuilder.newJob(com.its.main.Scheduler.class).withIdentity("erery10Min", "VMAXStorageData").build();  
        Trigger trigger = TriggerBuilder.newTrigger().withIdentity("erery10Min", "VMAXStorageData")
                            .withSchedule(CronScheduleBuilder.cronSchedule("02 * * * * ?"))
                            .build();
        Scheduler scheduler = getScheduler();
        scheduler.scheduleJob(job, trigger);
        scheduler.start();      
    } 
    public void schedulerTest() throws SchedulerException{
        JobDetail job = JobBuilder.newJob(com.its.main.Scheduler.class).withIdentity("test", "test").build();  
        Trigger trigger = TriggerBuilder.newTrigger().withIdentity("test", "test")
                            .withSchedule(CronScheduleBuilder.cronSchedule("02 * * * * ?"))
                            .build();
        Scheduler scheduler = getScheduler();
        scheduler.scheduleJob(job, trigger);
        scheduler.start();      
    }    
    public static void main(String [] args) throws SchedulerException{
    	Quartz quartz=new Quartz();
    	//���ݴ��ݹ����Ĳ����ж����������͵���ҵ������
    	if(args.length<1){
    		System.out.println("����дһ��job���Ͳ�����ֵΪECCReportJob��K2StorageJob,VMAXStorageJob,OSDataJob,AlarmJob," +
    				"SQLServerPublishAlarmJob,SqlserverReportExcuteEelayedAlarmJob,DBPerformanceDataJob,DBPerformanceDataToHDFSJob,CleanITOAMiddleDataJob�е�֮һ");
    		return;
    	}
    	if(args[0].equals("ECCReportJob")){
	    	quartz.schedulerECCJob();
    	}else if(args[0].equals("K2StorageJob")){
	    	quartz.schedulerK2StorageJob();
    	}
    	else if(args[0].equals("OSDataJob")){
	    	quartz.schedulerOSDataJob();
    	}
    	else if(args[0].equals("AlarmJob")){
	    	if(args.length<2){
	    		System.out.println("��������дһ����ȡƵ��Ϊxmin,xhour,xday,xweek,xmonth(XΪ��������)�е�һ������5min");
	    		return;
	    	}else if(!args[1].endsWith("min") && !args[1].endsWith("hour") &&!args[1].endsWith("day") && !args[1].endsWith("week") && !args[1].endsWith("month")){
	    		System.out.println("����д��ȷ��Ƶ�ʲ���Ϊxmin,xhour,xday,xweek,xmonth(XΪ��������)�е�һ������5min");
	    		return;
	    	}
	    	quartz.schedulerAlarmJob(args[1]);
    	} 
    	else if(args[0].equals("SQLServerPublishAlarmJob")){
	    	quartz.schedulersqlserverPublishAlarmJob();
    	} 
    	else if(args[0].equals("SqlserverReportExcuteEelayedAlarmJob")){
	    	quartz.schedulersqlserverReportExcuteEelayedAlarm();
    	}
    	else if(args[0].equals("DBPerformanceDataJob")){
	    	if(args.length<3){
	    		System.out.println("��������д����������1����ȡƵ��Ϊxmin,xhour,xday,xweek,xmonth(XΪ��������)�е�һ������5min��2������Դ�����Lenovo");
	    		return;
	    	}else if(!args[1].endsWith("min") && !args[1].endsWith("hour") &&!args[1].endsWith("day") && !args[1].endsWith("week") && !args[1].endsWith("month")){
	    		System.out.println("����д��ȷ��Ƶ�ʲ���Ϊxmin,xhour,xday,xweek,xmonth(XΪ��������)�е�һ������5min");
	    		return;
	    	}
    		quartz.schedulerDatabaseDataJob(args[1],args[2]);
    	}
    	else if(args[0].equals("DBPerformanceDataToHDFSJob")){
	    	quartz.schedulerDBPerformanceDataToHDFS();
    	}
    	else if(args[0].equals("CleanITOAMiddleDataJob")){
	    	quartz.schedulerCleanITOAMiddleData();
    	} 
    	else if(args[0].equals("VMAXStorageJob")){
	    	quartz.schedulerVMAXStorageData();
    	} 
    	else if(args[0].equals("test")){
	    	quartz.schedulerTest();
    	}     	
    	else{
    		System.out.println("����д��ȷ��job���Ͳ�����ֵΪECCReportJob��K2StorageJob,VMAXStorageJob,OSDataJob,AlarmJob," +
    				"SQLServerPublishAlarmJob,SqlserverReportExcuteEelayedAlarmJob,DBPerformanceDataJob,DBPerformanceDataToHDFSJob,CleanITOAMiddleDataJob�е�֮һ");
    		return;
    	}
    	
    }
    public static String getCycle(){
    	return Quartz.cycle;
    }
    public static void setCycle(String cycle){
    	Quartz.cycle=cycle;
    }
    public static String getEtlSource() {
		return etlSource;
	}
	public static void setEtlSource(String etlSource) {
		Quartz.etlSource = etlSource;
	}    
}
   
     

