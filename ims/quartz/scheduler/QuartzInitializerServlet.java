/*
 * Created on 03-Oct-2005
 *
 */

/*
 * Copyright 2004-2005 OpenSymphony
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 */

/*
 * Previously Copyright (c) 2001-2004 James House
 */

package ims.quartz.scheduler;

import ims.configuration.gen.ConfigFlag;
import ims.quartz.scheduler.hibernate.HibernateProxy;
import ims.quartz.scheduler.job.CentralBatchPrintJob;
import ims.quartz.scheduler.job.DnaBatchJob;
import ims.quartz.scheduler.job.Generic;
import ims.quartz.scheduler.job.MonthlyActivityReport;
import ims.quartz.scheduler.job.OrderingLocationBatchPrintJob;
import ims.quartz.scheduler.job.PatientLetters;
import ims.quartz.scheduler.job.ProcedureSpecialtyLinkRecords;
import ims.quartz.scheduler.job.SatisfactionSurvey;
import ims.quartz.scheduler.job.TransactionExport;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.quartz.old.CronTrigger;
import org.quartz.old.JobDataMap;
import org.quartz.old.JobDetail;
import org.quartz.old.JobExecutionException;
import org.quartz.old.Scheduler;
import org.quartz.old.SchedulerException;
import org.quartz.old.impl.StdSchedulerFactory;

/**
 * <p>
 * A Servlet that can be used to initialize Quartz, if configured as a
 * load-on-startup servlet in a web application.
 * </p>
 * 
 * <p>
 * You'll want to add something like this to your WEB-INF/web.xml file:
 * 
 * <pre>
 *       &lt;servlet&gt;
 *           &lt;servlet-name&gt;
 *               QuartzInitializer
 *           &lt;/servlet-name&gt;
 *           &lt;display-name&gt;
 *               Quartz Initializer Servlet
 *           &lt;/display-name&gt;
 *           &lt;servlet-class&gt;
 *               org.quartz.ee.servlet.QuartzInitializerServlet
 *           &lt;/servlet-class&gt;
 *           &lt;load-on-startup&gt;
 *               1
 *           &lt;/load-on-startup&gt;
 *           &lt;init-param&gt;
 *               &lt;param-name&gt;config-file&lt;/param-name&gt;
 *               &lt;param-value&gt;/some/path/my_quartz.properties&lt;/param-value&gt;
 *           &lt;/init-param&gt;
 *           &lt;init-param&gt;
 *               &lt;param-name&gt;shutdown-on-unload&lt;/param-name&gt;
 *               &lt;param-value&gt;true&lt;/param-value&gt;
 *           &lt;/init-param&gt;
 *  
 *           &lt;init-param&gt;
 *               &lt;param-name&gt;start-scheduler-on-load&lt;/param-name&gt;
 *               &lt;param-value&gt;true&lt;/param-value&gt;
 *           &lt;/init-param&gt;
 *  
 *       &lt;/servlet&gt;
 * </pre>
 * 
 * </p>
 * <p>
 * The init parameter 'config-file' can be used to specify the path (and
 * filename) of your Quartz properties file. If you leave out this parameter,
 * the default ("quartz.properties") will be used.
 * </p>
 * 
 * <p>
 * The init parameter 'shutdown-on-unload' can be used to specify whether you
 * want scheduler.shutdown() called when the servlet is unloaded (usually when
 * the application server is being shutdown). Possible values are "true" or
 * "false". The default is "true".
 * </p>
 * 
 * <p>
 * The init parameter 'start-scheduler-on-load' can be used to specify whether
 * you want the scheduler.start() method called when the servlet is first
 * loaded. If set to false, your application will need to call the start()
 * method before teh scheduler begins to run and process jobs. Possible values
 * are "true" or "false". The default is "true", which means the scheduler is
 * started.
 * </p>
 * 
 * A StdSchedulerFactory instance is stored into the ServletContext. You can
 * gain access to the factory from a ServletContext instance like this: <br>
 * <code>
 * StdSchedulerFactory factory = (StdSchedulerFactory) ctx
 *				.getAttribute(QuartzFactoryServlet.QUARTZ_FACTORY_KEY);
 * </code>
 * <br>
 * Once you have the factory instance, you can retrieve the Scheduler instance
 * by calling <code>getScheduler()</code> on the factory.
 * 
 * @author James House
 * @author Chuck Cavaness
 */

public class QuartzInitializerServlet extends HttpServlet
{
	private static final String BAD_PARAMETERS = "Bad parameters. Usage: /QuartzInitializer?action=update|run&jobid=_id_of_job_|ping|start|stop|access&key=_control_key_here_";
	public static final String QUARTZ_FACTORY_KEY = "org.quartz.old.impl.StdSchedulerFactory.KEY";
	public static final int JOB_TYPE_PATIENT_LETTERS = -696;
	public static final int JOB_TYPE_SATISFACTION_SURVEY = -697;
	public static final int JOB_TYPE_TRANSACTION_EXPORT = -780;
	public static final int JOB_TYPE_GENERIC = -804;
	public static final int JOB_TYPE_PROCEDURE_SPECIALTY_LINK_RECORDS = -975;
	public static final int JOB_TYPE_MONTHLY_ACTIVITY_REPORT = -1593;
	public static final int JOB_TYPE_DNA_BATCH_JOB = -1872;
	public static final int JOB_TYPE_CENTRAL_BATCH_PRINT = -1959;	
	public static final int JOB_TYPE_ORDERING_LOCATION_BATCH_PRINT = -2061;
	

	private boolean performShutdown = true;

	private Scheduler scheduler = null;

	/*
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	 * 
	 * Interface.
	 * 
	 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	 */

	public void init(ServletConfig cfg) throws javax.servlet.ServletException
	{
		super.init(cfg);
		
		logMessage("Quartz Initializer Servlet loaded, init...");

		if(ConfigFlag.GEN.QUARTZ_SERVER_URL.getValue() == null || ConfigFlag.GEN.QUARTZ_SERVER_URL.getValue().trim().equals(""))
		{
			logMessage("QUARTZ_SERVER_URL flag not set...exiting init()...");
			return;
		}
		
		String tomcatHost = null;
		String tomcatIP = null;
		String quartzHost = null;
		
		try
		{
			tomcatHost = InetAddress.getLocalHost().getHostName();
			tomcatIP = InetAddress.getLocalHost().getHostAddress();
		}
		catch (UnknownHostException e1)
		{
			logMessage(e1.toString());
		}
		
		if(tomcatHost == null)
			return;
		
		try
		{
			URL url = new URL(ConfigFlag.GEN.QUARTZ_SERVER_URL.getValue());
			
			quartzHost = url.getHost();
		}
		catch (MalformedURLException e)
		{
			logMessage(e.toString());
		}
		
		if(quartzHost == null)
		{
			logMessage("Quartz host is null..." + quartzHost + " : " + tomcatHost);
			return;
		}

		if(!quartzHost.equalsIgnoreCase(tomcatHost) && !quartzHost.equalsIgnoreCase(tomcatIP))
		{
			logMessage("Quartz host not equal to tomcatHost or tomcatIP..." + quartzHost + " : " + tomcatHost + " : " + tomcatIP);
			return;
		}
		
		logMessage("Quartz Initializer Servlet loaded, initializing Scheduler...");

		StdSchedulerFactory factory;
		try
		{

			String configFile = cfg.getInitParameter("config-file");
			String shutdownPref = cfg.getInitParameter("shutdown-on-unload");

			if (shutdownPref != null)
				performShutdown = Boolean.valueOf(shutdownPref).booleanValue();

			// get Properties
			if (configFile != null)
			{
				factory = new StdSchedulerFactory(configFile);
			}
			else
			{
				factory = new StdSchedulerFactory();
			}

			// Should the Scheduler being started now or later
			String startOnLoad = cfg
					.getInitParameter("start-scheduler-on-load");
			
			if (startOnLoad == null)
				startOnLoad = "true";
			/*
			 * If the "start-scheduler-on-load" init-parameter is not specified,
			 * the scheduler will be started. This is to maintain backwards
			 * compatability.
			 */
			if (Boolean.valueOf(startOnLoad).booleanValue())
			{
				// Start now
				scheduler = factory.getScheduler();
				scheduler.start();
				logMessage("Scheduler has been started...");
				
				createJobs();				
			}
			else
			{
				logMessage("Scheduler has not been started. Use scheduler.start()");
			}

			logMessage("Storing the Quartz Scheduler Factory in the servlet context at key: "
					+ QUARTZ_FACTORY_KEY);
			cfg.getServletContext().setAttribute(QUARTZ_FACTORY_KEY, factory);

		}
		catch (Exception e)
		{
			logMessage("Quartz Scheduler failed to initialize: " + e.toString());
			throw new ServletException(e);
		}
		
		//ServletContext ctx = this.getServletContext();
		//System.out.println(ctx.getAttribute(QUARTZ_FACTORY_KEY));
	}

	private void createJobs() throws SchedulerException
	{
		String query = "select p1_1.id, p1_1.trigger, p1_1.description, p1_1.isActive, p1_1.jobType.id from PrintAgent as p1_1";
		Integer jobId;
		String triggerXml;
		Boolean isActive;
		Integer jobTypeId;
		
		HibernateProxy hp = new HibernateProxy();
		
		ArrayList al = hp.find(query);
		
		for(int i = 0; i < al.size(); i++)
		{
			Object[] obj = (Object[])al.get(i);

			// obj[0] = PrintAgent.id
			// obj[1] = PrintAgent.trigger
			// obj[2] = PrintAgent.description
			// obj[3] = PrintAgent.active
			// obj[4] = PrintAgent.jobType.id
			jobId = (Integer)obj[0];
			triggerXml = (String)obj[1];
			isActive = (Boolean)obj[3];
			jobTypeId = (Integer)obj[4];
			
			Class clazz = null;
			
			switch (jobTypeId.intValue())
			{
				case JOB_TYPE_PATIENT_LETTERS:
					clazz = PatientLetters.class;
					break;
				case JOB_TYPE_SATISFACTION_SURVEY:
					clazz = SatisfactionSurvey.class;
					break;
				case JOB_TYPE_TRANSACTION_EXPORT:
					clazz = TransactionExport.class;
					break;
				case JOB_TYPE_GENERIC:
					clazz = Generic.class;
					break;
				case JOB_TYPE_PROCEDURE_SPECIALTY_LINK_RECORDS:
					clazz = ProcedureSpecialtyLinkRecords.class;
					break;
				case JOB_TYPE_MONTHLY_ACTIVITY_REPORT:
					clazz = MonthlyActivityReport.class;
					break;
				case JOB_TYPE_DNA_BATCH_JOB:
					clazz = DnaBatchJob.class;
					break;	
				case JOB_TYPE_CENTRAL_BATCH_PRINT:
					clazz = CentralBatchPrintJob.class;
					break;
				case JOB_TYPE_ORDERING_LOCATION_BATCH_PRINT:
					clazz = OrderingLocationBatchPrintJob.class;
					break;
				default:
					break;
			}
				
			if(isActive.booleanValue())
			{
				JobDetail jobDetail = new JobDetail("ImsJob_" + String.valueOf(jobId), null, clazz);
				jobDetail.getJobDataMap().put("PrintAgentID", jobId);
				jobDetail.setDescription((String)obj[2]);
				try
				{
					ImsTrigger imsTrigger = new ImsTrigger(triggerXml);
					
					if(imsTrigger.getLastError() != null)
					{
						logMessage("Error parsing trigger XML : " + imsTrigger.getLastError());
					}
					else
					{
						CronTrigger trigger = new CronTrigger("Trigger_" + String.valueOf(jobId), null, jobDetail.getName(), null, imsTrigger.getCronString());
						trigger.setStartTime(imsTrigger.getStartDateTime().getJavaDate());
						trigger.setMisfireInstruction(CronTrigger.MISFIRE_INSTRUCTION_DO_NOTHING);
						
						scheduler.deleteJob(jobDetail.getName(), null);
						scheduler.scheduleJob(jobDetail, trigger);
												
					}
				}
				catch (ParseException e)
				{
					logMessage("Error scheduling job: " + e.toString());
				}
			}
		}
	}

	public void destroy()
	{

		if (!performShutdown)
			return;

		try
		{
			ServletContext ctx = this.getServletContext();
			StdSchedulerFactory factory = (StdSchedulerFactory)ctx.getAttribute(QUARTZ_FACTORY_KEY);
			
			Iterator schedulerIter = factory.getAllSchedulers().iterator();
			Scheduler scheduler = null;
			while (schedulerIter.hasNext())
			{
				scheduler = (Scheduler) schedulerIter.next();
				if (scheduler != null)
				{
					scheduler.shutdown();
				}
			}
			
		}
		catch (Exception e)
		{
			logMessage("Quartz Scheduler failed to shutdown cleanly: " + e.toString());
			e.printStackTrace();
		}

		logMessage("Quartz Scheduler successful shutdown.");
	}

	public void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException
	{
		doGet(request, response);
	}

	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException
	{
		//action can be "update", "run", "ping", "start", "stop", "access"
		String[] val = request.getParameterValues("action");
		
		if(val == null || val.length == 0)
		{
			response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, BAD_PARAMETERS);
			return;
		}
		
		String action = val[0];
		
		if(action.equalsIgnoreCase("ping"))
		{
			PrintWriter out = response.getWriter();
			out.print("OK");
			return;
		}
		else if(action.equalsIgnoreCase("start"))
		{
			PrintWriter out = response.getWriter();
			String status = startQuartz();
			
			if(status.equals("OK"))
			{
				out.print(status);
				return;
			}
			else
			{
				response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, status);
				return;
			}
		}
		else if(action.equalsIgnoreCase("stop"))
		{
			PrintWriter out = response.getWriter();
			String status = stopQuartz();

			if(status.equals("OK"))
			{
				out.print(status);
				return;
			}
			else
			{
				response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, status);
				return;
			}
		}
		/*
		if(action.equalsIgnoreCase("access"))
		{
			val = request.getParameterValues("key");

			if(val == null || val.length == 0)
			{
				response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, BAD_PARAMETERS);
				return;
			}

			PrintWriter out = response.getWriter();
			
			if(KEY.equals(val))
				out.print("OK");
			else
				out.print("ERROR");
			
			return;
		}
		*/
		else if(action == null || (!action.equalsIgnoreCase("run") && !action.equalsIgnoreCase("update")))
		{
			response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, BAD_PARAMETERS);
			return;
		}
		
		val = request.getParameterValues("jobid");

		if(val == null || val.length == 0)
		{
			response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, BAD_PARAMETERS);
			return;
		}
		
		String jobid = val[0];
		if(jobid == null || jobid.length() == 0)
		{
			response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, BAD_PARAMETERS);
			return;
		}
		
		String seeds = null;
		val = request.getParameterValues("seeds");

		if(val != null && val.length > 0)
		{
			seeds = val[0];
		}
		
		try
		{
			if(action.equalsIgnoreCase("update"))
				updateJob(jobid);
			else if(action.equalsIgnoreCase("run"))
				runJob(jobid, seeds);
			
		}
		catch (JobExecutionException e) 
		{
			e.printStackTrace();
			logMessage("Error scheduling job: " + e.toString());
			response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Error scheduling job: " + e.toString());
		}
		catch (SchedulerException e)
		{
			e.printStackTrace();
			logMessage("Error scheduling job: " + e.toString());
			response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Error scheduling job: " + e.toString());
		}
		catch (ParseException e)
		{
			e.printStackTrace();
			logMessage("Error scheduling job: " + e.toString());
			response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Error scheduling job: " + e.toString());
		}
		catch (Exception e)
		{
			e.printStackTrace();
			logMessage("Error scheduling job: " + e.toString());
			response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Error scheduling job: " + e.toString());
		}
	}

	private String stopQuartz()
	{
		try
		{
			ServletContext ctx = this.getServletContext();
			StdSchedulerFactory factory = (StdSchedulerFactory)ctx.getAttribute(QUARTZ_FACTORY_KEY);
			
			if(factory != null)
			{
				Iterator schedulerIter = factory.getAllSchedulers().iterator();
				Scheduler scheduler = null;
				while (schedulerIter.hasNext())
				{
					scheduler = (Scheduler) schedulerIter.next();
					if (scheduler != null)
					{
						scheduler.shutdown();
					}
				}
			}
		}
		catch (Exception e)
		{
			String err = "Quartz Scheduler failed to shutdown cleanly: " + e.toString();
			logMessage(err);
			e.printStackTrace();
			
			return err;
		}

		logMessage("Quartz Scheduler successful shutdown.");
		return "OK";
	}

	private String startQuartz()
	{
		logMessage("Quartz Initializer Servlet loaded, initializing Scheduler...");

		ServletContext ctx = this.getServletContext();
		StdSchedulerFactory factory = (StdSchedulerFactory)ctx.getAttribute(QUARTZ_FACTORY_KEY);
		
		try
		{
			if(factory == null)
				factory = new StdSchedulerFactory();
			
			scheduler = factory.getScheduler();
			scheduler.start();
			logMessage("Scheduler has been started...");
				
			createJobs();				

			logMessage("Storing the Quartz Scheduler Factory in the servlet context at key: "
					+ QUARTZ_FACTORY_KEY);
			this.getServletContext().setAttribute(QUARTZ_FACTORY_KEY, factory);

		}
		catch (SchedulerException e)
		{
			String err = "Quartz Scheduler failed to initialize: " + e.toString();
			logMessage(err);
			return err;
		}
		return "OK";
	}

	private void runJob(String jobid, String seeds) throws SchedulerException
	{
		ServletContext ctx = this.getServletContext();
		
		StdSchedulerFactory factory = (StdSchedulerFactory)ctx.getAttribute(QUARTZ_FACTORY_KEY);
		
		if(factory == null || factory.getAllSchedulers() == null || factory.getAllSchedulers().iterator() == null)
			reStartQuartz();

		factory = (StdSchedulerFactory)ctx.getAttribute(QUARTZ_FACTORY_KEY);
		
		Iterator schedulerIter = factory.getAllSchedulers().iterator();
		Scheduler _scheduler = null;
		while (schedulerIter.hasNext())
		{
			_scheduler = (Scheduler) schedulerIter.next();
			if (_scheduler != null)
			{
				if(seeds != null)
				{
					HashMap<String, String> map = new HashMap<String, String>();
					map.put("seeds", seeds);
					JobDataMap jdm = new JobDataMap(map);
					
					_scheduler.triggerJob("ImsJob_" + jobid, null, jdm);
				}
				else
				{
					_scheduler.triggerJob("ImsJob_" + jobid, null);
				}
				
				break; //we should have one scheduler
			}
		}
	}

	private boolean updateJob(String jobid) throws SchedulerException, ParseException
	{
		Integer jobId = Integer.valueOf(jobid);
		String triggerXml;
		Boolean isActive;
		Integer jobTypeId;
		String query = "select p1_1.id, p1_1.trigger, p1_1.description, p1_1.isActive, p1_1.jobType.id from PrintAgent as p1_1 where p1_1.id = :id";
		
		HibernateProxy hp = new HibernateProxy();
		
		ArrayList al = hp.find(query, new String[] {"id"}, new Object[] {jobId});

		if(al.size() == 0)
		{
			logMessage("I could not find the job in the database : " + jobid);
			return false;
		}
		
		Object[] obj = (Object[])al.get(0);

		// obj[0] = PrintAgent.id
		// obj[1] = PrintAgent.trigger
		// obj[2] = PrintAgent.description
		// obj[3] = PrintAgent.active
		// obj[4] = PrintAgent.jobType.id
		triggerXml = (String)obj[1];
		isActive = (Boolean)obj[3];
		jobTypeId = (Integer)obj[4];
		
		Class clazz = null;
		
		switch (jobTypeId.intValue())
		{
			case JOB_TYPE_PATIENT_LETTERS:
				clazz = PatientLetters.class;
				break;
			case JOB_TYPE_SATISFACTION_SURVEY:
				clazz = SatisfactionSurvey.class;
				break;
			case JOB_TYPE_TRANSACTION_EXPORT:
				clazz = TransactionExport.class;
				break;
			case JOB_TYPE_GENERIC:
				clazz = Generic.class;
				break;
			case JOB_TYPE_PROCEDURE_SPECIALTY_LINK_RECORDS:
				clazz = ProcedureSpecialtyLinkRecords.class;
				break;
			case JOB_TYPE_MONTHLY_ACTIVITY_REPORT:
				clazz = MonthlyActivityReport.class;
				break;
			case JOB_TYPE_DNA_BATCH_JOB:
				clazz = DnaBatchJob.class;
				break;	
			case JOB_TYPE_CENTRAL_BATCH_PRINT:
				clazz = CentralBatchPrintJob.class;
				break;
			case JOB_TYPE_ORDERING_LOCATION_BATCH_PRINT:
				clazz = OrderingLocationBatchPrintJob.class;
				break;
			default:
				break;
		}
	
		JobDetail jobDetail = new JobDetail("ImsJob_" + String.valueOf(jobId), null, clazz);
		jobDetail.getJobDataMap().put("PrintAgentID", jobId);
		jobDetail.setDescription((String)obj[2]);
		
		ImsTrigger imsTrigger = new ImsTrigger(triggerXml);
		
		if(imsTrigger.getLastError() != null)
		{
			logMessage("Error parsing trigger XML : " + imsTrigger.getLastError());
		}
		else
		{		
			CronTrigger trigger = new CronTrigger("Trigger_" + String.valueOf(jobId), null, jobDetail.getName(), null, imsTrigger.getCronString());
			trigger.setStartTime(imsTrigger.getStartDateTime().getJavaDate());
			trigger.setMisfireInstruction(CronTrigger.MISFIRE_INSTRUCTION_DO_NOTHING);

			ServletContext ctx = this.getServletContext();
	
			StdSchedulerFactory factory = (StdSchedulerFactory)ctx.getAttribute(QUARTZ_FACTORY_KEY);
			
			if(factory == null || factory.getAllSchedulers() == null || factory.getAllSchedulers().iterator() == null)
				reStartQuartz();

			factory = (StdSchedulerFactory)ctx.getAttribute(QUARTZ_FACTORY_KEY);
			
			Iterator schedulerIter = factory.getAllSchedulers().iterator();
			Scheduler _scheduler = null;
			while (schedulerIter.hasNext())
			{
				_scheduler = (Scheduler) schedulerIter.next();
				if (_scheduler != null)
				{
					_scheduler.deleteJob(jobDetail.getName(), null);
					
					if(isActive.booleanValue())
						_scheduler.scheduleJob(jobDetail,  trigger);
				}
			}
		}
		
		return true;
	}
	
	private String reStartQuartz()
	{
		String result = "";
		
		stopQuartz();
		result = startQuartz();

		return result;
	}
	
	private void logMessage(String str)
	{
		System.out.println(str);
	}
}
