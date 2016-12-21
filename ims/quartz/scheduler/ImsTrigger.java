/*
 * Created on 17-Oct-2005
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Generation - Code and Comments
 */
package ims.quartz.scheduler;

import ims.framework.utils.DateTime;
import ims.framework.utils.DateTimeFormat;
import ims.framework.utils.StringUtils;
import ims.framework.utils.Time;
import ims.framework.utils.TimeFormat;

import java.text.ParseException;
import java.util.Calendar;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Node;

public class ImsTrigger
{
	private static String DAILY = "Daily";
	private static String WEEKLY = "Weekly";
	private static String MONTHLY	= "Monthly";
	private static String PERIODICALLY	= "Periodically";
	
	private String frequency;
	private DateTime startDateTime;
	private Integer days;
	private Integer weeks;
	private boolean mon;
	private boolean tue;
	private boolean wed;
	private boolean thu;
	private boolean fri;
	private boolean sat;
	private boolean sun;
	private Integer		dayOfMonth;
	private Time 		timeOfMonth;
	private Integer		mins;
	
	private String lastError = null;

	public ImsTrigger()
	{
	}

	public ImsTrigger(String xml)
	{
		try
		{
			this.initFromXML(xml);
		}
		catch (DocumentException e)
		{
			lastError = e.toString();
		}
		catch (ParseException e)
		{
			lastError = e.toString();
		}
		catch (Exception e)
		{
			lastError = e.toString();
		}
	}
	
	public Integer getDays()
	{
		return days;
	}

	public void setDays(Integer days)
	{
		this.days = days;
	}

	public String getFrequency()
	{
		return frequency;
	}

	public void setFrequency(String frequency)
	{
		this.frequency = frequency;
	}

	public boolean isFri()
	{
		return fri;
	}

	public void setFri(boolean fri)
	{
		this.fri = fri;
	}

	public boolean isMon()
	{
		return mon;
	}

	public void setMon(boolean mon)
	{
		this.mon = mon;
	}

	public boolean isSat()
	{
		return sat;
	}

	public void setSat(boolean sat)
	{
		this.sat = sat;
	}

	public DateTime getStartDateTime()
	{
		return startDateTime;
	}

	public void setStartDateTime(DateTime startDateTime)
	{
		this.startDateTime = startDateTime;
	}

	public boolean isSun()
	{
		return sun;
	}

	public void setSun(boolean sun)
	{
		this.sun = sun;
	}

	public boolean isThu()
	{
		return thu;
	}

	public void setThu(boolean thu)
	{
		this.thu = thu;
	}

	public boolean isTue()
	{
		return tue;
	}

	public void setTue(boolean tue)
	{
		this.tue = tue;
	}

	public boolean isWed()
	{
		return wed;
	}

	public void setWed(boolean wed)
	{
		this.wed = wed;
	}

	public Integer getWeeks()
	{
		return weeks;
	}

	public void setWeeks(Integer weeks)
	{
		this.weeks = weeks;
	}
	
	public Integer getDayOfMonth()
	{
		return dayOfMonth;
	}

	public void setDayOfMonth(Integer dayOfMonth)
	{
		this.dayOfMonth = dayOfMonth;
	}

	public Time getTimeOfMonth()
	{
		return timeOfMonth;
	}

	public void setTimeOfMonth(Time timeOfMonth)
	{
		this.timeOfMonth = timeOfMonth;
	}
	
	public Integer getMins()
	{
		return mins;
	}

	public void setMins(Integer mins)
	{
		this.mins = mins;
	}
	
	public String toXML()
	{
		if (frequency == null || frequency.length() == 0 || (days == null && weeks == null && WEEKLY.equals(frequency)) || (startDateTime == null && !MONTHLY.equals(frequency)))
			return null;
		
		StringBuffer sb = new StringBuffer();
		
		sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>");
		sb.append("<trigger>");
			sb.append("<frequency>");
				sb.append(StringUtils.encodeXML(frequency));
			sb.append("</frequency>");
			sb.append("<startdatetime>");
				sb.append(StringUtils.encodeXML(startDateTime != null ? startDateTime.toString(DateTimeFormat.ISO) : ""));
			sb.append("</startdatetime>");
			sb.append("<days>");
				sb.append(days != null ? String.valueOf(days) : "");
			sb.append("</days>");
			sb.append("<weeks>");
				sb.append(weeks != null ? String.valueOf(weeks) : "");
			sb.append("</weeks>");
			sb.append("<mon>");
				sb.append(mon);
			sb.append("</mon>");
			sb.append("<tue>");
				sb.append(tue);
			sb.append("</tue>");
			sb.append("<wed>");
				sb.append(wed);
			sb.append("</wed>");
			sb.append("<thu>");
				sb.append(thu);
			sb.append("</thu>");
			sb.append("<fri>");
				sb.append(fri);
			sb.append("</fri>");
			sb.append("<sat>");
				sb.append(sat);
			sb.append("</sat>");
			sb.append("<sun>");
				sb.append(sun);
			sb.append("</sun>");
			
			sb.append("<dayofmonth>");
			sb.append(dayOfMonth != null ? String.valueOf(dayOfMonth) : "");
			sb.append("</dayofmonth>");
			sb.append("<timeofmonth>");
			sb.append(StringUtils.encodeXML(timeOfMonth != null ? timeOfMonth.toString(TimeFormat.FLAT4) : ""));
			sb.append("</timeofmonth>");
			
			sb.append("<mins>");
			sb.append(mins != null ? String.valueOf(mins) : "");
			sb.append("</mins>");
			
		sb.append("</trigger>");
		
		return sb.toString();
	}
	
	private void initFromXML(String xmlTrigger) throws DocumentException, ParseException
	{
		Document maindoc = getXmlDocument(xmlTrigger);
	
		Node node = maindoc.selectSingleNode("trigger/frequency");
		
		if(node != null)
			frequency = node.getStringValue();
		node = maindoc.selectSingleNode("trigger/startdatetime");
		
		if(node != null && node.getStringValue() != null && node.getStringValue().length() > 0)
			startDateTime = new DateTime(node.getStringValue());
		node = maindoc.selectSingleNode("trigger/days");
		if(node != null && node.getStringValue() != null && node.getStringValue().length() > 0)
			days = new Integer(Integer.parseInt(node.getStringValue()));
		node = maindoc.selectSingleNode("trigger/weeks");
		if(node != null && node.getStringValue() != null && node.getStringValue().length() > 0)
			weeks = new Integer(Integer.parseInt(node.getStringValue()));

		node = maindoc.selectSingleNode("trigger/mon");
		if(node != null && node.getStringValue() != null && node.getStringValue().length() > 0)
			mon = node.getStringValue().equalsIgnoreCase("true") ? true : false;
		node = maindoc.selectSingleNode("trigger/tue");
		if(node != null && node.getStringValue() != null && node.getStringValue().length() > 0)
			tue = node.getStringValue().equalsIgnoreCase("true") ? true : false;
		node = maindoc.selectSingleNode("trigger/wed");
		if(node != null && node.getStringValue() != null && node.getStringValue().length() > 0)
			wed = node.getStringValue().equalsIgnoreCase("true") ? true : false;
		node = maindoc.selectSingleNode("trigger/thu");
		if(node != null && node.getStringValue() != null && node.getStringValue().length() > 0)
			thu = node.getStringValue().equalsIgnoreCase("true") ? true : false;
		node = maindoc.selectSingleNode("trigger/fri");
		if(node != null && node.getStringValue() != null && node.getStringValue().length() > 0)
			fri = node.getStringValue().equalsIgnoreCase("true") ? true : false;
		node = maindoc.selectSingleNode("trigger/sat");
		if(node != null && node.getStringValue() != null && node.getStringValue().length() > 0)
			sat = node.getStringValue().equalsIgnoreCase("true") ? true : false;
		node = maindoc.selectSingleNode("trigger/sun");
		if(node != null && node.getStringValue() != null && node.getStringValue().length() > 0)
			sun = node.getStringValue().equalsIgnoreCase("true") ? true : false;
		
		node = maindoc.selectSingleNode("trigger/dayofmonth");
		if (node != null && node.getStringValue() != null && node.getStringValue().length() > 0)
			dayOfMonth = new Integer(Integer.parseInt(node.getStringValue()));
		node = maindoc.selectSingleNode("trigger/timeofmonth");
		if(node != null && node.getStringValue() != null && node.getStringValue().length() > 0)
			timeOfMonth = new Time(node.getStringValue(), TimeFormat.FLAT4);
		
		node = maindoc.selectSingleNode("trigger/mins");
		if (node != null && node.getStringValue() != null && node.getStringValue().length() > 0)
			mins = new Integer(Integer.parseInt(node.getStringValue()));
	}
	
	private Document getXmlDocument(String xmlBuffer) throws DocumentException
	{
		return DocumentHelper.parseText(xmlBuffer);
	}

	public String getHtmlTranslation()
	{
		return getTriggerTranslation(true);
	}

	public String getTextTranslation()
	{
		return getTriggerTranslation(false);
	}
	
	public String getTriggerTranslation(boolean bHtml)
	{
		if (frequency == null || frequency.length() == 0 || (days == null && weeks == null && WEEKLY.equals(frequency)) || (startDateTime == null && !MONTHLY.equals(frequency)))
			return null;

		String fontHeader = "";
		String fontFooter = "";

		if (bHtml)
		{
			fontHeader = "<FONT color=#0000FF>";
			fontFooter = "</FONT>";
		}

		StringBuffer sb = new StringBuffer(100);

		if (frequency.equals(DAILY))
		{
			if (bHtml)
				sb.append("&nbsp;At ");
			else
				sb.append("At ");
			
			sb.append(fontHeader);
			sb.append(startDateTime.getTime().toString());
			sb.append(fontFooter);
			
			sb.append(" every ");

			if (days.intValue() == 1)
				sb.append("day");
			else
			{
				sb.append(fontHeader);
				sb.append(days.toString());
				sb.append(fontFooter);
				sb.append(" days");
			}

			sb.append(", starting ");
			sb.append(fontHeader);
			sb.append(startDateTime.getDate().toString());
			sb.append(fontFooter);

			return sb.toString();
		}
		else if (frequency.equals(WEEKLY))
		{
			if (bHtml)
				sb.append("&nbsp;At ");
			else
				sb.append("At ");
			
			sb.append(fontHeader);
			sb.append(startDateTime.getTime().toString());
			sb.append(fontFooter);
			
			sb.append(" every ");

			String comma = "";

			if (mon == false && tue == false && wed == false && thu == false && fri == false && sat == false && sun == false)
				return "";

			if (mon == true)
			{
				sb.append(fontHeader);
				sb.append("Mon");
				sb.append(fontFooter);
				comma = ",";
			}
			if (tue == true)
			{
				sb.append(comma);
				sb.append(fontHeader);
				sb.append("Tue");
				sb.append(fontFooter);
				comma = ",";
			}
			if (wed == true)
			{
				sb.append(comma);
				sb.append(fontHeader);
				sb.append("Wed");
				sb.append(fontFooter);
				comma = ",";
			}
			if (thu == true)
			{
				sb.append(comma);
				sb.append(fontHeader);
				sb.append("Thu");
				sb.append(fontFooter);
				comma = ",";
			}
			if (fri == true)
			{
				sb.append(comma);
				sb.append(fontHeader);
				sb.append("Fri");
				sb.append(fontFooter);
				comma = ",";
			}
			if (sat == true)
			{
				sb.append(comma);
				sb.append(fontHeader);
				sb.append("Sat");
				sb.append(fontFooter);
				comma = ",";
			}
			if (sun == true)
			{
				sb.append(comma);
				sb.append(fontHeader);
				sb.append("Sun");
				sb.append(fontFooter);
				comma = ",";
			}

			if (weeks.intValue() == 1)
				sb.append(" of every week");
			else
			{
				sb.append(" of every ");
				sb.append(fontHeader);
				sb.append(weeks.toString());
				sb.append(fontFooter);
				sb.append(" weeks");
			}

			sb.append(", starting ");
			sb.append(fontHeader);
			sb.append(startDateTime.getDate().toString());
			sb.append(fontFooter);

			return sb.toString();
		}
		else if (frequency.equals(MONTHLY))
		{
			if (bHtml)
				sb.append("&nbsp;At ");
			else
				sb.append("At ");
			
			if(bHtml)
				sb.append("<FONT color=#0000FF>");
			
			if(timeOfMonth != null)
				sb.append(timeOfMonth.toString());
			
			if(bHtml)
				sb.append("</FONT>");
			
			sb.append(" on the ");
			
			if(bHtml)
				sb.append("<FONT color=#0000FF>");
			
			if(dayOfMonth != null)
			{
				sb.append(dayOfMonth.intValue());
			
				if(dayOfMonth.intValue() == 1 || dayOfMonth.intValue() == 21 || dayOfMonth.intValue() == 31)
					sb.append("st ");
				else if(dayOfMonth.intValue() == 2 || dayOfMonth.intValue() == 22)
					sb.append("nd ");
				else if(dayOfMonth.intValue() == 3 || dayOfMonth.intValue() == 23)
					sb.append("rd ");
				else 
					sb.append("th ");
			}
			
			if(bHtml)
				sb.append("</FONT>");
			
			sb.append("day every month");
			
			sb.append(", starting ");
			
			if(bHtml)
				sb.append("<FONT color=#0000FF>");
			
			sb.append(startDateTime.getDate().toString());
			
			if(bHtml)
				sb.append("</FONT>");
			
			return sb.toString();
		}
	
		return null;
	}

	public String getLastError()
	{
		return lastError;
	}
	
	public String getCronString()
	{
		StringBuffer cronString = new StringBuffer();
		
		java.util.Calendar cal = Calendar.getInstance();
		
		if(frequency.equals(DAILY))
		{
			cal.setTime(startDateTime.getJavaDate());

			cronString.append(cal.get(Calendar.SECOND)); //second
			cronString.append(" ");
			cronString.append(cal.get(Calendar.MINUTE)); //minute
			cronString.append(" ");
			cronString.append(cal.get(Calendar.HOUR_OF_DAY)); //hour
			cronString.append(" ");
			cronString.append("*"); //Day of month
			cronString.append("/");
			cronString.append(days);
			cronString.append(" *"); //Month
			cronString.append(" ?"); //Day of week
		}
		else if(frequency.equals(WEEKLY))
		{
			cal.setTime(startDateTime.getJavaDate());
			
			String comma = "";
			StringBuffer weekDays = new StringBuffer();
			
			if(mon == true)
			{
				weekDays.append("Mon");
				comma = ",";
			}
			if(tue == true)
			{
				weekDays.append(comma);
				weekDays.append("Tue");
				comma = ",";
			}
			if(wed == true)
			{
				weekDays.append(comma);
				weekDays.append("Wed");
				comma = ",";
			}
			if(thu == true)
			{
				weekDays.append(comma);
				weekDays.append("Thu");
				comma = ",";
			}
			if(fri == true)
			{
				weekDays.append(comma);
				weekDays.append("Fri");
				comma = ",";
			}
			if(sat == true)
			{
				weekDays.append(comma);
				weekDays.append("Sat");
				comma = ",";
			}
			if(sun == true)
			{
				weekDays.append(comma);
				weekDays.append("Sun");
				comma = ",";
			}
			
			cronString.append(cal.get(Calendar.SECOND)); //second
			cronString.append(" ");
			cronString.append(cal.get(Calendar.MINUTE)); //minute
			cronString.append(" ");
			cronString.append(cal.get(Calendar.HOUR_OF_DAY)); //hour
			cronString.append(" ?"); //Day of month
			cronString.append(" *"); //Month
			cronString.append(" "); 
			cronString.append(weekDays.toString()); //Day of week
		}
		else if(frequency.equals(MONTHLY))
		{
			cronString.append(timeOfMonth.getSecond()); //second
			cronString.append(" ");
			cronString.append(timeOfMonth.getMinute()); //minute
			cronString.append(" ");
			cronString.append(timeOfMonth.getHour()); //hour
			cronString.append(" ");
			cronString.append(dayOfMonth); //Day of month
			cronString.append(" *"); //Month
			cronString.append(" ?"); //Day of week
		}
		else if(frequency.equals(PERIODICALLY))
		{		
			cronString.append("0 0/" + mins + " * * * ?"); 
		}
		
		return cronString.toString();
	}
}
