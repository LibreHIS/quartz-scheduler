/*
 * Created on 26-Oct-2005
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Generation - Code and Comments
 */
package ims.quartz.scheduler.hibernate;

public class HqlBatchUpdate
{
	private String hqlUpdate;
	private String[] paramNames;
	private Object[] paramValues;

	
	public HqlBatchUpdate(String hqlUpdate, String[] paramNames, Object[] paramValues)
	{
		this.hqlUpdate = hqlUpdate;
		this.paramNames = paramNames;
		this.paramValues = paramValues;
	}

	public HqlBatchUpdate(String hqlUpdate)
	{
		this.hqlUpdate = hqlUpdate;
	}
	
	public HqlBatchUpdate()
	{
		this.hqlUpdate = "";
	}

	public String getHqlUpdate()
	{
		return hqlUpdate;
	}

	public void setHqlUpdate(String hqlUpdate)
	{
		this.hqlUpdate = hqlUpdate;
	}

	public String[] getParamNames()
	{
		return paramNames;
	}

	public void setParamNames(String[] paramNames)
	{
		this.paramNames = paramNames;
	}

	public Object[] getParamValues()
	{
		return paramValues;
	}

	public void setParamValues(Object[] paramValues)
	{
		this.paramValues = paramValues;
	}

}
