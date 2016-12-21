/*
 * Created on 5 Dec 2008
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Generation - Code and Comments
 */
package ims.quartz.scheduler.helper;

public class HashLink
{
	private String boName;
	private String fieldName;

	public HashLink()
	{
	}
	
	public HashLink(String boName, String fieldName)
	{
		this.boName = boName;
		this.fieldName = fieldName;
	}
	
	public String getBoName()
	{
		return boName;
	}
	public void setBoName(String boName)
	{
		this.boName = boName;
	}
	public String getFieldName()
	{
		return fieldName;
	}
	public void setFieldName(String fieldName)
	{
		this.fieldName = fieldName;
	}

}
