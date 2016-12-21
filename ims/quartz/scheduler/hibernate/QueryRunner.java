/*
 * Created on 07-Oct-2005
 *
 * TODO To change the template for this generated file go to
 * Window - Preferences - Java - Code Generation - Code and Comments
 */
package ims.quartz.scheduler.hibernate;

import java.util.ArrayList;

public interface QueryRunner
{
	public ArrayList 	find(final String query, final String[] paramNames, final Object[] paramValues);
	public ArrayList 	find(final String query);
	public ArrayList 	findSql(final String query, final String[] paramNames, final Object[] paramValues);
	public ArrayList 	findSql(final String query);
	public boolean 		saveOrUpdate(final Object[] pojo);
	public Object 		getDomainObject(Class clazz, int id);
	public Object 		getDomainObject(Class clazz, Integer id);
	public boolean 		delete(final Object pojo);
}
