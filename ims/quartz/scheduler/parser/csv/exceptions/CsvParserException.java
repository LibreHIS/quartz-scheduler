/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ims.quartz.scheduler.parser.csv.exceptions;

/**
 *
 * @author vpurdila
 */
public class CsvParserException extends Exception
{
    public CsvParserException()
	{
		super();
	}

	public CsvParserException(String arg0)
	{
		super(arg0);
	}

	public CsvParserException(Throwable arg0)
	{
		super(arg0);
	}

	public CsvParserException(String arg0, Throwable arg1)
	{
		super(arg0, arg1);
	}
}
