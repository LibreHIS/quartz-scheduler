/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package ims.quartz.scheduler.parser.csv;

import ims.quartz.scheduler.parser.csv.exceptions.CsvParserException;

/**
 *
 * @author vpurdila
 */
public class CsvParser
{
    /*
    Each field on the line must be enclosed in double quotes and separated by comma
    Example:

    "123", "Hello", "A field that contains comma , and double quote ""..."

    The above line actually contains actually 3 fields:

    123
    Hello
    A field that contains comma , and double quote "...
    */
    public String[] parseLine(String line) throws CsvParserException
    {
        String[] ret = null;

        if (line == null)
        {
            return ret;
        }

        int len = line.length();
        int delim = 0;
        int dblquotes = 0;
        
        for (int i = 0; i < len; i++)
        {
            if(line.charAt(i) == '"')
                dblquotes++;
            
            if (line.charAt(i) == ',' && dblquotes % 2 == 0)
            {
                delim++;
            }
        }

        if(dblquotes % 2 != 0)
            throw new CsvParserException("The line: " + line + " doesn't have the correct number of double quote characters !");

        if (delim == 0)
        {
            return ret;
        }

        ret = new String[delim + 1];

        StringBuilder sb = new StringBuilder();
        int index = 0;
        dblquotes = 0;
        boolean prevWasDoubleQuote = false;
        boolean removeCurrentChar;

        for (int i = 0; i < len; i++)
        {
            removeCurrentChar = false;
            
            if(line.charAt(i) == '"')
            {
                dblquotes++;
                
                if(prevWasDoubleQuote == false)
                    prevWasDoubleQuote = true;
                else
                {
                    prevWasDoubleQuote = false;
                    removeCurrentChar = true;
                }
            }
            else
            {
                prevWasDoubleQuote = false;
            }

            if (line.charAt(i) == ',' && dblquotes % 2 == 0)
            {
                ret[index++] = sb.toString();
                sb.setLength(0);
                prevWasDoubleQuote = false;

                if("\"".equalsIgnoreCase(ret[index - 1]))
                {
                    ret[index - 1] = "";
                }

                if(ret[index - 1] != null && ret[index - 1].startsWith("\"") && ret[index - 1].endsWith("\""))
                {
                    ret[index - 1] = ret[index - 1].substring(1, ret[index - 1].length() - 1);
                }
            }
            else
            {
                if(removeCurrentChar == false)
                {
                    sb.append(line.charAt(i));
                }
            }
        }

        ret[index] = sb.toString();
        if("\"".equalsIgnoreCase(ret[index]))
        {
            ret[index] = "";
        }

        if(ret[index] != null && ret[index].startsWith("\"") && ret[index].endsWith("\""))
        {
            ret[index] = ret[index].substring(1, ret[index].length() - 1);
        }
        

        return ret;
    }

    public String[] split(char delimiter, String line)
    {
        String[] ret = null;

        if (line == null)
        {
            return ret;
        }

        int len = line.length();
        int delim = 0;
        for (int i = 0; i < len; i++)
        {
            if (line.charAt(i) == delimiter)
            {
                delim++;
            }
        }

        if (delim == 0)
        {
            return ret;
        }

        ret = new String[delim + 1];

        StringBuilder sb = new StringBuilder();
        int index = 0;
        for (int i = 0; i < len; i++)
        {
            if (line.charAt(i) == delimiter)
            {
                ret[index++] = sb.toString();
                sb.setLength(0);
            }
            else
            {
                sb.append(line.charAt(i));
            }
        }

        ret[index] = sb.toString();

        return ret;
    }
}
