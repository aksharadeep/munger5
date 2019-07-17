using System;
using System.IO;
using System.Globalization;
using System.Text.RegularExpressions;
using DbEngine.Query;
using DbEngine.Query.Parser;
using System.Collections.Generic;

namespace DbEngine.Reader
{
    public class CsvQueryProcessor: QueryProcessingEngine
    {
        /*
	    parameterized constructor to initialize filename. As you are trying to perform file reading, hence you need to be ready to handle the IO Exceptions.
	   */
        public CsvQueryProcessor(string fileName)
        {
        }

        /*
          read the first line which contains the header. Please note that the headers can contain spaces in between them. For eg: city, winner
          */
        public override Header GetHeader()
        {
            return null;
        }

        /*
	     implementation of getColumnType() method. To find out the data types, we will
	     read the first line after the header row from the file and extract the field values from it. In
	     the previous assignment, we have tried to convert a specific field value to
	     Integer or Double. However, in this assignment, we are going to use Regular
	     Expression to find the appropriate data type of a field. Integers: should
	     contain only digits without decimal point Double: should contain digits as
	     well as decimal point Date: Dates can be written in many formats in the CSV
	     file. However, in this assignment,we will test for the following date
	     formats('dd/mm/yyyy','mm/dd/yyyy','dd-mon-yy','dd-mon-yyyy','dd-month-yy','dd-month-yyyy','yyyy-mm-dd')
	    */
        public override DataTypeDefinitions GetColumnType() 
        {
            return null;
        }

        /*
	    This method will take QueryParameter object as a parameter which contains the parsed query and will process and populate the DataSet
	    */
        public override DataSet GetDataRow(QueryParameter queryParameter)
        {
             /*
					 * from QueryParameter object, read one condition at a time and evaluate the
					 * same. For evaluating the conditions, we will use evaluateExpressions() method
					 * of Filter class. Please note that evaluation of expression will be done
					 * differently based on the data type of the field. In case the query is having
					 * multiple conditions, you need to evaluate the overall expression i.e. if we
					 * have OR operator between two conditions, then the row will be selected if any
					 * of the condition is satisfied. However, in case of AND operator, the row will
					 * be selected only if both of them are satisfied.
					 */
            
            return null;
        }

    }
}