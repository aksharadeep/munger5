using DbEngine.Query.Parser;
using DbEngine.Reader;
using DbEngine.Query;

namespace DbEngine.Query
{
    public class Query
    {
        QueryParser queryParser =null;
        QueryParameter queryParameter=null;

        /*
	 * This method will: 
	 * 1.parse the query and populate the QueryParameter object
	 * 2.Based on the type of query, it will select the appropriate Query processor.
	 * 
	 * In this example, we are going to work with only one Query Processor which is
	 * CsvQueryProcessor, which can work with select queries containing zero, one or
	 * multiple conditions
	 */

        public DataSet executeQuery(string queryString)
        {
            /* instantiate QueryParser class */

            /* call parseQuery() method of the class by passing the queryString which will return object of QueryParameter
             */


            /*
             * Check for Type of Query based on the QueryParameter object. In this assignment, we will process only queries containing zero, one or multiple where conditions i.e. conditions without aggregate functions, order by clause or group by clause
             */



            /*
             call the GetDataRow() method of CsvQueryProcessor class by passing the QueryParameter Object to it. This method is supposed to return DataSet
             */
            return null;
        }
    }
}
