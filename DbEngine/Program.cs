﻿using System;

namespace DbEngine
{
    class Program
    {
        static void Main(string[] args)
        {
            // Read the query from the user

            /*
              Instantiate Query class. This class is responsible for:
              1. Parsing the query.
              2. Select the appropriate type of query processor.
              3. Get the DataSet which is populated by the Query Processor
             */


            /*
             * Instantiate JsonWriter class. This class is responsible for writing the DataSet into a JSON file
             */

            /*
             * call executeQuery() method of Query class to get the resultSet. Pass this resultSet as parameter to writeToJson() method of JsonWriter class to write the resultSet into a JSON file
             */
        }
    }
}
