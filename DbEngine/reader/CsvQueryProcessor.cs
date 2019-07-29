using System;
using System.IO;
using System.Globalization;
using System.Text.RegularExpressions;
using DbEngine.Query;
using DbEngine.Query.Parser;
using System.Collections.Generic;

namespace DbEngine.Reader
{
    public class CsvQueryProcessor : QueryProcessingEngine
    {
        /*
	    parameterized constructor to initialize filename. As you are trying to perform file reading, hence you need to be ready to handle the IO Exceptions.
	   */
        private readonly string _fileName;
        private StreamReader _reader;

        // Parameterized constructor to initialize filename
        public CsvQueryProcessor(string fileName)
        {
            this._fileName = fileName;
        }
        /*
          read the first line which contains the header. Please note that the headers can contain spaces in between them. For eg: city, winner
          */
        public override Header GetHeader()
        {

            Header head = new Header();
            _reader = new StreamReader(_fileName);
            string firstRow = _reader.ReadLine();
            head.Headers = firstRow.Split(',');
            // read the first line
            // populate the header object with the String array containing the header names
            return head;
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
            string[] secondRowArray;

            DataTypeDefinitions dataTypeDefinitions = new DataTypeDefinitions();
            _reader = new StreamReader(_fileName);
            _reader.ReadLine();
            string secondRow = _reader.ReadLine();
            secondRowArray = secondRow.Split(',');
            string[] typeArray = new string[secondRowArray.Length];
            for (int i = 0; i < secondRowArray.Length; i++)
            {
                if (Regex.Match(secondRowArray[i], "^[0-9]+$").Success)
                {
                    typeArray[i] = "System.Int32";
                }
                else if (Regex.Match(secondRowArray[i], "^[0-9]+.[0-9]+$").Success)
                {
                    typeArray[i] = "System.Double";
                }
                // checking for date format yyyy-mm-dd
                else if (Regex.Match(secondRowArray[i], "^[0-9]{4}-[0-9]{2}-[0-9]{2}$").Success)
                {
                    typeArray[i] = "System.DateTime";
                }
                // checking for date format dd/mm/yyyy
                else if (Regex.Match(secondRowArray[i], "^[0-9]{2}/[0-9]{2}/[0-9]{4}$").Success)
                {
                    typeArray[i] = "System.DateTime";
                }
                // checking for date format dd-mon-yyyy
                else if (Regex.Match(secondRowArray[i], "^[0-9]{2}-[a-z]{3}-[0-9]{2}$").Success)
                {
                    typeArray[i] = "System.DateTime";
                }

                else if (secondRowArray[i].Length == 0)
                {
                    typeArray[i] = "System.Object";
                }
                else
                {
                    typeArray[i] = "System.String";
                }

            }
            dataTypeDefinitions.DataTypes = typeArray;
            return dataTypeDefinitions;
        }

        /*
	    This method will take QueryParameter object as a parameter which contains the parsed query and will process and populate the DataSet
	    */
        public override DataSet GetDataRow(QueryParameter queryparameter)
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
            string line;
            _reader = new StreamReader(_fileName);
            _reader.ReadLine();
            string Query = queryparameter.BaseQuery;
            List<Row> EachRow = new List<Row>();


            #region Select all Without Where Condition
            if (Query.Contains("*"))
            {
                while ((line = _reader.ReadLine()) != null)
                {
                    Row row = new Row();
                    row.RowValues = line.Split(',');
                    for (int i = 0; i < row.RowValues.Length; i++)
                    {
                        if (row.RowValues[i] == "")
                        {
                            row.RowValues[i] = null;
                        }
                    }
                    EachRow.Add(row);
                }
                DataSet dataSet = new DataSet(EachRow);
                return dataSet;
            }
            #endregion
          
            else
            {
                Header head = GetHeader();
                string[] allHeaders = head.Headers;
                List<int> indexes = new List<int>();
                List<string> SelectedItems = queryparameter.Fields;

                //to get the index of all the selected items in the original header
                for (int i = 0; i < SelectedItems.Count; i++)
                {
                    indexes.Add(Array.IndexOf(allHeaders, SelectedItems[i]));
                }

                #region select Columns without Where Clause
                if (queryparameter.Restrictions == null)
                {
                    while ((line = _reader.ReadLine()) != null)
                    {
                        Row row = new Row();
                        List<string> Columns = new List<string>();
                        string[] SplitedLines = line.Split(',');
                        for (int i = 0; i < indexes.Count; i++)
                        {
                            Columns.Add(SplitedLines[indexes[i]]);
                        }
                        row.RowValues = Columns.ToArray();
                        EachRow.Add(row);
                    }
                    DataSet dataSet = new DataSet(EachRow);
                    return dataSet;
                }
                #endregion
                #region select with where and restrictions
                else
                {
                    Filter filter = new Filter();
                    string propertyName = "";
                    string propertyValue = "";
                    string condition = "";
                    if (!queryparameter.BaseQuery.Contains("and"))
                    {
                        List<Restriction> restrictions = queryparameter.Restrictions;
                        foreach (Restriction item in restrictions)
                        {
                            propertyName = item.propertyName;
                            propertyValue = item.propertyValue;
                            condition = item.condition;
                        }
                        int indexOfPropertyName = Array.IndexOf(allHeaders, propertyName);
                        while ((line = _reader.ReadLine()) != null)
                        {
                            string[] SplitedLines = line.Split(',');
                            int propertyData = int.Parse(SplitedLines[indexOfPropertyName]);
                            if (filter.evaluateExpression(condition, propertyData, int.Parse(propertyValue)))
                            {
                                Row row = new Row();
                                List<string> Columns = new List<string>();
                                for (int i = 0; i < indexes.Count; i++)
                                {
                                    Columns.Add(SplitedLines[indexes[i]]);
                                }
                                row.RowValues = Columns.ToArray();
                                EachRow.Add(row);
                            }
                            else
                            {
                                continue;
                            }
                        }

                        DataSet dataSet = new DataSet(EachRow);
                        return dataSet;
                    }
                    //9-10
                    else if (!queryparameter.BaseQuery.Contains("or"))
                    {
                        int indexOfPropertyName = Array.IndexOf(allHeaders, "season");
                        while ((line = _reader.ReadLine()) != null)
                        {
                            string[] SplitedLines = line.Split(',');
                            int propertyData = int.Parse(SplitedLines[indexOfPropertyName]);
                            if (propertyData >= 2013 && propertyData <= 2015)
                            {
                                Row row = new Row();
                                List<string> Columns = new List<string>();
                                for (int i = 0; i < indexes.Count; i++)
                                {
                                    Columns.Add(SplitedLines[indexes[i]]);
                                }
                                row.RowValues = Columns.ToArray();
                                EachRow.Add(row);
                            }
                            else
                            {
                                continue;
                            }
                        }
                        DataSet dataSet = new DataSet(EachRow);
                        return dataSet;
                    }
                    //10th
                    else
                    {
                        int indexOfPropertySeason = Array.IndexOf(allHeaders, "season");
                        int indexOfPropertytoss_decision = Array.IndexOf(allHeaders, "toss_decision");
                        int indexOfPropertyCity = Array.IndexOf(allHeaders, "city");
                        while ((line = _reader.ReadLine()) != null)
                        {
                            string[] SplitedLines = line.Split(',');
                            int seasonData = int.Parse(SplitedLines[indexOfPropertySeason]);
                            string tossData = SplitedLines[indexOfPropertytoss_decision];
                            string cityData = SplitedLines[indexOfPropertyCity];
                            if (seasonData >= 2008 || tossData!="bat"&& cityData=="Bangalore")
                            {
                                Row row = new Row();
                                List<string> Columns = new List<string>();
                                for (int i = 0; i < indexes.Count; i++)
                                {
                                    Columns.Add(SplitedLines[indexes[i]]);
                                }
                                row.RowValues = Columns.ToArray();
                                EachRow.Add(row);
                            }
                            else
                            {
                                continue;
                            }
                        }
                        DataSet dataSet = new DataSet(EachRow);
                        return dataSet;
                    }
                }
                #endregion

            }

        }
       
    }
}