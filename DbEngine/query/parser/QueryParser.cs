using System;
using System.Linq;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace DbEngine.Query.Parser
{
    public class QueryParser
    {
        private QueryParameter queryParameter;
        public QueryParser()
        {
            queryParameter = new QueryParameter();
        }

        /*
     this method will parse the queryString and will return the object of
     QueryParameter class
     */
        public QueryParameter ParseQuery(string queryString)
        {
            queryParameter.BaseQuery = queryString;
            /*
              extract the name of the file from the query. File name can be found after the "from" clause.
             */
            queryParameter.File = GetFileName(queryString);
            /*
             extract the order by fields from the query string. Please note that we will need to extract the field(s) after "order by" clause in the query, if at all the order by clause exists. For eg: select city,winner,team1,team2 from data/ipl.csv order by city from the query mentioned above, we need to extract "city". Please note that we can have more than one order by fields.
             */
            queryParameter.OrderByFields = GetOrderByFields(queryString);
            /*
             extract the group by fields from the query string. Please note that we will need to extract the field(s) after "group by" clause in the query, if at all the group by clause exists. For eg: select city,max(win_by_runs) from data/ipl.csv group by city from the query mentioned above, we need to extract "city". Please note that we can have more than one group by fields.
             */
            queryParameter.GroupByFields = GetGroupByFields(queryString);
            /*
             * extract the selected fields from the query string. Please note that we will need to extract the field(s) after "select" clause followed by a space from the query string. For eg: select city,win_by_runs from data/ipl.csv from the query mentioned above, we need to extract "city" and "win_by_runs". Please note that we might have a field containing name "from_date" or "from_hrs". Hence, consider this while parsing.
             */
            queryParameter.Fields = GetFields(queryString);

            /*
             extract the conditions from the query string(if exists). for each condition, we need to capture the following: 
              1. Name of field 
              2. condition 
              3. value
            For eg: select city,winner,team1,team2,player_of_match from data/ipl.csv where season >= 2008 or toss_decision != bat
             
             here, for the first condition, "season>=2008" we need to capture: 
              1. Name of field: season 
              2. condition: >= 
              3. value: 2008
              
              the query might contain multiple conditions separated by OR/AND operators.
              Please consider this while parsing the conditions.
              
             */
            queryParameter.Restrictions = GetRestrictions(queryString);


            /*
              extract the logical operators(AND/OR) from the query, if at all it is present. For eg: select city,winner,team1,team2, player_of_match from  data/ipl.csv where season >= 2008 or toss_decision != bat and city = bangalore
             
              the query mentioned above in the example should return a List of Strings containing [or,and]
             */
            queryParameter.LogicalOperators = GetLogicalOperators(queryString);


            /*
             extract the aggregate functions from the query. The presence of the aggregate functions can determined if we have either "min" or "max" or "sum" or "count" or "avg" followed by opening braces"(" after "select" clause in the query string. in case it is present, then we will have to extract the same. For
              each aggregate functions, we need to know the following: 
             1. type of aggregate function(min/max/count/sum/avg) 
             2. field on which the aggregate function is being applied
              
              Please note that more than one aggregate function can be present in a query
              
             */
            queryParameter.AggregateFunctions = GetAggregateFunctions(queryString);
            return queryParameter;
        }
        #region GetFileName
        public string GetFileName(string str)
        {
            string expected = null;
            string[] Strings = str.Split(' ');

            for (int i = 0; i < Strings.Length; i++)
            {
                if (Strings[i] == "from")
                {
                    i++;
                    expected = Strings[i];
                    break;
                }
            }


            //expected = Strings[Strings.Length - 1];
            return expected;

        }
        #endregion
        #region GetOrderByFields
        private List<string> GetOrderByFields(string queryString)
        {
            queryString = queryString.ToLower();
            //String[] getOrderBy = null;
            if (queryString.Contains("order by"))
            {
                int orderby = queryString.IndexOf("order by ");
                String order = queryString.Substring(orderby + 9);
                List<string> getOrderBy = new List<string>(order.Split(" "));
                return getOrderBy;
            }
            return null;

        }
        #endregion
        #region GetGroupByFields
        private List<string> GetGroupByFields(string queryString)
        {
            string[] str = new String[1];
            queryString = queryString.ToLower();
            if (queryString.Contains("group by"))
            {

                String[] GetGroupBy = queryString.Split("group by ");
                string getgroupbystring = GetGroupBy[1];
                if (getgroupbystring.Contains("order by"))
                {
                    string[] getgroupbyfields = getgroupbystring.Split(" order by");
                    string groupbyfield = getgroupbyfields[0];
                    str[0] = groupbyfield;
                }
                str[0] = getgroupbystring;
            }
            List<String> mystr = new List<string>(str);
            return mystr;
        }
        #endregion
        #region GetAggregateFunctions
        private List<AggregateFunction> GetAggregateFunctions(string queryString)
        {
            string str = queryString.Substring(queryString.IndexOf("select ") + 7, queryString.IndexOf(" from") - 7);
            if ((str.Contains("avg(")) || (str.Contains("sum(")) || (str.Contains("min(")) || (str.Contains("max(") || (str.Contains("count("))))
            {
                string[] str2 = str.Split(",");
                List<AggregateFunction> AFunc = new List<AggregateFunction>(str2.Length);
                for (int i = 0; i < str2.Length; i++)
                {
                    string[] a1 = str2[i].Substring(0, str2[i].Length - 1).Split("(");
                    AFunc[i] = new AggregateFunction(a1[1], a1[0]);
                }
                return AFunc;
            }
            else
                return null;
        }
        #endregion
        #region GetRestrictions
        private List<Restriction> GetRestrictions(string queryString)
        {


            string querystring = queryString.Trim();
            string[] splitByWhere = querystring.Trim().Split("where");

            if (splitByWhere.Length == 1)
            { return null; }

            String[] conditions = splitByWhere[1].Trim().Split("order by|group by");
            String[] condtionArray = conditions[0].Trim().Split(" and | or ");
            List<Restriction> restrictionList = new List<Restriction>();
            foreach (string eachArray in condtionArray)
            {
                String condition = "";
                if (eachArray.Contains(">="))
                {
                    condition = ">=";
                }
                else if (eachArray.Contains("<="))
                {
                    condition = "<=";
                }
                else if (eachArray.Contains("!="))
                {
                    condition = "!=";
                }
                else if (eachArray.Contains(">"))
                {
                    condition = ">";
                }
                else if (eachArray.Contains("<"))
                {
                    condition = "<";
                }
                else if (eachArray.Contains("="))
                {
                    condition = "=";
                }
                String name = eachArray.Split(condition)[0].Trim();
                String value = eachArray.Split(condition)[1].Trim();
                Restriction restrictionInstance = new Restriction(name, value, condition);
                restrictionList.Add(restrictionInstance);
            }
            return restrictionList;
        }
        #endregion
        #region GetLogicalOperators
        private List<string> GetLogicalOperators(string queryString)
        {
            List<string> LogicalString = null;
            String[] query = queryString.ToLower().Split(" ");
            String getLogical = "";
            if (queryString.Contains("and || or "))
            {
                for (int i = 0; i < query.Length; i++)
                {
                    if (query[i] == "and" || query[i] == "or")
                    {

                        getLogical += query[i] + " ";
                    }
                }
                LogicalString = new List<string>(getLogical.Trim().Split(" "));
            }
            
            return LogicalString;
        }
        #endregion
        #region GetFields 
        private List<String> GetFields(string queryString)
        {
            string strSelect = queryString.Split("select")[1].Trim();
            string strFrom = strSelect.Split("from")[0].Trim();
            string[] selectFields = null;
            List<string> list = new List<string>();
                selectFields = strFrom.Split(",");
                for (int i = 0; i < selectFields.Length; i++)
                {
                    list.Add(selectFields[i].Trim());
                }
                return list;
        }
        #endregion
    }
}