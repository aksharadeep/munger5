using System;

namespace DbEngine.Query
{
    //This class contains methods to evaluate expressions
    public class Filter
    {
        /* 
	 The evaluateExpression() method of this class is responsible for evaluating the expressions mentioned in the query. It has to be noted that the process of evaluating expressions will be different for different data types. there are 6 operators that can exist within a query i.e. >=,<=,<,>,!=,= This method should be able to evaluate all of them. 
     Note: while evaluating string expressions, please handle uppercase and lowercase 
	 */

       public bool evaluateExpression(string condition, int propertyData, int propertyValue)
        {
            bool value=false;
            switch (condition)
            {
                case "<":
                    value = lessThan(propertyData, propertyValue);
                    return value;
                case ">":
                    value =greaterThan(propertyData, propertyValue);
                    return value;
                case "<=":
                    value= lessThanOrEqualTo(propertyData, propertyValue);
                    return value;
                default:
                    break;
            }
            return value;
        }
        //Method containing implementation of lessThan operator
        private bool lessThan(int propertyData, int propertyValue)
        {
            if (propertyData < propertyValue)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        //Method containing implementation of lessThanOrEqualTo operator
        private bool lessThanOrEqualTo(int propertyData, int propertyValue)
        {
            if (propertyData <= propertyValue)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        //Method containing implementation of greaterThan operator
        private bool greaterThan(int propertyData, int propertyValue)
        {
            if (propertyData > propertyValue)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

       



        //Method containing implementation of equalTo operator





        //Method containing implementation of notEqualTo operator















        //Method containing implementation of greaterThanOrEqualTo operator






       





       
    }
}
