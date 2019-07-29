namespace DbEngine.Query.Parser
{
    /*
     This class is used for storing name of field, condition and value for 
     each conditions generate properties for this class, Also override toString method
 */
    public class Restriction {
       public string propertyName;
        public string propertyValue;
        public string condition;

        public Restriction(string propertyName, string propertyValue, string condition)
        {
            this.propertyName = propertyName;
            this.propertyValue = propertyValue;
            this.condition = condition;
        }
    }
}