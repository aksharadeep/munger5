using System;
using System.IO;
using DbEngine.Query;
using DbEngine.Reader;
using Xunit;
using FluentAssertions;
using System.Linq;
namespace test
{
    public class DataMungerTest
    {
        #region Don't Try to Delete/Change the Logic Keep it as it is
        readonly string strtogetipldotcsv = "";
        readonly string rootdir = "";
        #endregion
        private static CsvQueryProcessor _reader;
        public DataMungerTest()
        {
            #region Don't Try to Delete/Change the Logic Keep it as it is in constuctor
            var mydirectory = Environment.CurrentDirectory;
            int indexofmyvar = mydirectory.IndexOf("bin");
            string substr = mydirectory.Substring(0, indexofmyvar);
            if (substr.Contains("test"))
            {
                int commanderindex = substr.IndexOf("test");
                rootdir = substr.Substring(0, commanderindex);
                strtogetipldotcsv = rootdir + "data/ipl.csv";
            }
            _reader = new CsvQueryProcessor(strtogetipldotcsv);
            #endregion
        }
        [Fact]
        public void TestRetrieveHeader()
        {
            Header result = _reader.GetHeader();
            string[] expectedHeaders = new string[] {
              "id", "season", "city", "date", "team1", "team2", "toss_winner", "toss_decision",
                          "result", "dl_applied", "winner", "win_by_runs", "win_by_wickets", "player_of_match", "venue",
                          "umpire1", "umpire2", "umpire3"
            };
            result.Headers.Should().NotBeNull();
            result.Headers.Should().BeEquivalentTo(expectedHeaders);
        }
        [Fact]
        public void TestRetrieveDataTypes()
        {
            DataTypeDefinitions result = _reader.GetColumnType();
            string[] expectedDataTypes = new string[] {
                "System.Int32", "System.Int32", "System.String", "System.DateTime",
                        "System.String", "System.String", "System.String", "System.String",
                        "System.String", "System.Int32", "System.String", "System.Int32",
                        "System.Int32", "System.String", "System.String", "System.String",
                        "System.String", "System.Object"
            };
            result.DataTypes.Should().BeEquivalentTo(expectedDataTypes);
        }
        [Fact]
        public void TestFileNotFound()
        {
            try
            {
                CsvQueryProcessor reader = new CsvQueryProcessor(strtogetipldotcsv);
            }
            catch (Exception exception)
            {
                exception.GetType().ToString().Should().Be("System.IO.FileNotFoundException");
            }
        }
        [Fact]
        public void TestSelectAllWithoutWhereClause()
        {
            int totalrecordsexpected = 577;
            int recordscounter = 1;
            bool dataexpectedstatus = false;
            DataSet dataSet = null;
            try
            {
                CsvQueryProcessor reader = new CsvQueryProcessor(strtogetipldotcsv);
                dataSet = new Query().executeQuery(@"select * from "+strtogetipldotcsv);
                int counter = 0;
                foreach (Row row in dataSet.Rows)
                {
                    if (recordscounter == 1)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "1", "2008", "Bangalore", "2008-04-18", "Kolkata Knight Riders", "Royal Challengers Bangalore", "Royal Challengers Bangalore", "field", "normal", "0", "Kolkata Knight Riders", "140", "0", "BB McCullum", "M Chinnaswamy Stadium", "Asad Rauf", "RE Koertzen", null }))

                        {
                            counter++;
                        }
                    }
                    else if (recordscounter == 289)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "289", "2012", "Cuttack", "2012-05-01", "Deccan Chargers", "Pune Warriors", "Deccan Chargers", "bat", "normal", "0", "Deccan Chargers", "13", "0", "KC Sangakkara", "Barabati Stadium", "Aleem Dar", "AK Chaudhary", null }))
                        {
                            counter++;
                        }
                    }
                    else if (recordscounter == 577)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "577", "2016", "Bangalore", "2016-05-29", "Sunrisers Hyderabad", "Royal Challengers Bangalore", "Sunrisers Hyderabad", "bat", "normal", "0", "Sunrisers Hyderabad", "8", "0", "BCJ Cutting", "M Chinnaswamy Stadium", "HDPK Dharmasena", "BNJ Oxenford", null }))
                        {
                            counter++;
                        }
                    }

                    recordscounter++;
                }
                if (counter > 2)
                {
                    dataexpectedstatus = true;
                }

            }
            catch (Exception exception)
            {
                exception.GetType().ToString().Should().Be("System.IO.FileNotFoundException");
            }
            totalrecordsexpected.Should().Be(577);
            dataSet.Should().NotBeNull("because records exist in ipl.csv file");
            dataexpectedstatus.Should().BeTrue("because no. of matching records should be more than 2");
        }
        [Fact]
        public void TestSelectColumnsWithoutWhereClause()
        {
            int totalrecordsexpected = 577;
            int recordscounter = 1;
            bool dataexpectedstatus = false;
            DataSet dataSet = null;
            try
            {
                CsvQueryProcessor reader = new CsvQueryProcessor(strtogetipldotcsv);

                dataSet = new Query().executeQuery(@"select city, winner, team1, team2 from "+strtogetipldotcsv);

                int counter = 0;

                foreach (Row row in dataSet.Rows)
                {
                    if (recordscounter == 1)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "Bangalore", "Kolkata Knight Riders", "Kolkata Knight Riders", "Royal Challengers Bangalore" }))
                        {
                            counter++;
                        }
                    }
                    else if (recordscounter == 289)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "Cuttack", "Deccan Chargers", "Deccan Chargers", "Pune Warriors" }))
                        {
                            counter++;
                        }
                    }
                    else if (recordscounter == 577)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "Bangalore", "Sunrisers Hyderabad", "Sunrisers Hyderabad", "Royal Challengers Bangalore" }))
                        {
                            counter++;
                        }
                    }

                    recordscounter++;
                }
                if (counter > 2)
                {
                    dataexpectedstatus = true;
                }

            }
            catch (Exception exception)
            {
                exception.GetType().ToString().Should().Be("System.IO.FileNotFoundException");
            }

            totalrecordsexpected.Should().Be(577);
            dataSet.Should().NotBeNull("because records exist in ipl.csv file");
            dataexpectedstatus.Should().BeTrue("because no. of matching records should be more than 2");
        }
        [Fact]
        public void TestWithWhereGreaterThanClause()
        {
            int totalrecordsexpected = 60;
            int recordscounter = 1;
            bool dataexpectedstatus = false;

            DataSet dataSet = null;
            try
            {
                CsvQueryProcessor reader = new CsvQueryProcessor(strtogetipldotcsv);

                dataSet = new Query().executeQuery(@"select season,city,winner,team1,team2,player_of_match from "+strtogetipldotcsv+" where season > 2015");

                int counter = 0;

                foreach (Row row in dataSet.Rows)
                {
                    if (recordscounter == 1)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "2016", "Mumbai", "Rising Pune Supergiants", "Mumbai Indians", "Rising Pune Supergiants", "AM Rahane" }))
                        {
                            counter++;
                        }
                    }
                    else if (recordscounter == 30)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "2016", "Bangalore", "Kolkata Knight Riders", "Royal Challengers Bangalore", "Kolkata Knight Riders", "AD Russell" }))
                        {
                            counter++;
                        }
                    }
                    else if (recordscounter == 60)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "2016", "Bangalore", "Sunrisers Hyderabad", "Sunrisers Hyderabad", "Royal Challengers Bangalore", "BCJ Cutting" }))
                        {
                            counter++;
                        }
                    }

                    recordscounter++;
                }
                if (counter > 2)
                {
                    dataexpectedstatus = true;
                }

            }
            catch (Exception exception)
            {
                exception.GetType().ToString().Should().Be("System.IO.FileNotFoundException");
            }

            totalrecordsexpected.Should().Be(60);
            dataSet.Should().NotBeNull("because records exist in ipl.csv file");
            dataexpectedstatus.Should().BeTrue("because no. of matching records should be more than 2");
        }
        [Fact]
        public void TestWithWhereLessThanClause()
        {
            int totalrecordsexpected = 458;
            int recordscounter = 1;
            bool dataexpectedstatus = false;

            DataSet dataSet = null;
            try
            {
                CsvQueryProcessor reader = new CsvQueryProcessor(strtogetipldotcsv);

                dataSet = new Query().executeQuery(@"select city,winner,team1,team2,player_of_match from "+strtogetipldotcsv+" where season < 2015");

                int counter = 0;

                foreach (Row row in dataSet.Rows)
                {
                    if (recordscounter == 1)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "Bangalore", "Kolkata Knight Riders", "Kolkata Knight Riders", "Royal Challengers Bangalore", "BB McCullum" }))
                        {
                            counter++;
                        }
                    }
                    else if (recordscounter == 229)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "Jaipur", "Royal Challengers Bangalore", "Rajasthan Royals", "Royal Challengers Bangalore", "S Aravind" }))
                        {
                            counter++;
                        }
                    }
                    else if (recordscounter == 458)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "Bangalore", "Kolkata Knight Riders", "Kings XI Punjab", "Kolkata Knight Riders", "MK Pandey" }))
                        {
                            counter++;
                        }
                    }

                    recordscounter++;
                }
                if (counter > 2)
                {
                    dataexpectedstatus = true;
                }

            }
            catch (Exception exception)
            {
                exception.GetType().ToString().Should().Be("System.IO.FileNotFoundException");
            }

            totalrecordsexpected.Should().Be(458);
            dataSet.Should().NotBeNull("because records exist in ipl.csv file");
            dataexpectedstatus.Should().BeTrue("because no. of matching records should be more than 2");
        }
        [Fact]
        public void TestWithWhereLessThanOrEqualToClause()
        {
            int totalrecordsexpected = 517;
            int recordscounter = 1;
            bool dataexpectedstatus = false;

            DataSet dataSet = null;
            try
            {
                CsvQueryProcessor reader = new CsvQueryProcessor(strtogetipldotcsv);

                dataSet = new Query().executeQuery(@"select season,city,winner,team1,team2,player_of_match from "+strtogetipldotcsv+" where season <= 2015");

                int counter = 0;

                foreach (Row row in dataSet.Rows)
                {
                    if (recordscounter == 1)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "2008","Bangalore", "Kolkata Knight Riders", "Kolkata Knight Riders", "Royal Challengers Bangalore", "BB McCullum" }))
                        {
                            counter++;
                        }
                    }
                    else if (recordscounter == 258)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "2012","Bangalore", "Kolkata Knight Riders", "Kolkata Knight Riders", "Royal Challengers Bangalore", "L Balaji" }))
                        {
                            counter++;
                        }
                    }
                    else if (recordscounter == 517)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "2015","Kolkata", "Mumbai Indians", "Mumbai Indians", "Chennai Super Kings", "RG Sharma" }))
                        {
                            counter++;
                        }
                    }

                    recordscounter++;
                }
                if (counter > 2)
                {
                    dataexpectedstatus = true;
                }

            }
            catch (Exception exception)
            {
                exception.GetType().ToString().Should().Be("System.IO.FileNotFoundException");
            }

            totalrecordsexpected.Should().Be(517);
            dataSet.Should().NotBeNull("because records exist in ipl.csv file");
            dataexpectedstatus.Should().BeTrue("because no. of matching records should be more than 2");
        }
        [Fact]
        public void TestWithWhereEqualAndNotEqualClause()
        {
            int totalrecordsexpected = 195;
            int recordscounter = 1;
            bool dataexpectedstatus = false;

            DataSet dataSet = null;
            try
            {
                CsvQueryProcessor reader = new CsvQueryProcessor(strtogetipldotcsv);

                dataSet = new Query().executeQuery(@"select season,city,winner,team1,team2,player_of_match from "+strtogetipldotcsv+" where season >= 2013 and season <= 2015");

                int counter = 0;

                foreach (Row row in dataSet.Rows)
                {
                    if (recordscounter == 1)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "2013", "Kolkata", "Kolkata Knight Riders", "Delhi Daredevils", "Kolkata Knight Riders", "SP Narine" }))
                        {
                            counter++;
                        }
                    }
                    else if (recordscounter == 97)
                    {
                    if (row.RowValues.SequenceEqual(new string[] { "2014", "Ranchi", "Chennai Super Kings", "Chennai Super Kings", "Kolkata Knight Riders",   "RA Jadeja" }))
                        {
                            counter++;
                        }
                    }
                    else if (recordscounter == 195)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "2015", "Kolkata", "Mumbai Indians", "Mumbai Indians", "Chennai Super Kings", "RG Sharma" }))
                        {
                            counter++;
                        }
                    }

                    recordscounter++;
                }
                if (counter > 2)
                {
                    dataexpectedstatus = true;
                }

            }
            catch (Exception exception)
            {
                exception.GetType().ToString().Should().Be("System.IO.FileNotFoundException");
            }

            totalrecordsexpected.Should().Be(195);
            dataSet.Should().NotBeNull("because records exist in ipl.csv file");
            dataexpectedstatus.Should().BeTrue("because no. of matching records should be more than 2");
        }
        [Fact]
        public void TestWithWhereThreeConditionsEqualOrNotEqualClause()
        {
            int totalrecordsexpected = 577;
            int recordscounter = 1;
            bool dataexpectedstatus = false;

            DataSet dataSet = null;
            try
            {
                CsvQueryProcessor reader = new CsvQueryProcessor(strtogetipldotcsv);

                dataSet = new Query().executeQuery(@"select city,winner,team1,team2,player_of_match from "+strtogetipldotcsv+" where season >= 2008 or toss_decision != bat and city = bangalore");

                int counter = 0;

                foreach (Row row in dataSet.Rows)
                {
                    if (recordscounter == 1)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "Bangalore", "Kolkata Knight Riders", "Kolkata Knight Riders", "Royal Challengers Bangalore", "BB McCullum" }))
                        {
                            counter++;
                        }
                    }
                    else if (recordscounter == 289)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "Cuttack", "Deccan Chargers", "Deccan Chargers", "Pune Warriors", "KC Sangakkara" }))
                        {
                            counter++;
                        }
                    }
                    else if (recordscounter == 577)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "Bangalore", "Sunrisers Hyderabad", "Sunrisers Hyderabad", "Royal Challengers Bangalore", "BCJ Cutting" }))
                        {
                            counter++;
                        }
                    }

                    recordscounter++;
                }
                if (counter > 2)
                {
                    dataexpectedstatus = true;
                }

            }
            catch (Exception exception)
            {
                exception.GetType().ToString().Should().Be("System.IO.FileNotFoundException");
            }
            totalrecordsexpected.Should().Be(577);
            dataSet.Should().NotBeNull("because records exist in ipl.csv file");
            dataexpectedstatus.Should().BeTrue("because no. of matching records should be more than 2");
        }
    }
}
