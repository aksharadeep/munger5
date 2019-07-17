## Seed code - Boilerplate for step 5 - Database Engine Assignment

### Problem Statement

Until step 4, we are not fetching data from the file.

In this step, we will begin executing the queries - we will read the data from the CSV file based on to the query and filter the fields and records based on "select" and "where" condition. 
We will store the extracted data in a separate JSON file named result.json.

**Please note that in this step, we are not handling aggregate functions, group by and order by.** 

For Example
1. Filter the data based on selected fields.

            Query : select city,winner,team1,team2 from data/ipl.csv;
            Expected Output in JSON file:

                       {
                          "1": {
                            "winner": "Kolkata Knight Riders",
                            "city": "Bangalore",
                            "team1": "Kolkata Knight Riders",
                            "team2": "Royal Challengers Bangalore"
                          },
                          "2": {
                            "winner": "Chennai Super Kings",
                            "city": "Chandigarh",
                            "team1": "Chennai Super Kings",
                            "team2": "Kings XI Punjab"
                          },
                          "3": {
                            "winner": "Delhi Daredevils",
                            "city": "Delhi",
                            "team1": "Rajasthan Royals",
                            "team2": "Delhi Daredevils"
                          },
                          "4": {
                            "winner": "Royal Challengers Bangalore",
                            "city": "Mumbai",
                            "team1": "Mumbai Indians",
                            "team2": "Royal Challengers Bangalore"
                          },
                          "5": {
                            "winner": "Kolkata Knight Riders",
                            "city": "Kolkata",
                            "team1": "Deccan Chargers",
                            "team2": "Kolkata Knight Riders"
                          }, ......
                             ......
                        }


2. Filter the data based on the query with multiple where conditions.
                  
            Query : select city, win_by_runs, season from data/ipl.csv where season > 2014 and city ='Bangalore'
            Expected output in JSON File:
                    {
                      "1": {
                        "city": "Bangalore",
                        "season": "2015",
                        "win_by_runs": "0"
                      },
                      "2": {
                        "city": "Bangalore",
                        "season": "2015",
                        "win_by_runs": "18"
                      },
                      "3": {
                        "city": "Bangalore",
                        "season": "2015",
                        "win_by_runs": "27"
                      },
                      "4": {
                        "city": "Bangalore",
                        "season": "2015",
                        "win_by_runs": "0"
                      },
                      "5": {
                        "city": "Bangalore",
                        "season": "2015",
                        "win_by_runs": "0"
                      }, .....
                         .....
                    }


`Note : Once you have cloned boilerplate from the given gitlab URL, import the project into eclipse. 
Your project’s test cases might show compile time errors for methods, as you have not written the complete code.`

### Expected solution

A JSON file containing the filtered result set.

### Following are the broad tasks:

- Read the query from the user
- parse the query
- forward the object of query parameter to CsvQueryProcessor
- filter out rows basis on the conditions mentioned in the where clause
- write the result set into a JSON file

### Project structure

The folders and files you see in this repositories, is how it is expected to be in projects, which are submitted for automated evaluation by Hobbes

	Project
	|
	├── data 			                    // If project needs any data file, it can be found here/placed here, if data is huge 
	|                                            they can be mounted, no need put it in your repository
	|
	├── DbEngine	            // all your java file will be stored in this package
    |    └── query
	|	 |     └── DataSet.cs 		              // This class will be acting as the DataSet containing multiple rows
	|	 |     └── DataTypeDefinitions.cs      // This class contains methods to find the column data types
	|	 |     └── Filter.cs                   // This class contains methods to evaluate expressions
	|	 |     └── Header.cs                 // This class contains the Headers property to return a String array for containing headers.
	|	 |     └── Query.cs                  // This class selects the appropriate processor based on the type of query
	|    | 	   └── Row.cs                    // This class contains the row object as string array 
	|    |     └── parser
	|	 |         └── AggregateFunction.cs                  // This class is used to store Aggregate Function
	|    |    	   └── QueryParameter.cs                     // This class contains the parameters 
	|	 |         └── QueryParser.cs        // This class will parse the queryString and return an object of QueryParameter class
	|	 |         └── Restriction.cs        // This class is for storing Restriction object
	|    └──reader
	|	 |   └── CsvQueryProcessor.cs                  // This class is used to read data from CSV file
	|	 |   └── QueryProcessingEngine.cs              // This is an interface with GetDataRow() in it.
	|    └── writer
	|		      └── JsonWriter.cs                         // This class writes the result in a JSON file
    |    └── Program.cs                   //This contains main function to execute your code
	├──test                                     
    |   └── DataMungerTest.cs                // all your test cases are written using XUnit 
    |
	├── .hobbes   			                    // Hobbes specific config options, such as type of evaluation schema,
	|                                            type of tech stack etc., Have saved a default values for convenience
	├── .sln                                    // This is automatically generated by Visual Studio.
    |
    └── PROBLEM.md

> PS: All lint rule files are by default copied during the evaluation process, however if need to be customizing, you should copy from this repo and modify in your project repo


#### To use this as a boilerplate for your new project, you can follow these steps

1. Clone the base boilerplate in the folder **assignment-solution-step5** of your local machine
     
    `git clone https://gitlab-cgi.stackroute.in/database_engine_dotnet/DataMungerStep5_Boilerplate.git`

2. Navigate to assignment-solution-step5 folder

    `cd assignment-solution-step5`

3. Remove its remote or original reference

     `git remote rm origin`

4. Create a new repo in gitlab named `assignment-solution-step5` as private repo

5. Add your new repository reference as remote

     `git remote add origin https://gitlab-cgi.stackroute.in/{{yourusername}}/assignment-solution-step5.git`

     **Note: {{yourusername}} should be replaced by your username from gitlab**

5. Check the status of your repo 
     
     `git status`

6. Use the following command to update the index using the current content found in the working tree, to prepare the content staged for the next commit.

     `git add .`
 
7. Commit and Push the project to git

     `git commit -a -m "Initial commit | or place your comments according to your need"`

     `git push -u origin master`

8. Check on the git repo online, if the files have been pushed

### Important instructions for Participants
> - We expect you to write the assignment on your own by following through the guidelines, learning plan, and the practice exercises
> - The code must not be plagiarized, the mentors will randomly pick the submissions and may ask you to explain the solution
> - The code must be properly indented, code structure maintained as per the boilerplate and properly commented
> - Follow through the problem statement shared with you