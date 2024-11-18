# youtube-network-analyzer
# Moses Branch --- Where Team Member Moises work resides

- Completed the 'Data_Cleaner.py' script that cleans the dataset file (.csv only)
    * Removes all broken links from the files
    * Provides statistics currently on Number of: videos without uploaders, videos with length 0, videos with no links
    * Algorithm can be updated to remove any other additional data that is identified as "unfit" for the team's application

How does the algorithm work?
    When running the script, provide the .csv file you want to clean. Hit 'Enter' on your keyboard, and the algorithm should take care of the rest.
    *Important Note: The .csv file must be in the same directory as the python script for the script to work.
    
- Began development of the SearchEngine algorithm for the project.
    * The file is called "Neo4jSearchEngine.py"
        * Must have py2neo installed for it to operate properly.
    * Current work
        * Has most match-type methods for search query construction completed
            * Needs additional work with parsing input string into a searchable query (optimization?)
        * Search Engine can search all categories for their top videos

To - Do:
* Continue with the development of the Search Engine, specifically query processing.
    * Breaking down input text into sizeable subexpressions for query processing.
* Incorporate PageRank system into the search engine algorithm


here is a link to the data:
https://netsg.cs.sfu.ca/youtubedata/
