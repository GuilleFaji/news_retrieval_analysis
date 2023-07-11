# news_retrieval_analysis
Scrapping news database (gdelt) and processing+filtering the news to get relevant information that will be fed to LLMs

Test code in src/0_tests.ipynb

Main functions in src/downloader.py

Practical example in Example.ipynb

If you import from src/downloader.py, you can directly use the function 'pipeline_total' to get a DF with your query.

The pipeline function takes several arguments:

query: string name of the company to search. Ex 'Meta Platforms'

 positives: list of tuples. Each tuple has EQUIVALENT words. 
 Ex [('COVID19', 'COVID-19'), ('Environmental', 'ESG-2022')] where COVID19 and COVID-19 are accepted with an 'OR' statement
 
negatives: list of strings. Each string is a term you don't want to see in the articles found.

start_date: of your query in an AAAAMMDDHHMMSS format.

end_date: of your query in an AAAAMMDDHHMMSS format.

This code was harder to make than it might seem. It's far from perfect but it works.

There is a harder, wider version of the code for bulk search to thousands of companies through the years. Feel free to ask.

EACH SEARCH WILL ONLY RETURN 250 RESULTS AT MOST, SO TAKE THAT INTO ACCOUNT WHEN SETTING START AND END DATES.
