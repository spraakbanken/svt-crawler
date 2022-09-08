# SVT crawler

Programme for crawling SVT's API for news articles and converting the data to XML.


## How to run

Setup virtual environment and install dependencies from `requirements.txt`.

With activated virtual environment run:

```
python crawler.py
```

Follow the instructions given by the command line interface.

**Note:** Due to caching issues in the SVT API it may happen that not all articles
are downloaded on the first attempt.


## Todo

- [ ] Add "date collected" attribute to json when downloading an article.
