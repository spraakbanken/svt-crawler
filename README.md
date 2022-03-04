# SVT-insamling

Script for searching SVT's API for news articles.

Date for last crawl: 2021-09

Data should be updated once every 6 months.


## TODO

- [x] Split corpus into one corpus per year
- [x] Remove dates from articles before 2004 (since they are probably wrong and mess up the time graph)?
  ~Alternatively save "modified" date for articles that have a "published" date before 2004.~
- [x] text attribute must be called "text" for Korp's sake!
- [x] Remove nested paragraphs somehow!
  Last time nested paragraphs were removed with these loops:
    `for x in *; do sed -i 's:<p><p:<p:g' $x/*.xml; done`
    `for x in *; do sed -i 's:</p></p>:</p>:g' $x/*.xml; done`
- [x] Check if removal of empty spans works as it should!
  Last time empty tags were removed manually from xml with this loop:
    `for x in *; do sed -i 's:<p />::g' $x/*.xml; done`

- [x] How do we update without overriding previous data?

- [] Move code to GitHub and let peter.dahlgren@jmg.gu.se know about the repository
- [] When crawling: don't write crawled_pages.json all the time!


Check nested:

    grep '</p></p>' svt-*/source/*/*.xml

Check empty:

    grep '<p />' svt-*/source/*/*.xml
    grep '<p/>' svt-*/source/*/*.xml


## How to run

Setup virtual environment and install dependencies from `requirements.txt`.
Modify the `if __name__ == "__main__":` section to configure what the script should do.

With activated virtual environment run:

```
python crawler.py
```
