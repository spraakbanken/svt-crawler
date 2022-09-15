#!/usr/bin/env python3

"""Crawler for SVT news."""

import argparse
import json
import math
import re
import sys
import time
import traceback
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import requests
from lxml import etree

DATADIR = Path("data")
CRAWLED = DATADIR / Path("crawled_pages.json")
FAILED = DATADIR / Path("failed_urls.json")
PROCESSED_JSON = DATADIR / Path("processed_json.json")
MAX_SEEN_ARTICLES = 50  # stop crawling when encountering this many articles that have been downloaded already


#-------------------------------------------------------------------------------
# Define the command line args
#-------------------------------------------------------------------------------
class CustomHelpFormatter(argparse.RawDescriptionHelpFormatter):
    """Custom help formatter for argparse, silencing subparser lists."""
    def _format_action(self, action):
        result = super()._format_action(action)
        if isinstance(action, argparse._SubParsersAction):
            return ""
        return result


def valid_date(s):
    """Make sure that s is a valid date in the correct format."""
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = f"Not a valid date of format YY-MM-DD: {s}"
        raise argparse.ArgumentTypeError(msg)


def valid_year(s):
    """Make sure that s is a valid year."""
    try:
        return datetime.strptime(s, "%Y")
    except ValueError:
        msg = f"Not a valid year: {s}"
        raise argparse.ArgumentTypeError(msg)

parser = argparse.ArgumentParser(description=
                                "Programme for crawling svt.se for news articles and converting the data to XML.",
                                 formatter_class=CustomHelpFormatter)

description = [
    "crawl            Crawl svt.se and download news articles",
    "summary          Print summary of collected data",
    "xml              Convert articles from JSON to XML",
    "build-index      Compile an index of the crawled data based on the downloaded files",
]
subparsers = parser.add_subparsers(dest="command", title="commands", metavar="<command>",
                                   description="\n".join(description))
subparsers.required = True

crawl_parser = subparsers.add_parser("crawl", description="Crawl svt.se and download news articles")
crawl_parser.add_argument("-r", "--retry", action="store_true", help="try to crawl pages that have failed previously")
crawl_parser.add_argument("-f", "--force", action="store_true", help="crawl all pages even if they have been crawled before")
crawl_parser.add_argument("-d", "--debug", action="store_true", help="print some debug info while crawling")
crawl_parser.add_argument("-s", "--stop", type=valid_date, default=None,
                          help="stop crawling when reaching articles published before this date (format 'YYYY-MM-DD'); "
                               f"otherwise crawling will stop when reaching {MAX_SEEN_ARTICLES} consecutive articles "
                               "that have already been downloaded")

summary_parser = subparsers.add_parser("summary", description="Print summary of collected data")

xml_parser = subparsers.add_parser("xml", description="Convert articles from JSON to XML")
xml_parser.add_argument("-y", "--year", type=valid_year, default=None, help="preprocess only articles published in a certain year")
xml_parser.add_argument("-o", "--override", action="store_true", help="override existing xml files")
xml_parser.add_argument("-d", "--debug", action="store_true", help="print some debug info while converting")

index_parser = subparsers.add_parser("build-index",
                                     description="Compile an index of the crawled data based on the downloaded files")
index_parser.add_argument("--out", default="crawled_pages_from_files.json", type=str,
                          help=f"name of the output file (will be stored in '{DATADIR}')")

#-------------------------------------------------------------------------------
# Parser for article listings
#-------------------------------------------------------------------------------

class SvtParser():
    """Parser for 'nyheter' article listing pages."""

    API_URL = "https://api.svt.se/nss-api/page/"
    ARTICLE_URL = "https://api.svt.se/nss-api/page{}?q=articles"
    LIMIT = 50
    TOPICS = [
        "nyheter/ekonomi",
        "nyheter/granskning",
        "nyheter/inrikes",
        "nyheter/svtforum",
        "nyheter/nyhetstecken",
        "nyheter/vetenskap",
        "nyheter/konsument",
        "nyheter/utrikes",
        "sport",
        "vader",
        "kultur",
    ]
    LOCAL = [
        "blekinge",
        "dalarna",
        "gavleborg",
        "halland",
        "helsingborg",
        "jamtland",
        "jonkoping",
        "norrbotten",
        "skane",
        "smaland",
        "stockholm",
        "sodertalje",
        "sormland",
        "uppsala",
        "varmland",
        "vast",
        "vasterbotten",
        "vasternorrland",
        "vastmanland",
        "orebro",
        "ost",
    ]

    TOPICS.extend(["nyheter/lokalt/" + area for area in LOCAL])

    def __init__(self, debug=False):
        self.get_crawled_data()
        self.debug = debug

    def get_crawled_data(self):
        """Get list of crawled URLs from CRAWLED file."""
        self.crawled_data = dict()
        self.saved_urls = set()
        if CRAWLED.is_file():
            with open(CRAWLED) as f:
                self.crawled_data = json.load(f)
                self.saved_urls = set(self.crawled_data.keys())

        # Keep track of articles that could not be downloaded
        self.failed_urls = []
        if FAILED.is_file():
            with open(FAILED) as f:
                self.failed_urls = json.load(f)

    def crawl(self, force=False, stopdate=None):
        """Get all article URLs from a certain topic from the SVT API."""
        self.stopdate = stopdate
        self.query_params = {"q": "auto", "limit": self.LIMIT}
        for topic in self.TOPICS:
            topic_name = topic
            self.query_params["page"] = 1
            self.seen_articles_counter = 0
            self.new_articles = 0
            if "/" in topic:
                topic_name = topic.split("/")[-1]
            topic_url = self.API_URL + topic + "/"
            encoded_params = ",".join(f"{k}={v}" for k, v in self.query_params.items())
            request = requests.get(topic_url, params=encoded_params)
            firstpage = request.json()
            items = firstpage.get("auto", {}).get("pagination", {}).get("totalAvailableItems", 0)
            pages = int(math.ceil(int(items) / self.LIMIT))
            print(f"\nCrawling {topic}: {items} items, {pages} pages")
            if self.debug:
                print(f"  >> {request.url}")
            self.get_urls(topic_name, topic_url, pages, firstpage, request, force)
            print(f"  New articles downloaded for '{topic_name}': {self.new_articles}")

        print(f"\nDone crawling! Failed to process {len(self.failed_urls)} URLs")

    def get_urls(self, topic_name, topic_url, pages, firstpage, request, force=False):
        """Get article URLs from every page."""
        self.prev_crawled = len(self.saved_urls)
        for i in range(self.query_params["page"], pages + 1):

            pagecontent = []
            try:
                if i == self.query_params["page"]:
                    pagecontent = firstpage.get("auto", {}).get("content", {})
                else:
                    self.query_params["page"] = i
                    encoded_params = ",".join(f"{k}={v}" for k, v in self.query_params.items())
                    request = requests.get(topic_url, params=encoded_params)
                    if self.debug:
                        print(f"  >> {request.url}")
                    pagecontent = request.json().get("auto", {}).get("content", {})
                    if request.url in self.failed_urls:
                        self.remove_from_failed(request.url)
            except Exception:
                tb = traceback.format_exc().replace("\n", "\n  ")
                if self.debug:
                    print(f"  Error when parsing listing '{request.url}'\n  {tb}")
                self.add_to_failed(request.url)

            for c in pagecontent:
                short_url = c.get("url", "")
                if short_url.startswith("https://www.svt.se"):
                    short_url = short_url[18:]
                if short_url:
                    article_date = c.get("published", None)
                    if self.stopdate and article_date:
                        parsed_article_date = datetime.strptime(article_date[:10], "%Y-%m-%d")
                        if parsed_article_date < self.stopdate:
                            print(f"  Encountered an article with publishing date {article_date[:10]}. Skipping remaining.")
                            self.save_results()
                            return

                    if not force and short_url in self.saved_urls:
                        self.seen_articles_counter += 1
                        if self.debug:
                            print(f"  Article already saved. article_date[:10] {short_url} Page: {request.url}")
                        # Stop crawling pages when encountered MAX_SEEN_ARTICLES consecutive articles that have already
                        # been processed (This should work because pages are sorted by publication date, but sometimes
                        # SVT seems to reuse URLs, so that's why we check multiple articles in a row.)
                        if not self.stopdate and self.seen_articles_counter >= MAX_SEEN_ARTICLES:
                            print(f"  Encountered {MAX_SEEN_ARTICLES} seen articles. Skipping remaining.")
                            self.save_results()
                            return
                    else:
                        self.seen_articles_counter = 0

                    # Save article
                    succeeded = self.get_article(short_url, topic_name, article_date[:10], force)
                    if succeeded:
                        self.remove_from_failed(short_url)
                    else:
                        self.add_to_failed(short_url)

            self.save_results()

    def get_article(self, short_url, topic_name, article_date, force=False):
        """Get the content from the article URL and save as json."""
        # Check if article has been downloaded already
        if short_url.startswith("https://www.svt.se"):
            short_url = short_url[18:]
        if short_url in self.saved_urls and not force:
            return True

        article_url = self.ARTICLE_URL.format(short_url)
        if self.debug:
            print(f"  New article: {article_date} {article_url}")
        try:
            article_json = requests.get(article_url).json().get("articles", {}).get("content", [])

            if len(article_json) == 0:
                if self.debug:
                    print(f"  No data found in article '{article_url}'")
                return False

            if len(article_json) > 1:
                print(f"  Found article with multiple content entries: {short_url}")

            article_id = str(article_json[0].get("id"))

            year = 0
            if article_json[0].get("published"):
                year = int(article_json[0].get("published")[:4])
            elif article_json[0].get("modified"):
                year = int(article_json[0].get("modified")[:4])

            # If year is out of range, put article in nodate folder
            this_year = int(datetime.today().strftime("%Y"))
            if (year < 2004) or (year > this_year):
                year = "nodate"

            filepath = DATADIR / Path("svt-" + str(year)) / topic_name / Path(article_id + ".json")
            write_json(article_json, filepath)

            self.crawled_data[short_url] = [article_id, str(year), topic_name]
            self.saved_urls.add(short_url)
            self.new_articles += 1
            return True

        except Exception:
            tb = traceback.format_exc().replace("\n", "\n  ")
            if self.debug:
                print(f"  Error when parsing article '{article_url}'\n  {tb}")
            return False

    def add_to_failed(self, url):
        """Add URL to list of failed URLs."""
        if url not in self.failed_urls:
            self.failed_urls.append(url)

    def remove_from_failed(self, url):
        """Remove from failed URLs if present."""
        if url in self.failed_urls:
            self.failed_urls.remove(url)

    def save_results(self):
        """Save results of sucessfully and unsuccessfully crawled URLs."""
        if self.failed_urls:
            write_json(self.failed_urls, FAILED)
        if len(self.saved_urls) > self.prev_crawled:
            write_json(self.crawled_data, CRAWLED)
            self.prev_crawled = len(self.saved_urls)

    def get_articles_summary(self):
        """Print number of articles per topic."""
        summary = defaultdict(int)
        local = defaultdict(int)
        per_year = defaultdict(int)
        translations = {
            "blekinge": "Blekinge",
            "dalarna": "Dalarna",
            "gavleborg": "Gävleborg",
            "granskning": "uppdrag granskning",
            "halland": "Halland",
            "helsingborg": "Helsingborg",
            "jamtland": "Jämtland",
            "jonkoping": "Jönköping",
            "norrbotten": "Norrbotten",
            "nyhetstecken": "nyheter teckenspråk",
            "orebro": "Örebro",
            "ost": "Öst",
            "skane": "Skåne",
            "smaland": "Småland",
            "sodertalje": "Södertälje",
            "sormland": "Sörmland",
            "stockholm": "Stockholm",
            "uppsala": "Uppsala",
            "vader": "väder",
            "varmland": "Värmland",
            "vast": "Väst",
            "vasterbotten": "Västerbotten",
            "vasternorrland": "Västernorrland",
            "vastmanland": "Västmanland",
        }
        if not self.crawled_data:
            print("No crawled data available!")
            return

        for _article_id, year, topic in self.crawled_data.values():
            if topic in self.LOCAL:
                local[translations.get(topic, topic)] += 1
            else:
                summary[translations.get(topic, topic)] += 1
            per_year[year] += 1

        # Count number of articles per topic
        print("SVT nyheter")
        total = 0
        for topic, amount in sorted(summary.items(), key=lambda x: x[1], reverse=True):
            total += amount
            print(f"{topic}\t{amount}")
        print(f"SVT nyheter totalt\t{total}")
        print()

        # Count local news separately
        print("SVT lokalnyheter")
        local_total = 0
        for area, amount in sorted(list(local.items()), key=lambda x: x[1], reverse=True):
            local_total += amount
            total += amount
            print(f"{area}\t{amount}")
        print(f"Lokalnyheter totalt\t{local_total}")
        print()

        # Articles per year
        print("SVT artiklar per år")
        for year, n in sorted(per_year.items()):
            print(f"{year}\t{n}")
        print()

        # Total of all news items
        print(f"Alla nyhetsartiklar\t{total}")

    def retry_failed(self):
        """Retry crawling/downloading failed URLs."""
        if not self.failed_urls:
            print("Can't find any URLs that failed previously")
            return

        success = set()
        new_failed = set()

        for url in self.failed_urls:
            short_url = url
            if short_url.startswith("https://api.svt.se/nss-api/page"):
                short_url = url[31:]
            if short_url.startswith("/nyheter/lokalt"):
                topic_name = short_url.split("/")[3]
            elif short_url.startswith("/nyheter"):
                topic_name = short_url.split("/")[2]
            else:
                topic_name = short_url.split("/")[1]

            # Process article listing
            if url.startswith("https://api.svt.se/nss-api/page"):
                try:
                    request = requests.get(url)
                    pagecontent = request.json().get("auto", {}).get("content", {})
                    for c in pagecontent:
                        short_url = c.get("url", "")
                        if short_url:
                            if self.get_article(short_url, topic_name):
                                success.add(url)
                            else:
                                new_failed.add(url)
                    success.add(url)
                except Exception:
                    tb = traceback.format_exc().replace("\n", "\n  ")
                    if self.debug:
                        print(f"  Error when parsing listing '{request.url}'\n  {tb}")
                    new_failed.add(url)

            # Process article
            else:
                if self.get_article(url, topic_name):
                    success.add(url)
                else:
                    new_failed.add(url)

        # Update fail file
        for i in success:
            self.remove_from_failed(i)
        for i in new_failed:
            self.add_to_failed(i)
        write_json(self.failed_urls, FAILED)

        # Update file with crawled data
        write_json(self.crawled_data, CRAWLED)


#-------------------------------------------------------------------------------
# Process JSON data
#-------------------------------------------------------------------------------

def process_articles(year=None, override_existing=False, debug=False):
    """Convert json data to Sparv-friendly XML."""
    def write_contents(contents, contents_dir, filecounter):
        contents += "</articles>"
        filepath = contents_dir / (str(filecounter) + ".xml")
        print(f"  Writing file {filepath}")
        write_data(contents, filepath)

    # Get previously processed data
    processed_json = {}
    if PROCESSED_JSON.is_file():
        with open(PROCESSED_JSON) as f:
            processed_json = json.load(f)

    if year:
        year = year.strftime("%Y")
    processed_now = 0

    # Loop through json files and convert them to XML
    for topicpath in sorted(DATADIR.glob(f"svt-{year or '*'}/*")):
        processed = 0
        print(f"Processing '{topicpath}'")
        yeardir = topicpath.parts[1]
        make_corpus_config(yeardir, Path(yeardir))
        contents = "<articles>\n"
        contents_dir = Path(yeardir) / "source" / topicpath.name
        if not override_existing and list(contents_dir.glob("*.xml")):
            filecounter = max(int(p.stem) for p in list(contents_dir.glob("*.xml"))) + 1
        else:
            filecounter = 1
        for p in sorted(topicpath.rglob("./*")):
            if p.is_file() and p.suffix == ".json":

                if not override_existing and str(p) in processed_json:
                    if debug:
                        print(f"  Skipping {p}, already processed in {processed_json[str(p)]}")
                    continue

                if debug:
                    print(f"  Processing {p}")
                with open(p) as f:
                    article_json = json.load(f)
                    xml = process_article(article_json[0])
                    contents += xml + "\n"
                    processed_json[str(p)] = str(contents_dir / str(filecounter)) + ".xml"
                    # Write files that are around 5 MB in size
                    if len(contents.encode("utf-8")) > 5000000:
                        write_contents(contents, contents_dir, filecounter)
                        contents = "<articles>\n"
                        filecounter += 1
                processed += 1
                processed_now += 1
        # Write remaining contents
        if len(contents) > 11:
            write_contents(contents, contents_dir, filecounter)
        print(f"Processed {processed} new article files in '{topicpath}'\n")

        write_json(processed_json, PROCESSED_JSON)

    if not processed_now:
        print(f"No new articles found")
    else:
        print(f"Done converting {processed_now} articles to XML!")


def process_article(article_json):
    """Parse JSON for one article and transform to XML"""
    def parse_element(elem, parent):
        xml_elem = parent
        json_tag = elem.get("type")
        # Skip images and videos
        if elem.get("type") not in ["svt-image", "svt-video", "svt-scribblefeed"]:
            if parent.text is not None:
                # If parent already contains text, don't override it
                parent.text = parent.text + " " + elem.get("content", "")
            elif elem.get("content", "").strip():
                parent.text = elem.get("content", "")
        if "children" in elem:
            # Keep only p and h* tags (but convert h* to p), avoid nested p tags
            if re.match(r"p|h\d", json_tag) and parent.tag != "p":
                xml_elem = etree.SubElement(parent, "p")
            # xml_elem = etree.SubElement(parent, elem.get("type"))
            for c in elem.get("children"):
                return parse_element(c, xml_elem)
        return parent

    def set_attribute(xml_elem, article_json, json_name, xml_name):
        attr = str(article_json.get(json_name, "")).strip()
        if attr:
            xml_elem.set(xml_name, attr)

    article = etree.Element("text")

    # Set article date or omit if year is out of range
    this_year = int(datetime.today().strftime("%Y"))
    if article_json.get("published"):
        year = int(article_json.get("published")[:4])
        if (year >= 2004) and (year <= this_year):
            article.set("date", article_json.get("published"))
    elif article_json.get("modified"):
        year = int(article_json.get("modified")[:4])
        if (year >= 2004) and (year <= this_year):
            article.set("date", article_json.get("modified"))

    # Set article attributes
    set_attribute(article, article_json, "id", "id")
    set_attribute(article, article_json, "sectionDisplayName", "section")
    set_attribute(article, article_json, "title", "title")
    set_attribute(article, article_json, "subtitle", "subtitle")
    set_attribute(article, article_json, "url", "url")
    url = str(article_json.get("url", "")).strip()
    if url and not url.startswith("http") or url.startswith("www"):
        article.set("url", "https://www.svt.se" + url)
    authors = "|".join(a.get("name", "").strip() for a in article_json.get("authors", []))
    if authors:
        article.set("authors", "|" + authors + "|")
    tags = "|".join(a.get("name", "") for a in article_json.get("tags", []))
    if tags:
        article.set("tags", "|" + tags + "|")

    # Include the title and lead in the text
    title = etree.SubElement(article, "p")
    title.text = article_json.get("title", "").strip()
    title.set("type", "title")
    if article_json.get("structuredLead"):
        for i in article_json.get("structuredLead"):
            p = parse_element(i, article)
            p.set("type", "lead")

    # Process body
    if article_json.get("structuredBody"):
        for i in article_json.get("structuredBody"):
            try:
                parse_element(i, article)
            except Exception as e:
                print("Something went wrong with this element:")
                print(json.dumps(i))
                print(e)
                exit()

    # Remove empty elemets (not tested!!)
    # https://stackoverflow.com/questions/30652470/clean-xml-remove-line-if-any-empty-tags
    for element in article.xpath(".//*[not(node())]"):
        element.getparent().remove(element)
    # Stringify tree
    contents = etree.tostring(article, encoding="utf-8").decode("utf-8")
    # Replace non-breaking spaces with ordinary spaces
    contents = contents.replace(u"\xa0", u" ")
    return contents


def crawled_data_from_files(outfile):
    """Compile an index of the crawled data based on downloaded files."""
    crawled_data = {}
    for jsonpath in DATADIR.rglob("svt-*/*/*.json"):
        year = jsonpath.parts[1][4:]
        topic = jsonpath.parts[2]
        article_id = jsonpath.stem

        with open(jsonpath) as f:
            article_json = json.load(f)[0]
            url = article_json.get("url")
            crawled_data[url] = [article_id, year, topic]

    write_json(crawled_data, DATADIR / outfile)
    print(f"Done writing index of crawled data to '{DATADIR / outfile}'\n")


#-------------------------------------------------------------------------------
# Auxiliaries
#-------------------------------------------------------------------------------

def write_json(data, filepath):
    """Write json data to filepath."""
    dirpath = filepath.parent
    dirpath.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def write_data(data, filepath):
    """Write arbitrary data to filepath."""
    dirpath = filepath.parent
    dirpath.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w") as f:
        f.write(data)


def make_corpus_config(corpus_id, path, override=False):
    """Write Sparv corpus config file for sub corpus."""
    config_file = path / "config.yaml"
    if config_file.is_file() and not override:
        return
    path.mkdir(parents=True, exist_ok=True)
    year = corpus_id.split("-")[-1]
    config_content = (
        "parent: ../config.yaml\n"
        "\n"
        "metadata:\n"
        f"  id: {corpus_id}\n"
        "  name:\n"
        f"    swe: SVT nyheter {year if year != 'nodate' else 'okänt datum'}\n"
        f"    eng: SVT news {year if year != 'nodate' else 'unknown date'}\n"
    )
    with open(config_file, "w") as f:
        f.write(config_content)
    print(f"{config_file} written")


#-------------------------------------------------------------------------------
if __name__ == "__main__":
    # Parse command line args, print help if none are given
    args = parser.parse_args(args=None if sys.argv[1:] else ["--help"])

    if args.command == "crawl":
        if args.retry:
            print("\nTrying to crawl pages that failed last time ...\n")
            if args.force:
                print("Argument '--force' is ignored when recrawling failed pages.\n")
            SvtParser(debug=True).retry_failed()
        else:
            if args.stop:
                print(f"\nStarting to crawl svt.se (until {args.stop.strftime('%Y-%m-%d')}) ...\n")
            else:
                print("\nStarting to crawl svt.se ...\n")
            time.sleep(5)
            SvtParser(debug=args.debug).crawl(force=args.force, stopdate=args.stop)

    elif args.command == "summary":
        print("\nCalculating summary of collected articles ...\n")
        SvtParser().get_articles_summary()

    elif args.command == "xml":
        if args.year:
            print(f"\nPreparing to convert articles from {args.year.strftime('%Y')} to XML ...\n")
        else:
            print("\nPreparing to convert articles to XML ...\n")
        process_articles(year=args.year, override_existing=args.override, debug=args.debug)

    elif args.command == "build-index":
        print("\nBuilding an index of crawled files based on the downloaded JSON files ...\n")
        crawled_data_from_files(args.out)


    ## DEBUG STUFF

    # SvtParser().get_article("/nyheter/inrikes/toppmote-om-arktis-i-kiruna", "inrikes")

    # with open("data/svt-2020/konsument/28334881.json") as f:
    #     article_json = json.load(f)
    #     xml = process_article(article_json[0])
    #     print(xml)
