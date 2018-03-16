"""Scrape Green Card applications from Trackitt."""

import re

import bs4
import os
import requests

import pandas as pd

from multiprocessing import Pool


BASE_URL = "http://www.trackitt.com/usa-immigration-trackers"
URL_DATA_TABLE = os.path.join(BASE_URL, "i140", 'page', '%d')
URL_APPLICATION_PAGE = os.path.join(BASE_URL, "discuss", "i140", "%s")
URL_FILTER_PAGE = os.path.join(BASE_URL, "i140", "filter")


def soup_from_url(url):
    """Return a bs4 object from a url."""
    page = requests.get(url)
    return bs4.BeautifulSoup(page.text)


def trackitt_basic_filters(url):
    """Return a dictionary with basic filters to select i-140 cases."""
    soup = soup_from_url(url)
    select_filters = soup.find("div", {"id": "filterdiv"})
    return {s["name"]: {
        option.text: option["value"] for option in s.find_all("option")}
        for s in select_filters.find_all("select")}


def trackitt_number_of_cases(url):
    """Return total number of cases."""
    page = requests.get(url)
    return int(re.findall(
        'Total # of cases in this tracker = (\d+[,]\d{3})', page.text)[0]
        .replace(',', ''))


def trackitt_number_of_pages(url):
    """Return total number of pages to scrape."""
    soup = soup_from_url(url)
    return len(soup.find("div", {"class": "paginator"})
               .find_all("option"))


def trackitt_data_table_soup(urlstring, pagenum=1):
    """Return bs4 object of the table with i-140 cases."""
    soup = soup_from_url(urlstring % pagenum)
    return soup.find("table", {'id': "myTable01"})


def urls_from_trackitt_table_body(table_body_soup):
    """Return all links to pages with further details of a case."""
    return [link["href"] for link in table_body_soup.find_all(
        "a", {"title": "Discuss this case"})]


def dataframe_from_trackitt_table(data_table_soup):
    """Return a dataframe with all case data from a bs4 table object."""
    header = data_table_soup.find('thead')
    cols = ["ApplicationID", "Data Snapshot Date"] + [
        col.text for col in header.find_all('font')]
    body = data_table_soup.find('tbody')
    # TODO: edit notes using case's link
    data_rows = [
        [row.find("a", {"title": "Discuss this case"})['href'].split('/')[-1],
         pd.Timestamp.today()] +
        [field.text.strip() for field in row.find_all('td')]
        for row in body.find_all('tr')]
    return pd.DataFrame(data_rows, columns=cols)


def application_ids_w_additional_notes(data):
    """Return a list with application ids with incomplete notes."""
    return data[data.Notes.str.endswith("more"), 'ApplicationID'].tolist()


def notes_from_application_id(application_id):
    """Return notes from application_id discussion page as a bs4 object."""
    page = requests.get(URL_APPLICATION_PAGE % application_id)
    soup = bs4.BeautifulSoup(page.text)
    return (soup
            .find("td", text=re.compile(r'Notes:'))
            .find_next_sibling("td")
            .text)


def updated_notes(data):
    """Return dictionary application_id: notes for all incomplete notes."""
    p = Pool(10)
    application_ids = application_ids_w_additional_notes(data)
    notes = p.map(notes_from_application_id, application_ids)
    return dict(zip(application_ids, notes)
