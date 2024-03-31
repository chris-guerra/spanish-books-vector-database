"""
This script is designed to scrape book information from a specific website,
collecting details such as author names, book titles, and download links,
and saving this information into CSV files for further analysis.
"""

import os
import requests
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

def get_author(soup_element):
    """Extract the list of authors from a BeautifulSoup element."""
    authors = []

    for line in soup_element.find_all('div', {"class": "subdetail"}):
        try:
            if 'autor' in line.a.get('href'):
                authors.append(line.a.text)
        except AttributeError:
            authors.append(np.nan)

    return authors

def get_title_and_website(soup_element):
    """Extract book titles and their corresponding URLs."""
    base_url = 'https://www.lectulandia.co'
    titles = []
    websites = []

    for line in soup_element.find_all('a', {"class": "title"}):
        titles.append(line.get('title'))
        websites.append(base_url + line.get('href'))

    return titles, websites

def scrape_books(pages):
    """Scrape book information from the specified number of pages."""
    df = pd.DataFrame()

    for page in range(1, pages + 1):
        url = f'https://www.lectulandia.co/book/page/{page}/'
        attempts = 0
        success = False

        while attempts < 5 and not success:
            try:
                site_response = requests.get(url, timeout=50)
                soup_object = BeautifulSoup(site_response.content, 'html.parser')
                success = True  # If the request was successful, break out of the loop
            except requests.exceptions.Timeout:
                attempts += 1
                print(f"Timeout occurred for {url}. Retrying... Attempt {attempts}")
                if attempts == 5:
                    print(f"Failed to fetch page after 5 attempts: {url}")
                    continue  # Skip this page if all retries fail

        if not success:
            continue  # Skip to the next page if unsuccessful after retries

        data = {
            'author': get_author(soup_object),
            'title': get_title_and_website(soup_object)[0],
            'website': get_title_and_website(soup_object)[1]
        }

        page_df = pd.DataFrame(data)
        df = pd.concat([df, page_df], ignore_index=True)

        print(f"{int(page / pages * 100)}%", f"Page: {page}", end="\r")

    print('\nTotal Books Scraped:', df.shape[0])
    return df

def get_book_details(row, df):
    """Fetch genre, description, and download links for each book."""
    max_attempts = 5
    attempts = 0
    success = False

    while attempts < max_attempts and not success:
        try:
            site_response = requests.get(df.loc[row, 'website'], timeout=50)
            site_soup = BeautifulSoup(site_response.content, 'html.parser')

            df.loc[row, 'description'] = site_soup.find(
                'div', {"class": "realign", "id": "sinopsis"}).text.strip()
            genres = ' / '.join([a.text for a in site_soup.find(
                'div', {"id": "genero"}).find_all('a')])
            df.loc[row, 'genre'] = genres

            download_links = site_soup.find('div', {"id": "downloadContainer"}).find_all('a')
            for link in download_links:
                if "epub" in link.text.lower():
                    df.loc[row, 'epub'] = 'https://www.lectulandia.co' + link.get('href')
                elif "pdf" in link.text.lower():
                    df.loc[row, 'pdf'] = 'https://www.lectulandia.co' + link.get('href')

            success = True
        except RequestException as e:
            print(f"Network error occurred for row {row}: {e}")
            attempts += 1
        except AttributeError as e:
            print(f"Attribute error for row {row}: {e}")
            attempts += 1

        if attempts == max_attempts:
            print(f"Failed to process row {row} after {max_attempts} attempts.")

def enrich_book_data(df):
    """Add additional information for each book in the DataFrame."""
    total_books = df.shape[0]
    for index, _ in df.iterrows():
        get_book_details(index, df)
        print(f"Processing {int((index + 1) / total_books * 100)}%", f"Book: {index + 1}", end="\r")

# Main execution starts here
if __name__ == "__main__":
    # Initial data extraction
    INITIAL_URL = 'https://www.lectulandia.co/book/'
    response = requests.get(INITIAL_URL, timeout=50)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Assuming the second last link is the last page
    last_page_link = soup.find_all(
        'a', {"class": "page-numbers"})[-2]  
    total_pages = int(last_page_link.text.replace(".", ""))

    # Scraping all sites
    print('Scraping Information from', total_pages, 'Pages.')
    books_df = scrape_books(total_pages)

    # Define the directory and file path
    DATA_DIRECTORY = "data/raw_data"

    # Check if the directory exists, and create it if it does not
    if not os.path.exists(DATA_DIRECTORY):
        os.makedirs(DATA_DIRECTORY)      
    # Saving initial data
    FILE_NAME = "book_data_initial.csv"
    file_path = os.path.join(DATA_DIRECTORY, FILE_NAME)
    books_df.to_csv("data/raw_data/book_data_initial.csv", index=False, encoding="utf-8-sig")

    # Adding placeholders for additional information
    for column in ['genre', 'description', 'epub', 'pdf']:
        books_df[column] = np.nan

    # Adding detailed info for each row
    enrich_book_data(books_df)

    # Saving final data
    FILE_NAME = "book_data_final.csv"
    file_path = os.path.join(DATA_DIRECTORY, FILE_NAME)
    books_df.to_csv(file_path, index=False, encoding="utf-8-sig")
