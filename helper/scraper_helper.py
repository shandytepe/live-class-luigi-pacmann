import pandas as pd
import requests 
from bs4 import BeautifulSoup   
from tqdm import tqdm
import time

def init_scrapper_engine(year: int, season: str):
    """
    Function yang digunakan inisialisasi web scrapper engine MyAnimeList

    Parameters
    ----------
    year (int): tahun yang ingin discrape
    season (str): season yang ingin discrape

    Returns
    -------
    soup (object): BeautifulSoup engine untuk scrape website
    """
    resp = requests.get(f"https://myanimelist.net/anime/season/{year}/{season}")

    soup = BeautifulSoup(resp.text, "html.parser")

    return soup

def scrape_myanimelist(soup, html_tag, show_type):
    """
    Function yang digunakan untuk melakukan proses scrape website MyAnimeList

    Parameters
    ----------
    soup (object): BeautifulSoup engine
    html_tag (str): html tag dari masing - masing show type
    show_type (str): show type yang ingin discrape

    Returns
    -------
    converted_data (DataFrame): hasil data yang sudah berhasil discrape
    """
    
    raw_data = soup.find_all("div", class_ = html_tag)

    anime_data = []

    for data in tqdm(raw_data):
        
        time.sleep(0.05)

        # get title
        title = data.find("a").text

        # get release date
        release_date = data.find("span", class_ = "item").text
        
        # get link mal
        link_mal = data.find("a").get("href")

        # get all info data like release date, anime minutes, and anime episode
        get_anime_info = data.find("div", class_ = "info")

        # get more detail info
        get_anime_detail_info = get_anime_info.find_all('span', class_='item')

        # get release date
        release_date = get_anime_detail_info[0].text

        # get episode
        anime_episode = get_anime_detail_info[1].contents[1].text

        # get duration
        anime_duration = get_anime_detail_info[1].contents[3].text

        # get genre anime data
        genre_raw = data.find_all("div", class_ = "genres-inner js-genre-inner")

        # Iterate through each <div> and extract genres
        for genre_div in genre_raw:

            genre_links = genre_div.find_all('a')
            
            # Extract and print genre names
            genres = [link.text for link in genre_links]

        # get img link
        img_link = data.find("img").get("src")

        # get description
        description = data.find("p").text

        # get studio name
        
        get_studio = data.find_all("div", class_ = "property")[0]

        if get_studio.find("a") is not None:
            studio = get_studio.find("a").text

        else:
            studio = ""

        # get source 
        get_source = data.find_all("div", class_ = "property")[1]

        source = get_source.find("span", class_ = "item").text

        dict_data = {
            "title": title,
            "link_mal": link_mal,
            "release_date": release_date,
            "anime_episode": anime_episode,
            "anime_duration": anime_duration,
            "anime_genre": genres,
            "img_link": img_link,
            "description": description,
            "studio": studio,
            "source": source,
            "show_type": show_type
        }

        anime_data.append(dict_data)
    
    converted_data = pd.DataFrame(anime_data)

    return converted_data

def concat_anime_data(anime_data):
    """
    Function yang digunakan untuk menggabungkan data dari
    berbagai macam show type menjadi satu

    Parameters
    ----------
    anime_data (DataFrame): kumpulan data yang ingin digabungkan
    
    Returns
    -------
    concat_data (DataFrame): data yang sudah berhasil di concat
    """
    concat_data = pd.concat(anime_data)

    return concat_data