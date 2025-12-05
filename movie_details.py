import requests
import time
import csv
import random
import concurrent.futures
from bs4 import BeautifulSoup

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/120.0 Safari/537.36'
}

MAX_THREADS = 10


def extract_movie_details(movie_link):
    time.sleep(random.uniform(0.1, 0.3))

    try:
        response = requests.get(movie_link, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Erro ao acessar {movie_link}: {e}")
        return None

    soup = BeautifulSoup(response.content, 'html.parser')

    title = None
    date = None
    rating = None
    plot_text = None

    # Título
    title_tag = soup.find('h1')
    if title_tag:
        span = title_tag.find('span')
        title = span.get_text(strip=True) if span else title_tag.get_text(strip=True)

    # Data de lançamento
    date_tag = soup.find('a', href=lambda href: href and 'releaseinfo' in href)
    if date_tag:
        date = date_tag.get_text(strip=True)

    # Nota
    rating_tag = soup.find('div', attrs={'data-testid': 'hero-rating-bar__aggregate-rating__score'})
    if rating_tag:
        rating = rating_tag.get_text(strip=True)

    # Sinopse
    plot_tag = soup.find('span', attrs={'data-testid': 'plot-xs_to_m'})
    if plot_tag:
        plot_text = plot_tag.get_text(strip=True)

    if all([title, date, rating, plot_text]):
        print(title, date, rating)
        return [title, date, rating, plot_text]

    return None


def extract_movies(soup):
    chart_div = soup.find('div', attrs={'data-testid': 'chart-layout-main-column'})
    if not chart_div:
        print("Erro: não consegui encontrar a lista principal de filmes")
        return []

    movies_table = chart_div.find('ul')
    if not movies_table:
        print("Erro: não encontrei a <ul> da lista de filmes")
        return []

    movies_table_rows = movies_table.find_all('li')

    movie_links = [
        'https://www.imdb.com' + movie.find('a')['href']
        for movie in movies_table_rows
        if movie.find('a')
    ]

    results = []

    threads = min(MAX_THREADS, len(movie_links))

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        for result in executor.map(extract_movie_details, movie_links):
            if result:
                results.append(result)

    return results


def save_to_csv(data, filename='movies.csv'):
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Título', 'Data de Lançamento', 'Classificação', 'Sinopse'])
        writer.writerows(data)


def main():
    start_time = time.time()

    popular_movies_url = 'https://www.imdb.com/chart/moviemeter/?ref_=nv_mv_mpm'

    try:
        response = requests.get(popular_movies_url, headers=headers, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Erro ao acessar a página principal: {e}")
        return

    soup = BeautifulSoup(response.content, 'html.parser')

    movies_data = extract_movies(soup)

    if movies_data:
        save_to_csv(movies_data)

    end_time = time.time()
    print(f'Total time taken: {end_time - start_time:.2f} seconds')


if __name__ == '__main__':
    main()
