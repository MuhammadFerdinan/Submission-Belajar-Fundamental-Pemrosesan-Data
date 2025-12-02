import pandas as pd
import re
import requests
import time
from bs4 import BeautifulSoup
from datetime import datetime

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114"
    )
}

url = 'https://fashion-studio.dicoding.dev/'

try:
    initial_response = requests.get(url, headers=HEADERS)
    initial_response.raise_for_status()
    initial_content_soup = BeautifulSoup(initial_response.content.decode(), 'html.parser')
    all_content_global = initial_content_soup.find_all('div', recursive=False)
except requests.exceptions.RequestException as e:
    print(f"Terjadi kesalahan saat mengambil konten awal dari {url}: {e}")
    all_content_global = []

def extract_fashion_data(article):
    """Mengambil data Fashion Studio yang mencakup Title (Judul), Price (Harga),
       Rating (Peringkat), Colors (Warna), Size (Ukuran), dan Gender (Jenis Kelamin)"""
    try:
        product_details = article.find('div', class_='product-details')
        if not product_details:
            return None
        fashion_title = product_details.find('h3', class_='product-title').text.strip() if product_details.find('h3', class_='product-title') else "N/A"
        price = product_details.find('div', class_='price-container').text.strip() if product_details.find('div', class_='price-container') else "N/A"
        paragraphs = product_details.find_all('p')

        # Menginisialisasi nilai untuk di-extract
        numeric_rating = "N/A"
        num_colors = "N/A"
        extracted_size = "N/A"
        gender_value = "N/A"

        # 1. Proses extract 'Rating' (contoh, "Rating: ⭐ 3.9 / 5" atau "Rating: ⭐ Invalid Rating / 5")
        if len(paragraphs) > 0:
            rating_text = paragraphs[0].text.strip()
            match_rating = re.search(r'⭐\s*(\d+\.?\d*)', rating_text)
            if match_rating:
                numeric_rating = match_rating.group(1)
        
        # 2. Proses extract 'Colors' (contoh, "3 Colors")
        if len(paragraphs) > 1:
            colors_text = paragraphs[1].text.strip()
            match_colors = re.search(r'(\d+)\s*Colors', colors_text)
            if match_colors:
                num_colors = match_colors.group(1)

        # 3. Proses extract 'Size' (contoh, "Size: M")
        if len(paragraphs) > 2:
            size_text = paragraphs[2].text.strip()
            match_size = re.search(r'Size:\s*(\S+)', size_text)
            if match_size:
                extracted_size = match_size.group(1)

        # 4. Mengiterasi seluruh paragraf untuk menemukan 'Gender'
        for p_tag in paragraphs:
            text = p_tag.text.strip()
            match_gender = re.search(r'Gender:\s*(Men|Women|Unisex)', text)
            if match_gender:
                gender_value = match_gender.group(1)
                break

        extracted_data = {
            "Title": fashion_title,
            "Price": price,
            "Rating": numeric_rating,
            "Colors": num_colors,
            "Size": extracted_size,
            "Gender": gender_value,
            "Timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        return extracted_data
    except Exception as e:
        print(f"Gagal memuat data:{e}, lewati proses scraping untuk artikel ini")
        return None

def fetching_fashion_content(url):
    """Mengambil konten dari URL Fashion Studio"""
    try:
        session = requests.Session()
        response = session.get(url, headers=HEADERS)
        response.raise_for_status()
        return response.content
    except requests.exceptions.RequestException as e:
        print(f"Terjadi kesalahan saat memuat {url}: {e}")
        return None

def scrape_fashion(base_site_url, pagination_path_pattern, delay=2, max_pages=None):
    """Fungsi untuk mengambil semua data, mulai dari request hingga variabel data"""
    data = []
    page_number = 1

    while True:
        if max_pages is not None and page_number > max_pages:
            print(f"Mencapai jumlah halaman maksimum ({max_pages}), selesaikan proses scraping.")
            break
        if page_number == 1:
            url = base_site_url
        else:
            url = f"{base_site_url}{pagination_path_pattern.format(page_number)}"

        print(f"Scraping halaman: {url}")

        content = fetching_fashion_content(url)
        if content:
            try:
                soup = BeautifulSoup(content, 'html.parser')
                product_detail_divs = soup.find_all('div', class_='product-details')
                articles_element = []
                for pd_div in product_detail_divs:
                    parent_product_container = pd_div.find_parent()
                    if parent_product_container:
                        articles_element.append(parent_product_container)
                if not articles_element:
                    print(f"Tidak ditemukan kontainer item produk di {url}, akhiri proses scraping.")
                    break
                for article in articles_element:
                    fashion = extract_fashion_data(article)
                    if fashion:
                        data.append(fashion)

                next_page_link = soup.find('a', class_='page-link', string='Next')
                if next_page_link and next_page_link.get('href'):
                    page_number += 1
                    time.sleep(delay)
                else:
                    print(f"Tidak ditemukan halaman berikutnya di {url}, hentikan proses scraping.")
                    break
            except Exception as e:
                print(f"Terjadi kesalahan saat memproses halaman {url}: {e}")
                break
        else:
            print(f"Gagal mengambil konten untuk {url}, akhiri proses scraping.")
            break
    return data

def main(delay=0.1):
    """Mengambil waktu pada proses scraping Title, Price, Rating, Colors, Size, dan Gender"""
    try:
        print(f"Proses scraping dimulai pada: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        start_time = datetime.now()
        BASE_SITE_URL = 'https://fashion-studio.dicoding.dev'
        PAGINATION_PATH_PATTERN = '/page{}'
        all_content_data = scrape_fashion(BASE_SITE_URL, PAGINATION_PATH_PATTERN, delay=delay, max_pages=None)

        if all_content_data:
            df = pd.DataFrame(all_content_data)
            print(df.head())
            print(f"Jumlah produk yang sudah di-scrape: {len(df)}")
            return df
        else:
            print("Tidak tersedia data untuk di-scrape.")
            return pd.DataFrame()
    except Exception as e:
        print(f"Gagal melakukan scraping website secara keseluruhan: {e}")
        return pd.DataFrame()
    finally:
        end_time = datetime.now()
        total_time = end_time - start_time
        print(f"Proses scraping selesai pada: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Jumlah waktu proses extract: {total_time}")

if __name__ == "__main__":
    final_df = main()