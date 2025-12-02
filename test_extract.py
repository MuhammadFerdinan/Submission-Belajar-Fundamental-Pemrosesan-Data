import builtins
import coverage
import pandas as pd
import pytest
import re
import requests
import sys
import time
import unittest
from bs4 import BeautifulSoup
from unittest.mock import patch, Mock, call

if 'utils.extract' in sys.modules:
    del sys.modules['utils.extract']

try:
    from utils.extract import HEADERS, extract_fashion_data, fetching_fashion_content, scrape_fashion
except ImportError as e:
    print(f"Error: Tidak dapat mengimpor fungsi dari utils.extract. Pastikan extract.py ada dan berada di PYTHONPATH. Error: {e}")
    
class TestExtractFunctions(unittest.TestCase):

    def test_extract_fashion_data(self):
        """Menguji `extract_fashion_data` dengan contoh artikel HTML."""
        # Contoh konten HTML untuk satu artikel produk
        sample_html = """
        <div class=\"product-container\">
            <div class=\"product-details\">
                <h3 class=\"product-title\">Stylish Shirt</h3>
                <div class=\"price-container\">$25.50</div>
                <p>Rating: ⭐ 4.5 / 5</p>
                <p>2 Colors</p>
                <p>Size: M</p>
                <p>Gender: Men</p>
            </div>
        </div>
        """
        article_soup = BeautifulSoup(sample_html, 'html.parser').find('div', class_='product-container')

        expected_data = {
            "Title": "Stylish Shirt",
            "Price": "$25.50",
            "Rating": "4.5",
            "Colors": "2",
            "Size": "M",
            "Gender": "Men"
        }

        result = extract_fashion_data(article_soup)
        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        self.assertEqual(result['Title'], expected_data['Title'])
        self.assertEqual(result['Price'], expected_data['Price'])
        self.assertEqual(result['Rating'], expected_data['Rating'])
        self.assertEqual(result['Colors'], expected_data['Colors'])
        self.assertEqual(result['Size'], expected_data['Size'])
        self.assertEqual(result['Gender'], expected_data['Gender'])

        # Uji dengan skenario 'Unknown Product' dan 'N/A'
        sample_html_unknown = """
        <div class=\"product-container\">
            <div class=\"product-details\">
                <h3 class=\"product-title\">Unknown Product</h3>
                <div class=\"price-container\">$0.00</div>
                <p>Rating: ⭐ Invalid Rating / 5</p>
                <p>0 Colors</p>
                <p>Size: N/A</p>
                <p>Gender: N/A</p>
            </div>
        </div>
        """
        article_soup_unknown = BeautifulSoup(sample_html_unknown, 'html.parser').find('div', class_='product-container')
        result_unknown = extract_fashion_data(article_soup_unknown)
        self.assertEqual(result_unknown['Title'], 'Unknown Product')
        self.assertEqual(result_unknown['Rating'], 'N/A')
        self.assertEqual(result_unknown['Colors'], '0')
        self.assertEqual(result_unknown['Size'], 'N/A')
        self.assertEqual(result_unknown['Gender'], 'N/A')

    def test_extract_fashion_data_minimal_info(self):
        """Menguji `extract_fashion_data` dengan info minimal (hanya judul dan harga)."""
        sample_html = """
        <div class=\"product-container\">
            <div class=\"product-details\">
                <h3 class=\"product-title\">Basic Item</h3>
                <div class=\"price-container\">$10.00</div>
            </div>
        </div>
        """
        article_soup = BeautifulSoup(sample_html, 'html.parser').find('div', class_='product-container')
        result = extract_fashion_data(article_soup)
        self.assertIsNotNone(result)
        self.assertEqual(result['Title'], 'Basic Item')
        self.assertEqual(result['Price'], '$10.00')
        self.assertEqual(result['Rating'], 'N/A')
        self.assertEqual(result['Colors'], 'N/A')
        self.assertEqual(result['Size'], 'N/A')
        self.assertEqual(result['Gender'], 'N/A')

    def test_extract_fashion_data_no_product_details(self):
        """Menguji `extract_fashion_data` ketika tidak ada div detail produk yang ditemukan."""
        sample_html = """
        <div class=\"product-container\">
            <div>Konten lain</div>
        </div>
        """
        article_soup = BeautifulSoup(sample_html, 'html.parser').find('div', class_='product-container')
        result = extract_fashion_data(article_soup)
        self.assertIsNone(result)

    @patch('utils.extract.BeautifulSoup')
    def test_extract_fashion_data_exception_handling(self, mock_beautifulsoup):
        """Menguji penanganan pengecualian di `extract_fashion_data`."""
        # Mensimulasikan skenario di mana pencarian product-title menyebabkan pengecualian
        mock_product_details = Mock()
        mock_product_details.find.side_effect = Exception("Forced parsing error")

        mock_article = Mock()
        mock_article.find.return_value = mock_product_details

        with patch('builtins.print') as mock_print:
            result = extract_fashion_data(mock_article)
            self.assertIsNone(result)
            mock_print.assert_called_with("Gagal memuat data:Forced parsing error, lewati proses scraping untuk artikel ini")

    @patch('requests.Session')
    @patch('builtins.print')
    def test_fetching_fashion_content(self, mock_print, mock_session):
        """Menguji `fetching_fashion_content` untuk memastikan ia mengambil konten dengan benar.
        Kami menggunakan @patch untuk menghindari permintaan jaringan yang sebenarnya dan menekan pernyataan print.
        """
        mock_response = Mock()
        mock_response.content = b"<html><head></head><body><h1>Test Content</h1></body></html>"
        mock_response.raise_for_status.return_value = None
        mock_session_instance = mock_session.return_value
        mock_session_instance.get.return_value = mock_response

        test_url = 'https://fashion-studio.dicoding.dev/'
        result = fetching_fashion_content(test_url)

        mock_session_instance.get.assert_called_once_with(test_url, headers=HEADERS)
        self.assertEqual(result, b"<html><head></head><body><h1>Test Content</h1></body></html>")
        mock_response.raise_for_status.assert_called_once()
        mock_session_instance.get.reset_mock()
        mock_response.raise_for_status.side_effect = requests.exceptions.RequestException("Test Error")
        mock_session_instance.get.return_value = mock_response

        result_error = fetching_fashion_content(test_url)
        self.assertIsNone(result_error)
        mock_print.assert_called()

    @patch('utils.extract.extract_fashion_data')
    @patch('utils.extract.fetching_fashion_content')
    @patch('builtins.print')
    def test_scrape_fashion_process(self, mock_print, mock_fetching_content, mock_extract_data):
        """Menguji proses scrape_fashion secara keseluruhan, termasuk paginasi.
        Kami mengolok-olok fetching_fashion_content dan extract_fashion_data, dan menekan pernyataan print.
        """
        BASE_URL = 'https://fashion-studio.dicoding.dev'
        PAGINATION_PATH = '/page{}'

        # Mensimulasikan konten untuk 3 halaman
        page1_content = "<html><body><div class='product-container'><div class='product-details'><h3 class='product-title'>Item 1</h3></div></div><a class='page-link' href='/page2'>Next</a></body></html>".encode()
        page2_content = "<html><body><div class='product-container'><div class='product-details'><h3 class='product-title'>Item 2</h3></div></div><a class='page-link' href='/page3'>Next</a></body></html>".encode()
        page3_content = "<html><body><div class='product-container'><div class='product-details'><h3 class='product-title'>Item 3</h3></div></div></body></html>".encode()

        # Konfigurasi mock_fetching_content untuk mengembalikan konten yang berbeda untuk setiap halaman
        mock_fetching_content.side_effect = [
            page1_content,
            page2_content,
            page3_content
        ]

        # Konfigurasi mock_extract_data untuk mengembalikan kamus tertentu
        mock_extract_data.side_effect = [
            {'Title': 'Item 1', 'Price': '$10', 'Rating': '4.0', 'Colors': '1', 'Size': 'S', 'Gender': 'Unisex'},
            {'Title': 'Item 2', 'Price': '$20', 'Rating': '4.5', 'Colors': '2', 'Size': 'M', 'Gender': 'Men'},
            {'Title': 'Item 3', 'Price': '$30', 'Rating': '5.0', 'Colors': '3', 'Size': 'L', 'Gender': 'Women'}
        ]

        scraped_data = scrape_fashion(BASE_URL, PAGINATION_PATH, delay=0, max_pages=5)

        mock_fetching_content.assert_has_calls([
            call(BASE_URL),
            call(f"{BASE_URL}{PAGINATION_PATH.format(2)}"),
            call(f"{BASE_URL}{PAGINATION_PATH.format(3)}")
        ])
        self.assertEqual(mock_fetching_content.call_count, 3)
        self.assertEqual(mock_extract_data.call_count, 3)
        self.assertEqual(len(scraped_data), 3)
        self.assertEqual(scraped_data[0]['Title'], 'Item 1')
        self.assertEqual(scraped_data[1]['Title'], 'Item 2')
        self.assertEqual(scraped_data[2]['Title'], 'Item 3')

    @patch('utils.extract.extract_fashion_data')
    @patch('utils.extract.fetching_fashion_content')
    @patch('builtins.print')
    def test_scrape_fashion_max_pages_reached(self, mock_print, mock_fetching_content, mock_extract_data):
        """Menguji `scrape_fashion` berhenti ketika max_pages tercapai."""
        BASE_URL = 'http://test.com'
        PAGINATION_PATH = '/page{}'
        page_content = "<html><body><div class='product-container'><div class='product-details'><h3>Item</h3></div></div><a class='page-link' href='/page2'>Next</a></body></html>".encode()
        mock_fetching_content.return_value = page_content
        mock_extract_data.return_value = {'Title': 'Item'}

        scraped_data = scrape_fashion(BASE_URL, PAGINATION_PATH, delay=0, max_pages=1)

        self.assertEqual(mock_fetching_content.call_count, 1)
        self.assertEqual(len(scraped_data), 1)
        mock_print.assert_any_call("Mencapai jumlah halaman maksimum (1), selesaikan proses scraping.")

    @patch('utils.extract.extract_fashion_data')
    @patch('utils.extract.fetching_fashion_content')
    @patch('builtins.print')
    def test_scrape_fashion_no_product_items_on_page(self, mock_print, mock_fetching_content, mock_extract_data):
        """Menguji `scrape_fashion` berhenti ketika suatu halaman tidak memiliki item produk."""
        BASE_URL = 'http://test.com'
        PAGINATION_PATH = '/page{}'
        # Halaman pertama dengan item, halaman kedua tanpa item
        page1_content = "<html><body><div class='product-container'><div class='product-details'><h3>Item 1</h3></div></div><a class='page-link' href='/page2'>Next</a></body></html>".encode()
        page2_content_no_items = "<html><body><div>No items here</div></body></html>".encode()

        mock_fetching_content.side_effect = [page1_content, page2_content_no_items]
        mock_extract_data.return_value = {'Title': 'Item 1', 'Price': '$10', 'Rating': '4.0', 'Colors': '1', 'Size': 'S', 'Gender': 'Unisex'}

        scraped_data = scrape_fashion(BASE_URL, PAGINATION_PATH, delay=0, max_pages=5)

        self.assertEqual(mock_fetching_content.call_count, 2)
        self.assertEqual(len(scraped_data), 1)
        mock_print.assert_any_call(f"Tidak ditemukan kontainer item produk di {BASE_URL}{PAGINATION_PATH.format(2)}, akhiri proses scraping.")

    @patch('utils.extract.extract_fashion_data')
    @patch('utils.extract.fetching_fashion_content')
    @patch('builtins.print')
    def test_scrape_fashion_fetching_error_mid_process(self, mock_print, mock_fetching_content, mock_extract_data):
        """Menguji `scrape_fashion` menangani kesalahan pengambilan di tengah proses."""
        BASE_URL = 'http://test.com'
        PAGINATION_PATH = '/page{}'
        page1_content = "<html><body><div class='product-container'><div class='product-details'><h3>Item 1</h3></div></div><a class='page-link' href='/page2'>Next</a></body></html>".encode()

        mock_fetching_content.side_effect = [page1_content, None]
        mock_extract_data = Mock(return_value={'Title': 'Item 1', 'Price': '$10', 'Rating': '4.0', 'Colors': '1', 'Size': 'S', 'Gender': 'Unisex'})
        mock_extract_data.return_value = {'Title': 'Item 1'}
        scraped_data = scrape_fashion(BASE_URL, PAGINATION_PATH, delay=0, max_pages=5)

        self.assertEqual(mock_fetching_content.call_count, 2)
        self.assertEqual(len(scraped_data), 1)
        mock_print.assert_any_call(f"Gagal mengambil konten untuk {BASE_URL}{PAGINATION_PATH.format(2)}, akhiri proses scraping.")

    @patch('utils.extract.extract_fashion_data')
    @patch('utils.extract.fetching_fashion_content')
    @patch('builtins.print')
    def test_scrape_fashion_page_processing_exception(self, mock_print, mock_fetching_content, mock_extract_data):
        """Menguji `scrape_fashion` menangani pengecualian selama parsing BeautifulSoup dalam pemrosesan halaman."""
        BASE_SITE_URL = 'http://test.com'
        PAGINATION_PATH = '/page{}'
        page1_content = b"<html><body><div class='product-container'><div class='product-details'><h3>Item 1</h3></div></div><a class='page-link' href='/page2'>Next</a></body></html>"
        malformed_html_content = b"<html><body/<div>"
        mock_fetching_content.side_effect = [page1_content, malformed_html_content]
        mock_extract_data.return_value = {'Title': 'Item 1', 'Price': '$10', 'Rating': '4.0', 'Colors': '1', 'Size': 'S', 'Gender': 'Unisex'}
        with patch('utils.extract.BeautifulSoup') as mock_bs_constructor:
            mock_bs_constructor.side_effect = [
                BeautifulSoup(page1_content, 'html.parser'),
                Exception("Simulated BeautifulSoup parsing error")
            ]
            scraped_data = scrape_fashion(BASE_SITE_URL, PAGINATION_PATH, delay=0, max_pages=5)

        self.assertEqual(mock_fetching_content.call_count, 2)
        self.assertEqual(len(scraped_data), 1)

        mock_print.assert_any_call(unittest.mock.ANY)
        self.assertIn(f"Terjadi kesalahan saat memproses halaman {BASE_SITE_URL}{PAGINATION_PATH.format(2)}: Simulated BeautifulSoup parsing error", str(mock_print.call_args_list[-1]))

    @patch('utils.extract.fetching_fashion_content')
    @patch('builtins.print')
    def test_scrape_fashion_extract_data_returns_none(self, mock_print, mock_fetching_content):
        """Menguji `scrape_fashion` menangani dengan benar ketika `extract_fashion_data` mengembalikan None untuk sebuah artikel."""
        BASE_URL = 'http://test.com'
        PAGINATION_PATH = '/page{}'
        # Konten halaman dengan dua kontainer produk
        page_content = """
        <html><body>
            <div class='product-container'><div class='product-details'><h3>Item A</h3></div></div>
            <div class='product-container'><div class='product-details'><h3>Item B</h3></div></div>
            <a class='page-link' href='/page2'>Next</a>
        </body></html>
        """.encode()

        mock_fetching_content.return_value = page_content
        mock_extract_data_mock = Mock()
        mock_extract_data_mock.side_effect = [
            {'Title': 'Item A', 'Price': '$10', 'Rating': '4.0', 'Colors': '1', 'Size': 'S', 'Gender': 'Unisex'},
            None
        ]

        with patch('utils.extract.extract_fashion_data', mock_extract_data_mock):
            scraped_data = scrape_fashion(BASE_URL, PAGINATION_PATH, delay=0, max_pages=1)

        mock_fetching_content.assert_called_once()
        self.assertEqual(mock_extract_data_mock.call_count, 2)
        self.assertEqual(len(scraped_data), 1)
        self.assertEqual(scraped_data[0]['Title'], 'Item A')