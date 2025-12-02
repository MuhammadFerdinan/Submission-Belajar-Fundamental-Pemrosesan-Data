import os
import pandas as pd
import sqlite3
import sys
import unittest
from sqlalchemy import create_engine
from unittest.mock import patch, Mock

# Menambahkan direktori saat ini ke sys.path untuk mengizinkan impor utils.load
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

class TestLoadCSVFunctions(unittest.TestCase):

    def setUp(self):
        """Menyiapkan DataFrame sampel untuk menguji fungsi load."""
        self.test_data = {
            'Title': ['T-shirt 1', 'Hoodie 3', 'Pants 4'],
            'Price': [1600000.0, 7950080.0, 7476960.0],
            'Rating': [3.9, 4.8, 3.3],
            'Colors': [3, 3, 3],
            'Size': ['M', 'L', 'XL'],
            'Gender': ['Women', 'Unisex', 'Men'],
            'Timestamp': ['2023-01-01 12:00:00', '2023-01-01 12:00:01', '2023-01-01 12:00:02']
        }
        self.df = pd.DataFrame(self.test_data)
        self.csv_file = 'test_products.csv'

    def tearDown(self):
        """Membersihkan file apa pun yang dibuat setelah setiap pengujian."""
        if os.path.exists(self.csv_file):
            os.remove(self.csv_file)

    @patch('builtins.print')
    def test_export_to_csv_success(self, mock_print):
        """Menguji ekspor ke dalam format CSV yang berhasil."""
        if 'utils.load' in sys.modules:
            del sys.modules['utils.load']
        from utils.load import export_to_csv

        result = export_to_csv(self.df, filename=self.csv_file)
        self.assertTrue(result)
        self.assertTrue(os.path.exists(self.csv_file))

        df_read = pd.read_csv(self.csv_file)
        pd.testing.assert_frame_equal(self.df, df_read)
        mock_print.assert_any_call(f"Berhasil mengekspor data ke dalam format CSV: {self.csv_file}")

    @patch('builtins.print')
    def test_export_to_csv_failure(self, mock_print):
        """Menguji kegagalan ekspor ke dalam format CSV (contoh, kesalahan izin)."""
        if 'utils.load' in sys.modules:
            del sys.modules['utils.load']
        from utils.load import export_to_csv

        non_existent_path = '/non_existent_dir/product_fail.csv'
        result = export_to_csv(self.df, filename=non_existent_path)
        self.assertFalse(result)
        self.assertFalse(os.path.exists(non_existent_path))
        mock_print.assert_any_call(f"Mulai mengekspor data ke dalam format CSV: {non_existent_path}")
        self.assertIn('Gagal mengekspor data ke dalam format CSV:', str(mock_print.call_args_list[-1]))

class TestLoadGoogleSheetFunctions(unittest.TestCase):

    def setUp(self):
        """Menyiapkan sampel DataFrame untuk menguji fungsi load."""
        self.test_data = {
            'Title': ['T-shirt 1', 'Hoodie 3', 'Pants 4'],
            'Price': [1600000.0, 7950080.0, 7476960.0],
            'Rating': [3.9, 4.8, 3.3],
            'Colors': [3, 3, 3],
            'Size': ['M', 'L', 'XL'],
            'Gender': ['Women', 'Unisex', 'Men'],
            'Timestamp': ['2023-01-01 12:00:00', '2023-01-01 12:00:01', '2023-01-01 12:00:02']
        }
        self.df = pd.DataFrame(self.test_data)

    def tearDown(self):
        """Membersihkan file apa pun yang dibuat setelah setiap pengujian."""
        pass

    @patch('os.path.exists')
    @patch('google.oauth2.service_account.Credentials')
    @patch('googleapiclient.discovery.build')
    @patch('builtins.print')
    def test_export_to_google_sheet_success(self, mock_print, mock_build, mock_credentials_class, mock_os_path_exists):
        """Menguji ekspor ke dalam format Google Sheet yang berhasil."""
        mock_os_path_exists.return_value = True
        mock_credential_instance = Mock()
        mock_credentials_class.from_service_account_file.return_value = mock_credential_instance

        if 'utils.load' in sys.modules:
            del sys.modules['utils.load']
        from utils.load import export_to_google_sheet, SPREADSHEET_ID, RANGE_NAME

        mock_service = Mock()
        mock_sheet = Mock()
        mock_values = Mock()
        mock_build.return_value = mock_service
        mock_service.spreadsheets.return_value = mock_sheet
        mock_sheet.values.return_value = mock_values

        mock_values.get.return_value.execute.return_value = {'values': []}
        mock_values.append.side_effect = [
            Mock(get=Mock(return_value={'updates': {'updatedCells': len(self.df.columns)}})),
            Mock(get=Mock(return_value={'updates': {'updatedCells': len(self.df) * len(self.df.columns)}}))
        ]
        result = export_to_google_sheet(self.df)
        self.assertTrue(result)

        mock_build.assert_called_once_with('sheets', 'v4', credentials=mock_credential_instance)
        mock_sheet.values().get.assert_called_once_with(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME)
        self.assertEqual(mock_sheet.values().append.call_count, 2)
        mock_print.assert_any_call(unittest.mock.ANY)
        self.assertIn('Berhasil mengekspor data ke dalam format Google Sheets.', str(mock_print.call_args_list[-1]))

    @patch('os.path.exists')
    @patch('google.oauth2.service_account.Credentials')
    @patch('googleapiclient.discovery.build')
    @patch('builtins.print')
    def test_export_to_google_sheet_failure(self, mock_print, mock_build, mock_credentials_class, mock_os_path_exists):
        """Menguji kegagalan ekspor ke dalam format Google Sheet."""
        mock_os_path_exists.return_value = True
        mock_credential_instance = Mock()
        mock_credentials_class.from_service_account_file.return_value = mock_credential_instance

        if 'utils.load' in sys.modules:
            del sys.modules['utils.load']
        from utils.load import export_to_google_sheet

        mock_service = Mock()
        mock_sheet = Mock()
        mock_values = Mock()
        mock_build.return_value = mock_service
        mock_service.spreadsheets.return_value = mock_sheet
        mock_sheet.values.return_value = mock_values
        mock_values.get.return_value.execute.return_value = {'values': []}
        mock_values.append.side_effect = Exception('Simulated Google Sheet API error')

        result = export_to_google_sheet(self.df)
        self.assertFalse(result)

        mock_print.assert_any_call(unittest.mock.ANY)
        self.assertIn('Gagal mengekspor data ke dalam format Google Sheets:', str(mock_print.call_args_list[-1]))

    @patch('os.path.exists')
    @patch('google.oauth2.service_account.Credentials')
    @patch('builtins.print')
    def test_export_to_google_sheet_not_available(self, mock_print, mock_credentials_class, mock_os_path_exists):
        """Menguji ketika Google Sheets API tidak tersedia karena file kredensial tidak ditemukan."""
        mock_os_path_exists.return_value = False

        if 'utils.load' in sys.modules:
            del sys.modules['utils.load']
        from utils.load import export_to_google_sheet, google_sheets_available

        result = export_to_google_sheet(self.df)
        self.assertFalse(result)
        mock_print.assert_any_call("Lewati proses ekspor data ke dalam format Google Sheets: Google Sheets API tidak ada atau hilangnya kredensial.")
        self.assertFalse(google_sheets_available)
        mock_credentials_class.from_service_account_file.assert_not_called()

class TestLoadSQLFunctions(unittest.TestCase):

    def setUp(self):
        """Buat sampel DataFrame untuk menguji fungsi load."""
        self.test_data = {
            'Title': ['T-shirt 1', 'Hoodie 3', 'Pants 4'],
            'Price': [1600000.0, 7950080.0, 7476960.0],
            'Rating': [3.9, 4.8, 3.3],
            'Colors': [3, 3, 3],
            'Size': ['M', 'L', 'XL'],
            'Gender': ['Women', 'Unisex', 'Men'],
            'Timestamp': ['2023-01-01 12:00:00', '2023-01-01 12:00:01', '2023-01-01 12:00:02']
        }
        self.df = pd.DataFrame(self.test_data)
        self.db_file = 'test_products.db'
        self.table_name = 'fashion_data'
        self.db_url = f"sqlite:///{self.db_file}"
        self.engine = create_engine(self.db_url)

    def tearDown(self):
        """Bersihkan file apa pun yang dibuat setelah setiap pengujian."""
        self.engine.dispose()
        if os.path.exists(self.db_file):
            try:
                os.remove(self.db_file)
            except PermissionError as e:
                print(f"Peringatan: Tidak dapat memindahkan file database {self.db_file} karena terjadi kesalahan: {e}")

    @patch('builtins.print')
    def test_postgre_export_success(self, mock_print):
        """Menguji ekspor ke dalam format PostgreSQL yang berhasil."""
        if 'utils.load' in sys.modules:
            del sys.modules['utils.load']
        from utils.load import export_to_postgre

        # Ekspor data ke database PostgreSQL
        result = export_to_postgre(self.df, db_url=self.db_url, table_name=self.table_name, engine=self.engine)
        self.assertTrue(result)
        self.assertTrue(os.path.exists(self.db_file))

        # Membaca data dari database
        with self.engine.connect() as conn:
            df_from_db = pd.read_sql_table(self.table_name, conn)

        # Standardisasi dtypes untuk perbandinan yang konsisten
        expected_df = self.df.copy()
        expected_df['Title'] = expected_df['Title'].astype('object')
        expected_df['Price'] = expected_df['Price'].astype('float64')
        expected_df['Rating'] = expected_df['Rating'].astype('float64')
        expected_df['Colors'] = expected_df['Colors'].astype('int64')
        expected_df['Size'] = expected_df['Size'].astype('object')
        expected_df['Gender'] = expected_df['Gender'].astype('object')
        expected_df['Timestamp'] = expected_df['Timestamp'].astype('object')

        df_from_db['Title'] = df_from_db['Title'].astype('object')
        df_from_db['Price'] = df_from_db['Price'].astype('float64')
        df_from_db['Rating'] = df_from_db['Rating'].astype('float64')
        df_from_db['Colors'] = df_from_db['Colors'].astype('int64')
        df_from_db['Size'] = df_from_db['Size'].astype('object')
        df_from_db['Gender'] = df_from_db['Gender'].astype('object')
        df_from_db['Timestamp'] = df_from_db['Timestamp'].astype('object')

        pd.testing.assert_frame_equal(expected_df, df_from_db, check_like=True, check_exact=False, rtol=1e-5, check_index_type=False)
        mock_print.assert_any_call(f"Berhasil mengekspor data ke dalam format PostgreSQL. Tabel: {self.table_name}")

    @patch('pandas.DataFrame.to_sql')
    @patch('builtins.print')
    def test_postgre_export_failure(self, mock_print, mock_to_sql):
        """Menguji kegagalan ekspor ke PostgreSQL (contoh, kesalahan selama to_sql)."""
        if 'utils.load' in sys.modules:
            del sys.modules['utils.load']
        from utils.load import export_to_postgre

        mock_to_sql.side_effect = Exception('Simulated to_sql error')
        result = export_to_postgre(self.df, db_url=self.db_url, table_name=self.table_name, engine=self.engine)
        self.assertFalse(result)

        mock_print.assert_any_call(unittest.mock.ANY)
        self.assertIn('Gagal mengekspor data ke dalam format PostgreSQL:', str(mock_print.call_args_list[-1]))