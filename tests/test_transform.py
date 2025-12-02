import coverage
import numpy as np
import pandas as pd
import pytest
import re
import sys
import unittest
from unittest.mock import patch, Mock

if 'utils.transform' in sys.modules:
    del sys.modules['utils.transform']
try:
    from utils.transform import transform_data, convert_dollar_to_rupiah
except ImportError as e:
    print(f"Error: Gagal mengimpor fungsi transform dari utils.transform, pastikan transform.py tersedia dan berada di PYTHONPATH. Error: {e}")

class TestTransformFunctions(unittest.TestCase):

    def setUp(self):
        """Buat sampel DataFrame untuk menguji fungsi transform."""
        self.sample_data = {
            'Title': ['T-shirt 1', 'Unknown Product', 'Hoodie 3', 'Pants 4', 'Outerwear 5', 'Shirt 6', 'Invalid Price Item'],
            'Price': ['$100.00', '$0.00', '$496.88', '467.31', '$321.59', 'Invalid Price', '$50.00'],
            'Rating': ['3.9', 'N/A', '4.8', '3.3', '3.5', 'N/A', '4.0'],
            'Colors': ['3', '0', '3', '3', '3', 'N/A', '2'],
            'Size': ['M', 'N/A', 'L', 'XL', 'XXL', 'S', 'M'],
            'Gender': ['Women', 'N/A', 'Unisex', 'Men', 'Women', 'Unisex', 'Men'],
            'Timestamp': [f'2023-01-01 12:00:0{i}' for i in range(7)]
        }
        self.df = pd.DataFrame(self.sample_data)

    def tearDown(self):
        """Bersihkan file apa pun yang dibuat setelah setiap pengujian."""
        pass

    def test_transform_data_functionality(self):
        """Menguji fungsionalitas inti dari transform_data."""
        final_transformed_df = transform_data(self.df.copy())

        # 1. Periksa baris 'Unknown Product' dan 'Shirt 6' telah dihapus
        self.assertNotIn('Unknown Product', final_transformed_df['Title'].values)
        self.assertNotIn('Shirt 6', final_transformed_df['Title'].values)
        self.assertEqual(len(final_transformed_df), 5)

        # 2. Periksa tipe dan nilai kolom 'Price' untuk baris valid yang tersisa
        self.assertTrue(pd.api.types.is_float_dtype(final_transformed_df['Price']))
        self.assertFalse(final_transformed_df['Price'].isnull().any())
        self.assertEqual(final_transformed_df.loc[final_transformed_df['Title'] == 'T-shirt 1', 'Price'].iloc[0], 100.00)
        self.assertEqual(final_transformed_df.loc[final_transformed_df['Title'] == 'Invalid Price Item', 'Price'].iloc[0], 50.00)

        # 3. Periksa tipe kolom 'Rating' dan pastikan tidak ada NaN setelah proses transform dan drop
        self.assertTrue(pd.api.types.is_float_dtype(final_transformed_df['Rating']))
        self.assertFalse(final_transformed_df['Rating'].isnull().any())

        # 4. Periksa tipe kolom 'Colors' dan pastikan tidak ada NaN setelah proses transform dan drop
        self.assertTrue(pd.api.types.is_integer_dtype(final_transformed_df['Colors']))
        self.assertFalse(final_transformed_df['Colors'].isnull().any())
        self.assertEqual(final_transformed_df.loc[final_transformed_df['Title'] == 'Invalid Price Item', 'Colors'].iloc[0], 2)

        # 5. Periksa tipe kolom 'Size' dan pastikan tidak ada NaN setelah proses transform dan drop
        self.assertTrue(pd.api.types.is_object_dtype(final_transformed_df['Size']))
        self.assertFalse(final_transformed_df['Size'].isnull().any())

        # 6. Periksa tipe kolom 'Gender' dan pastikan tidak ada NaN setelah proses transform dan drop
        self.assertTrue(pd.api.types.is_object_dtype(final_transformed_df['Gender']))
        self.assertFalse(final_transformed_df['Gender'].isnull().any())
        self.assertEqual(final_transformed_df.loc[final_transformed_df['Title'] == 'T-shirt 1', 'Gender'].iloc[0], 'Women')
        self.assertEqual(final_transformed_df.loc[final_transformed_df['Title'] == 'Hoodie 3', 'Gender'].iloc[0], 'Unisex')

        # 7. Periksa drop duplikat
        df_with_duplicates = pd.DataFrame({
            'Title': ['Item A', 'Item A'],
            'Price': ['$10.00', '$10.00'],
            'Rating': ['4.0', '4.0'],
            'Colors': ['1', '1'],
            'Size': ['S', 'S'],
            'Gender': ['Unisex', 'Unisex'],
            'Timestamp': ['2023-01-01 12:00:00', '2023-01-01 12:00:00']
        })
        transformed_duplicates_df = transform_data(df_with_duplicates.copy())
        self.assertEqual(len(transformed_duplicates_df), 1)

    def test_transform_data_empty_dataframe(self):
        """Menguji transform_data dengan DataFrame kosong."""
        empty_df = pd.DataFrame(columns=self.df.columns)
        transformed_empty_df = transform_data(empty_df.copy())
        self.assertTrue(transformed_empty_df.empty)
        self.assertIn('Title', transformed_empty_df.columns)
        self.assertIn('Price', transformed_empty_df.columns)
        self.assertIn('Rating', transformed_empty_df.columns)

    def test_transform_data_no_gender_column(self):
        """Menguji transform_data ketika DataFrame input tidak tersedia kolom 'Gender'."""
        df_no_gender = self.df.drop(columns=['Gender'])
        transformed_df = transform_data(df_no_gender.copy())
        self.assertIn('Gender', transformed_df.columns)
        self.assertTrue((transformed_df['Gender'] == 'Unisex').all())
        self.assertFalse(transformed_df['Gender'].isnull().any())

    def test_transform_data_with_rupiah_column(self):
        """Menguji transform_data ketika kolom 'Rupiah' tersedia sebagai pengganti 'Price'."""
        df_with_rupiah = pd.DataFrame({
            'Title': ['Rupiah Item'],
            'Rupiah': [160000.00],
            'Rating': ['4.0'],
            'Colors': ['1'],
            'Size': ['M'],
            'Gender': ['Unisex'],
            'Timestamp': ['2023-01-01 12:00:00']
        })
        transformed_df = transform_data(df_with_rupiah.copy())
        self.assertFalse(transformed_df.empty)
        self.assertFalse(transformed_df['Rupiah'].isnull().any())

    def test_convert_dollar_to_rupiah_functionality(self):
        """Menguji fungsi convert_dollar_to_rupiah."""
        df_for_conversion = transform_data(self.df.copy())
        original_dollar_prices = df_for_conversion['Price'].copy()
        converted_df = convert_dollar_to_rupiah(df_for_conversion.copy())

        # 1. Periksa tipe kolom 'Price' setelah konversi
        self.assertTrue(pd.api.types.is_float_dtype(converted_df['Price']))

        # 2. Periksa logika konversi
        exchange_rate = 16000
        tshirt_original_price = original_dollar_prices.loc[df_for_conversion['Title'] == 'T-shirt 1'].iloc[0]
        expected_rupiah_price = tshirt_original_price * exchange_rate
        actual_rupiah_price = converted_df.loc[converted_df['Title'] == 'T-shirt 1', 'Price'].iloc[0]
        self.assertAlmostEqual(actual_rupiah_price, expected_rupiah_price, places=2)

        # 3. Pastikan Price_in_dollar dihapus jika dibuat sementara
        self.assertNotIn('Price_in_dollar', converted_df.columns)

        # 4. Periksa urutan kolom
        self.assertEqual(list(converted_df.columns[:6]), ['Title', 'Price', 'Rating', 'Colors', 'Size', 'Gender'])

    def test_convert_dollar_to_rupiah_existing_rupiah_price(self):
        """Menguji convert_dollar_to_rupiah ketika harga sudah dalam kisaran Rupiah (>100000)."""
        df_high_price = pd.DataFrame({
            'Title': ['High Price Item'],
            'Price': [500000.00],
            'Rating': ['4.5'],
            'Colors': ['1'],
            'Size': ['L'],
            'Gender': ['Men'],
            'Timestamp': ['2023-01-01 12:00:00']
        })
        transformed_df = transform_data(df_high_price.copy())
        converted_df = convert_dollar_to_rupiah(transformed_df.copy())
        self.assertEqual(converted_df.loc[0, 'Price'], 500000.00)
        self.assertNotIn('Price_in_dollar', converted_df.columns)

    def test_convert_dollar_to_rupiah_price_in_rupiah_column_rename(self):
        """Menguji convert_dollar_to_rupiah ketika kolom 'Price in rupiah' tersedia dan diubah namanya."""
        df_with_old_name = pd.DataFrame({
            'Title': ['Old Price Name Item'],
            'Price in rupiah': [100000.00],
            'Rating': ['3.8'],
            'Colors': ['2'],
            'Size': ['M'],
            'Gender': ['Unisex'],
            'Timestamp': ['2023-01-01 12:00:00']
        })
        converted_df = convert_dollar_to_rupiah(df_with_old_name.copy())
        self.assertIn('Price', converted_df.columns)
        self.assertNotIn('Price in rupiah', converted_df.columns)
        self.assertEqual(converted_df.loc[0, 'Price'], 100000.00)

    @patch('builtins.print')
    def test_convert_dollar_to_rupiah_error_handling(self, mock_print):
        """Menguji penanganan kesalahan pada convert_dollar_to_rupiah."""
        # Buat DataFrame di mana 'Price' bukan numerik untuk memicu konversi harga yang tidak valid
        df_non_numeric_price = pd.DataFrame({
            'Title': ['Test Item'],
            'Price': ['Not a number'],
            'Rating': ['4.0'],
            'Colors': ['1'],
            'Size': ['M'],
            'Gender': ['Unisex'],
            'Timestamp': ['2023-01-01 12:00:00']
        })
        transformed_df = transform_data(df_non_numeric_price.copy())
        self.assertTrue(transformed_df.empty)
        converted_df = convert_dollar_to_rupiah(transformed_df.copy())
        self.assertTrue(converted_df.empty)
        mock_print.assert_not_called()

    @patch('builtins.print')
    @patch('pandas.Series.max', side_effect=TypeError("Simulated max() error"))
    def test_convert_dollar_to_rupiah_conversion_error(self, mock_series_max, mock_print):
        """Menguji `convert_dollar_to_rupiah` ketika terjadi kesalahan selama konversi numerik."""
        df_valid_input = pd.DataFrame({
            'Title': ['Item A'],
            'Price': [100.00],
            'Rating': [4.0],
            'Colors': [1],
            'Size': ['M'],
            'Gender': ['Unisex'],
            'Timestamp': ['2023-01-01 12:00:00']
        })
        converted_df = convert_dollar_to_rupiah(df_valid_input.copy())
        mock_print.assert_called_with('Terjadi kesalahan saat mengonversi ke rupiah: Simulated max() error')
        self.assertIn('Price', converted_df.columns)
        self.assertTrue(pd.isna(converted_df['Price'].iloc[0]))

    def test_convert_dollar_to_rupiah_column_reordering(self):
        """Menguji `convert_dollar_to_rupiah` dengan memastikan urutan kolom tertentu dan buat kolom lainnya."""
        df_minimal = pd.DataFrame({
            'Title': ['A'], 'Price': [10.0], 'Rating': [3.0],
            'Colors': [1], 'Size': ['S'], 'Gender': ['Unisex'],
            'Timestamp': ['2023-01-01 12:00:00']
        })
        transformed_df = transform_data(df_minimal.copy())
        converted_df = convert_dollar_to_rupiah(transformed_df.copy())
        self.assertEqual(list(converted_df.columns), ['Title', 'Price', 'Rating', 'Colors', 'Size', 'Gender', 'Timestamp'])

        df_with_extra = pd.DataFrame({
            'Title': ['B'], 'Price': [20.0], 'Rating': [4.0],
            'Colors': [2], 'Size': ['M'], 'Gender': ['Men'],
            'ExtraCol': ['Value'], 'Timestamp': ['2023-01-01 12:00:01']
        })
        transformed_df_extra = transform_data(df_with_extra.copy())
        converted_df_extra = convert_dollar_to_rupiah(transformed_df_extra.copy())
        self.assertEqual(list(converted_df_extra.columns), ['Title', 'Price', 'Rating', 'Colors', 'Size', 'Gender', 'ExtraCol', 'Timestamp'])