import numpy as np
import pandas as pd

def transform_data(df_input):
    """Mengubah format data dari hasil proses scraping"""
    df_transformed = df_input.copy()
    df_transformed = df_transformed[df_transformed['Title'] != 'Unknown Product']
    if 'Price' in df_transformed.columns:
        df_transformed['Price'] = pd.to_numeric(df_transformed['Price'].astype(str).str.replace('$', '', regex=False), errors='coerce')
    df_transformed['Rating'] = pd.to_numeric(df_transformed['Rating'], errors='coerce').astype(float)
    df_transformed['Colors'] = pd.to_numeric(df_transformed['Colors'], errors='coerce')
    df_transformed['Colors'] = df_transformed['Colors'].fillna(0).astype(int)
    df_transformed['Size'] = df_transformed['Size'].astype(str)
    df_transformed['Size'] = df_transformed['Size'].replace('nan', np.nan)
    if 'Gender' in df_transformed.columns:
        df_transformed['Gender'] = df_transformed['Gender'].replace('N/A', np.nan)
        df_transformed['Gender'] = df_transformed['Gender'].fillna('Unisex').astype(str)
    else:
        df_transformed['Gender'] = 'Unisex'
    # Hapus NaN dan sesuaikan critical_subset (Title, Price, Size) berdasarkan kolom mata uang yang tersedia
    critical_subset = ['Title', 'Size']
    if 'Price' in df_transformed.columns:
        critical_subset.append('Price')
    elif 'Rupiah' in df_transformed.columns:
        critical_subset.append('Rupiah')
    df_transformed.dropna(subset=critical_subset, inplace=True);
    df_transformed.drop_duplicates(inplace=True);
    return df_transformed

def convert_dollar_to_rupiah(df_input):
    """Mengonversi nilai dollar ke nilai rupiah sebesar Rp 16.000"""
    df_converted = df_input.copy()
    # Tahap 1: Tangani perubahan nama kolom jika 'Price in rupiah' tersedia dari eksekusi sebelumnya
    if 'Price in rupiah' in df_converted.columns:
        df_converted.rename(columns={'Price in rupiah': 'Price'}, inplace=True)
    if 'Price' not in df_converted.columns:
        df_converted['Price'] = np.nan
    try:
        if pd.api.types.is_numeric_dtype(df_converted['Price']):
            if df_converted['Price'].max() < 100000:
                df_converted['Price_in_dollar'] = df_converted['Price']
                exchange_rate = 16000
                df_converted['Price'] = (df_converted['Price_in_dollar'] * exchange_rate).astype(float)
                df_converted.drop(columns=['Price_in_dollar'], inplace=True)
            else:
                df_converted['Price'] = df_converted['Price'].astype(float)
    except Exception as e:
        print(f"Terjadi kesalahan saat mengonversi ke rupiah: {e}")
        df_converted['Price'] = np.nan

    # Tahap 2: Susun kembali kolom sesuai format yang diinginkan
    final_column_order_base = ['Title', 'Price', 'Rating', 'Colors', 'Size', 'Gender']
    # Buat urutan aktual sesuai di df_converted dan urutan dasar yang diinginkan
    ordered_columns = []
    for col in final_column_order_base:
        if col in df_converted.columns:
            ordered_columns.append(col)
    # Menambahkan kolom yang tidak tercatat di akhir secara eksplisit
    for col in df_converted.columns:
        if col not in ordered_columns:
            ordered_columns.append(col)
    df_converted = df_converted[ordered_columns]
    return df_converted

if __name__ == "__main__":
    final_transformed_df = pd.DataFrame(columns=['Title', 'Price', 'Rating', 'Colors', 'Size', 'Gender'])
    if 'final_df' not in globals() or not isinstance(final_df, pd.DataFrame) or final_df.empty: # Cakupan: Uji kedua cabang true dan false
        print("Tidak tersedia data untuk proses transform, jalankan tahapan scraping terlebih dahulu.")
    else:
        df = final_df.copy()
        transformed_df = transform_data(df)
        final_transformed_df = convert_dollar_to_rupiah(transformed_df)
        print("Head DataFrame yang diubah:")
        # Kecualikan kolom 'Timestamp' pada tampilan head dan verifikasi tipe data
        print(final_transformed_df.drop(columns=['Timestamp'], errors='ignore').head())
        print(f"Total produk setelah proses transform: {len(final_transformed_df)}")
        print("\nTipe data DataFrame setelah proses transform:")
        print(final_transformed_df.dtypes.drop('Timestamp', errors='ignore'))

    final_df = final_transformed_df