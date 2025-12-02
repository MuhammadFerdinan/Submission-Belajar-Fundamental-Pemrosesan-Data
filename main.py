import importlib
import os
import pandas as pd
import sys
from datetime import datetime

current_dir = os.getcwd()
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

print(f"Direktori kerja sekarang: {current_dir}")
print(f"sys.path: {sys.path}")
print(f"File dalam direktori sekarang: {os.listdir('.')}")

# Mengimpor fungsi extract, transform, dan load dengan penanganan kesalahan
try:
    for module_name in ['utils.extract', 'utils.transform', 'utils.load']:
        if module_name in sys.modules:
            print(f"Menghapus modul {module_name} dari sys.modules cache.")
            del sys.modules[module_name]

    import utils.extract
    importlib.reload(utils.extract)
    from utils.extract import main as extract_main_function

    import utils.transform
    importlib.reload(utils.transform)
    from utils.transform import transform_data, convert_dollar_to_rupiah

    import utils.load
    importlib.reload(utils.load)
    from utils.load import export_to_csv, export_to_google_sheet, export_to_postgre

except ImportError as e:
    print(f"CRITICAL ERROR mengimpor komponen ETL: {e}.")
    print("Pastikan extract.py, transform.py, and load.py terdeifnisikan dengan benar dan berada di alur Python.")
    print(f"sys.path sekarang: {sys.path}")
    print(f"File dalam direktori sekarang: {os.listdir('.')}")
    # Gunakan fungsi dummy untuk mencegah NameError jika impor gagal
    def extract_main_function(delay=0): return pd.DataFrame(columns=['Title', 'Price', 'Rating', 'Colors', 'Size', 'Gender', 'Timestamp'])
    def transform_data(df): return df
    def convert_dollar_to_rupiah(df): return df
    def export_to_csv(df, filename): print(f"Dummy export_to_csv untuk {filename}")
    def export_to_google_sheet(df): print(f"Dummy export_to_google_sheet untuk Google Sheets")
    def export_to_postgre(df, db_url, table_name): print(f"Dummy export_to_postgre untuk {db_url}")


if __name__ == "__main__":
    print(f"Proses ETL dimulai pada: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    start_time_total = datetime.now()

    # 1. Tahap Extract
    print("\nMemulai proses extract...")
    extracted_df = extract_main_function(delay=0)
    print(f"Proses extract selesai dengan jumlah baris: {len(extracted_df)}")

    # 2. Tahap Transform
    print("\nMemulai proses transform...")
    if not extracted_df.empty:
        transformed_df = transform_data(extracted_df)
        final_df_for_load = convert_dollar_to_rupiah(transformed_df)
        print("Tampilan Head DataFrame setelah proses transform:")
        print(final_df_for_load.head())
        print(f"Proses transform selesai dengan jumlah baris: {len(final_df_for_load)}")
    else:
        final_df_for_load = pd.DataFrame(columns=['Title', 'Price', 'Rating', 'Colors', 'Size', 'Gender', 'Timestamp'])
        print("Tidak ada DataFrame setelah ekstraksi, gagal melakukan proses transform.")

    # 3. Load
    print("\nMemulai proses load...")
    if not final_df_for_load.empty:
        print("DataFrame tersedia untuk proses load dengan melakukan 'export'.")
        export_to_csv(final_df_for_load, 'products.csv')
        export_to_google_sheet(final_df_for_load)
        export_to_postgre(final_df_for_load, 'postgresql+psycopg2://ferdimuh:Ferdinan691*@localhost:5432/fashion_db', table_name='fashion_products')
        print("Load data lengkap untuk semua format.")
    else:
        print("DataFrame tidak tersedia untuk proses load.")

    end_time_total = datetime.now()
    total_time_total = end_time_total - start_time_total
    print(f"\nProses ETL selesai pada: {end_time_total.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total waktu ETL: {total_time_total}")
