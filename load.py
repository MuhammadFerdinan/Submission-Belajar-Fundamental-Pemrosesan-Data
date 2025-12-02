import csv
import os
import pandas as pd

try:
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build

    SERVICE_ACCOUNT_FILE = 'google-sheets-api.json'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    # Buatkan kredensial jika file tersedia
    if os.path.exists(SERVICE_ACCOUNT_FILE):
        credential = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        google_sheets_available = True
    else:
        print(f"Peringatan: '{SERVICE_ACCOUNT_FILE}' tidak ditemukan, gagal melakukan ekspor ke Google Sheets.")
        credential = None
        google_sheets_available = False

    SPREADSHEET_ID = '1TI_Tx-X3I-ruwbepiE_KeUbj4YSLktWH7lu_Nlrldaw'
    RANGE_NAME = 'Sheet1'

except ImportError:
    print("Peringatan: library klien Google API (google-auth, google-api-python-client) tidak terinstal, gagal mengekspor data ke Google Sheets.")
    credential = None
    google_sheets_available = False
except Exception as e:
    print(f"Terjadi kesalahan saat inisiasi API Google Sheets: {e}. tidak tersedia ekspor data ke Google Sheets.")
    credential = None
    google_sheets_available = False

def export_to_csv(df, filename='products.csv'):
    """Mengekspor data ke csv"""
    try:
        print(f"Mulai mengekspor data ke dalam format CSV: {filename}")
        df.to_csv(filename, index=False)
        print(f"Berhasil mengekspor data ke dalam format CSV: {filename}")
        return True
    except Exception as e:
        print(f"Gagal mengekspor data ke dalam format CSV: {e}")
        return False

def export_to_google_sheet(df, spreadsheet_id=SPREADSHEET_ID, range_name=RANGE_NAME):
    """Mengekspor data ke Google Sheets dengan nilai-nilai dari DataFrame.
    Buat header terlebih dahulu jika sheet kosong, lalu tambahkan baris data."""
    global google_sheets_available

    if not google_sheets_available or credential is None:
        print("Lewati proses ekspor data ke dalam format Google Sheets: Google Sheets API tidak ada atau hilangnya kredensial.")
        return False
    try:
        print(f"Mulai mengekspor data ke dalam format Google Sheets (ID: {spreadsheet_id}, Range: {range_name})")
        service = build('sheets', 'v4', credentials=credential)
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
        existing_values = result.get('values', [])
        headers = df.columns.tolist()
        values = df.values.tolist()
        # Jika tidak tersedia nilai, buat header terlebih dahulu
        if not existing_values:
            print("Buat header ke dalam format Google Sheets.")
            body = {
                'values': [headers]
            }
            sheet.values().append(
                spreadsheetId=spreadsheet_id,
                range=range_name,
                valueInputOption='RAW',
                body=body
            ).execute()
        print("Mengisi nilai data ke dalam format Google Sheets.")
        body = {
            'values': values
        }
        result = sheet.values().append(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()
        print(f"Berhasil mengekspor data ke dalam format Google Sheets. {result.get('updates').get('updatedCells')} sel diperbarui.")
        return True
    except Exception as e:
        print(f"Gagal mengekspor data ke dalam format Google Sheets: {e}")
        return False

def export_to_postgre(df, db_url, table_name='fashion_data', engine=None):
    """Mengekspor data ke dalam format PostgreSQL"""
    from sqlalchemy import create_engine
    try:
        print(f"Mulai mengekspor data ke dalam format PostgreSQL. Tabel: {table_name}")
        if engine is None:
            engine = create_engine(db_url)
        with engine.connect() as con:
            df.to_sql(table_name, con=con, if_exists='append', index=False)
        print(f"Berhasil mengekspor data ke dalam format PostgreSQL. Tabel: {table_name}")
        return True
    except Exception as e:
        print(f"Gagal mengekspor data ke dalam format PostgreSQL: {e}")
        return False

if __name__ == "__main__":
    if 'final_df' in globals() and isinstance(final_df, pd.DataFrame) and not final_df.empty:
        print("DataFrame akhir tersedia, lanjutkan dengan opsi ekspor.")
    else:
        print("DataFrame 'final_df' tidak tersedia atau kosong, pastikan tahapan sebelumnya berhasil dijalankan.")