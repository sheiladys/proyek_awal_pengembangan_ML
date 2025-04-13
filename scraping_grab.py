import pandas as pd
import time
import datetime
from google_play_scraper import Sort, reviews

def scrape_grab_reviews(count=50000, lang='id', country='id', sort=Sort.NEWEST):
    print(f"Memulai scraping {count} ulasan Grab...")
    app_id = 'com.grabtaxi.passenger'  # ID untuk aplikasi Grab
    all_reviews = []
    continuation_token = None

    while len(all_reviews) < count:
        batch_size = min(10000, count - len(all_reviews))  # Ambil ulasan per batch 100
        try:
            print(f"Mengambil {batch_size} ulasan...")
            result, continuation_token = reviews(
                app_id,
                lang=lang,
                country=country,
                sort=sort,
                count=batch_size,
                continuation_token=continuation_token
            )
            if not result:
                print("Tidak ada data ulasan yang didapat.")
                break

            all_reviews.extend(result)
            print(f"Berhasil mengambil {len(all_reviews)} dari {count} ulasan...")

            if continuation_token is None:
                print("Tidak ada lagi ulasan yang tersedia.")
                break

            time.sleep(1)

        except Exception as e:
            print(f"Terjadi error: {e}")
            time.sleep(5)

    df = pd.DataFrame(all_reviews)
    column_mapping = {
        'content': 'content',
        'score': 'score',
        'reviewId': 'reviewId',
        'userName': 'userName',
        'thumbsUpCount': 'thumbsUpCount',
        'reviewCreatedVersion': 'appVersion',
        'at': 'reviewDate'
    }
    valid_columns = {k: v for k, v in column_mapping.items() if k in df.columns}
    df = df.rename(columns=valid_columns)

    if 'reviewDate' in df.columns:
        df['reviewDate'] = pd.to_datetime(df['reviewDate']).dt.strftime('%Y-%m-%d %H:%M:%S')

    print(f"Scraping selesai. Total {len(df)} ulasan berhasil diambil.")
    return df

def main():
    review_count = 50000  
    df_reviews = scrape_grab_reviews(count=review_count)

    df_reviews = df_reviews.reset_index(drop=True)

    # Simpan ke file CSV
    timestamp = datetime.datetime.now().strftime("%Y%m%d")
    csv_filename = f'grab_reviews.csv'
    try:
        df_reviews.to_csv(csv_filename, index=False, encoding='utf-8-sig')
        print(f"\nData berhasil disimpan ke {csv_filename}")
    except Exception as e:
        print(f"Gagal menyimpan file CSV: {e}")

if __name__ == "__main__":
    main()
