from modules.etl_pipeline import fetch_kaggle_data, preprocess_data

def main():
    fetch_kaggle_data('rohitsahoo/sales-forecasting')
    df = preprocess_data('./data/train.csv')
    if df is not None: print(df.head())

if __name__ == '__main__':
    main()