# Description: Script para coleta, transformacao e carregamento de dados
# Author: Igor Miranda Eisenlohr

# Importacao das bibliotecas
import pandas as pd
import pandas_gbq
import numpy as np
import investpy
import yfinance as yf
import os
from google.cloud import storage
from google.oauth2 import service_account

# Classe para ETL
class DataCollector:
    def __init__(self, bucket_name):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'sprint3-storage.json' # Credenciais do Google Cloud
        self.bucket_name = bucket_name # Nome do bucket
        self.storage_client = storage.Client() # Cliente do Google Cloud
        self.bucket = self.storage_client.bucket(bucket_name) # Bucket

    def get_cdi(self):
        url_cdi  = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados?formato=json".format(12) # Codigo do CDI
        cdi = pd.read_json(url_cdi) # Leitura do JSON
        cdi['data'] = pd.to_datetime(cdi['data'], format='%d/%m/%Y')
        cdi['ano'] = cdi['data'].dt.year # Criacao da coluna ano
        cdi.rename(columns={'data': 'date'}, inplace=True) # Renomeacao da coluna data para Date
        cdi['ticker'] = 'CDI' # Criacao da coluna Ticker
        cdi['daily_variation'] = cdi['valor'] # Criacao da coluna Close
        cdi = cdi.query('ano >= 2022') # Filtragem dos dados a partir de 2021
        cdi_df = cdi[['date', 'ticker','daily_variation']] # Selecao das colunas

        return cdi_df # Retorno do dataframe

    def get_stocks(self):
        acoes_df = investpy.stocks.get_stocks(country='brazil') # Coleta de dados de acoes brasileiras
        acoes_df['symbol'] = acoes_df['symbol'].apply(lambda x: x+'.SA') # Adicao do sufixo .SA para acoes brasileiras
        acoes_df.rename(columns={'symbol': 'ticker'}, inplace=True) # Renomeacao da coluna symbol para Ticker
        return acoes_df # Retorno do dataframe

    def get_stocks_info(self, acoes_df):
        tickers = acoes_df['ticker'] # Lista de tickers
        tickers_info_list = [] # Lista vazia para armazenar os dados
        for ticker in tickers: # Loop para coletar os dados de cada ticker
            try:
                tickers_info_list.append(yf.Ticker(ticker).info ) # Coleta dos dados
                tickers_info_df = pd.DataFrame(tickers_info_list) # Transformacao da lista em dataframe
            except:
                print(f"Ticker não possui informações disponiveis: {ticker}") # Caso o ticker nao exista, printa o ticker
        tickers_info_df.rename(columns={'symbol': 'ticker'}, inplace=True) # Renomeacao da coluna symbol para Ticker
        return tickers_info_df[['city', 'state', 'country', 'industry', 'sector', 'ticker']] # Retorno do dataframe

    def get_stocks_historic(self, tickers):
        historicos_list = []  # Lista vazia para armazenar os dados
        for ticker in tickers:  # Loop para coletar os dados de cada ticker
            historico = yf.download(ticker, start="2022-01-03")  # Coleta dos dados
            historico['Ticker'] = ticker  # Adicao da coluna Ticker
            historicos_list.append(historico)  # Adicao do dataframe na lista
        historicos_df = pd.concat(historicos_list) # Transformacao da lista em dataframe
        historicos_df.reset_index(inplace=True) # Reset do index
        historicos_df.rename(columns=str.lower, inplace=True) # Renomeacao das colunas
        return historicos_df[['date', 'ticker','close', 'volume']]  # Retorno do dataframe

    def to_google_storage(self, *dataframes, func_names): 
        for df, func_name in zip(dataframes, func_names):  # Loop para salvar os dataframes
            file_name = f'df_{func_name}.csv'  # Nome do arquivo
            df.to_csv(file_name, index=False)  # Salvando o dataframe
            blob = self.bucket.blob(file_name)  # Blob
            blob.upload_from_filename(file_name)  # Upload do arquivo no bucket

    def extract(self):
        print("EXTRACTING DATA...")
        cdi_df = self.get_cdi()
        stocks_df = self.get_stocks() 
        stocks_info_df = self.get_stocks_info(stocks_df)
        stocks_historic_df = self.get_stocks_historic(stocks_df['ticker'].tolist())

        func_names = ['get_cdi', 'get_stocks', 'get_stocks_info', 'get_stocks_historic'] # Lista com os nomes das funcoes
        self.to_google_storage(cdi_df, stocks_df, stocks_info_df, stocks_historic_df, func_names=func_names) # Salvando os dataframes no bucket
        print("DATA EXTRACTED! FILES ON GOOGLE STORAGE!")

    def transform(self, cdi_file, stocks_file, stocks_info_file, stocks_historic_file):
        print("TRANSFORMING DATA...")
        cdi_df = pd.read_csv(cdi_file)
        cadastro_df = pd.read_csv(stocks_file)
        info_df = pd.read_csv(stocks_info_file)
        historic_df = pd.read_csv(stocks_historic_file)

        duplicate_symbols = cadastro_df[cadastro_df.duplicated(subset='ticker', keep=False)] # Verificacao de duplicatas
        if not duplicate_symbols.empty: # Caso existam duplicatas
            print(f'Duplicatas encontradas no cadastro: {duplicate_symbols["ticker"].tolist()}') # Printa os tickers duplicados
        cadastro_cleaned_df = cadastro_df.drop_duplicates(subset='ticker', keep='first') # Remocao das duplicatas

        cadastro_df = pd.merge(cadastro_cleaned_df, info_df, on='ticker', how='left') # Merge dos dataframes de cadastro e informações
        cadastro_df_cleaned = cadastro_df.drop(columns='country_y').rename(columns={'country_x': 'country'}) # Renomeacao da coluna country

        cdi_df['date'] = pd.to_datetime(cdi_df['date']).dt.strftime('%Y-%m-%d')

        prices_df = pd.concat([historic_df, cdi_df], ignore_index=True) # Uniao dos dataframes de historico de stocks e CDI
        duplicate_rows = prices_df[prices_df.duplicated(subset=['date', 'ticker'], keep=False)]
        if not duplicate_rows.empty:
            print(f'Duplicatas encontradas na tabela de preços: {duplicate_rows[["date", "ticker"]].to_dict(orient="records")}') # Printa as duplicatas
        prices_df_cleaned = prices_df.drop_duplicates(subset=['date', 'ticker'], keep='first') # Remocao das duplicatas

        prices_df_cleaned = prices_df_cleaned.sort_values(by=['ticker', 'date'], ascending=True)  # Ordenacao do dataframe
        prices_df_cleaned['prev_close'] = prices_df_cleaned.groupby('ticker')['close'].shift(1) # Criacao da coluna Prev_Close
        prices_df_cleaned['daily_factor'] = (prices_df_cleaned['close'] / prices_df_cleaned['prev_close']).fillna(1) # Criacao da coluna Daily_Factor

        prices_df_cleaned['date'] = pd.to_datetime(prices_df_cleaned['date']) # Conversao da coluna Date para datetime
        prices_df_cleaned['year'] = prices_df_cleaned['date'].dt.year # Criacao da coluna Year
        prices_df_cleaned['month'] = prices_df_cleaned['date'].dt.month # Criacao da coluna Month
        prices_df_cleaned['day'] = prices_df_cleaned['date'].dt.day # Criacao da coluna Day

        is_not_cdi = prices_df_cleaned['ticker'] != 'CDI' # Filtro para selecionar apenas os tickers que nao sao CDI
        prices_df_cleaned.loc[is_not_cdi, 'daily_variation'] = prices_df_cleaned.loc[is_not_cdi].groupby('ticker')['close'].transform(lambda x: x / x.shift(1) - 1) # Calculando a variacao diaria

        prices_df_cleaned['month_accumulated_variation'] = prices_df_cleaned.groupby(['ticker', 'year', 'month'])['daily_variation'].transform(lambda x: (x + 1).cumprod() - 1) * 100 # Calculando a variação acumulada mensal
        prices_df_cleaned['year_accumulated_variation'] = prices_df_cleaned.groupby(['ticker', 'year'])['daily_variation'].transform(lambda x: (x + 1).cumprod() - 1) * 100 # Calculando a variação acumulada anual
         
        prices_df_cleaned['date'] = prices_df_cleaned['date'].astype(str)  # Conversao da coluna Date para string

        print("DATA TRANSFORMED!")

        return cadastro_df_cleaned, prices_df_cleaned
    
    def load(self, cadastro_info_df, prices_df, dataset_name, cadastro_table_name, prices_table_name):
        print("LOADING DATA...")

        credentials = service_account.Credentials.from_service_account_file(
            'sprint3-storage.json'
        ) # Credenciais do Google Cloud

        # Carregar o dataframe cadastro_info_df no BigQuery
        pandas_gbq.to_gbq(
            cadastro_info_df,
            f"{dataset_name}.{cadastro_table_name}",
            project_id='bigquery-sandbox-385813',
            if_exists='replace',
            credentials=credentials
        )

        # Carregar o dataframe prices_df no BigQuery
        pandas_gbq.to_gbq(
            prices_df,
            f"{dataset_name}.{prices_table_name}",
            project_id='bigquery-sandbox-385813',
            if_exists='replace',
            credentials=credentials
        )

        print("DATA LOADED!")

if __name__ == "__main__":
    data_collector = DataCollector('sprint3-storage') # Nome do bucket
    data_collector.extract() # Execucao do script de extracao

    cdi_file = './df_get_cdi.csv'
    stocks_file = './df_get_stocks.csv'
    stocks_info_file = './df_get_stocks_info.csv'
    stocks_historic_file = './df_get_stocks_historic.csv'

    cadastro_info_df, prices_df = data_collector.transform(cdi_file, stocks_file, stocks_info_file, stocks_historic_file) # Execucao do script de transformacao
    data_collector.load(cadastro_info_df, prices_df, 'sprint3', 'cadastro', 'prices') # Execucao do script de carregamento