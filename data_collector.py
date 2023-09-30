import pandas as pd
import investpy
import yfinance as yf
import os
from google.cloud import storage

class DataCollector:
    def __init__(self, bucket_name):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'sprint3-storage.json' # Credenciais do Google Cloud
        self.bucket_name = bucket_name # Nome do bucket
        self.storage_client = storage.Client() # Cliente do Google Cloud
        self.bucket = self.storage_client.bucket(bucket_name) # Bucket

    def get_cdi(self):
        url_cdi  = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados?formato=json".format(12) # Codigo do CDI
        cdi = pd.read_json(url_cdi) # Leitura do JSON
        cdi['data'] = pd.to_datetime(cdi['data'], format='%d/%m/%Y') # Conversao da coluna data para datetime
        cdi['ano'] = cdi['data'].dt.year # Criacao da coluna ano
        cdi.rename(columns={'data': 'Date'}, inplace=True) # Renomeacao da coluna data para Date
        cdi['Ticker'] = 'CDI' # Criacao da coluna Ticker
        cdi['Close'] = cdi['valor'] # Criacao da coluna Close
        cdi = cdi.query('ano >= 2021') # Filtragem dos dados a partir de 2021
        cdi_df = cdi[['Date', 'Ticker','Close']] # Selecao das colunas

        return cdi_df # Retorno do dataframe

    def get_stocks(self):
        acoes_df = investpy.stocks.get_stocks(country='brazil') # Coleta de dados de acoes brasileiras
        acoes_df['symbol'] = acoes_df['symbol'].apply(lambda x: x+'.SA') # Adicao do sufixo .SA para acoes brasileiras
        return acoes_df # Retorno do dataframe

    def get_stocks_info(self, acoes_df):
        tickers = acoes_df['symbol'] # Lista de tickers

        tickers_info_list = [] # Lista vazia para armazenar os dados
        for ticker in tickers: # Loop para coletar os dados de cada ticker
            try:
                tickers_info_list.append(yf.Ticker(ticker).info ) # Coleta dos dados
                tickers_info_df = pd.DataFrame(tickers_info_list) # Transformacao da lista em dataframe
            except:
                print(ticker) # Caso o ticker nao exista, printa o ticker
        return tickers_info_df[['city', 'state', 'country', 'industry', 'sector', 'symbol']] # Retorno do dataframe

    def get_stocks_historic(self, tickers):
        historicos_list = [] # Lista vazia para armazenar os dados
        for ticker in tickers: # Loop para coletar os dados de cada ticker
            historico = yf.Ticker(ticker).history(period='2y') # Coleta dos dados
            historico['Ticker'] = ticker # Adicao da coluna Ticker
            historicos_list.append(historico) # Adicao do dataframe na lista

        historicos_df = pd.concat(historicos_list, axis=0).reset_index() # Concatenacao dos dataframes
        return historicos_df # Retorno do dataframe

    def to_google_storage(self, *dataframes): 
        for i, df in enumerate(dataframes, start=1): # Loop para salvar os dataframes
            file_name = f'dataframe_{i}.csv' # Nome do arquivo
            df.to_csv(file_name, index=False) # Salvando o dataframe
            blob = self.bucket.blob(file_name) # Blob
            blob.upload_from_filename(file_name) # Upload do arquivo no bucket

    def run(self):
        cdi_df = self.get_cdi()
        stocks_df = self.get_stocks() 
        stocks_info_df = self.get_stocks_info(stocks_df)
        stocks_historic_df = self.get_stocks_historic(stocks_df['symbol'])
        self.to_google_storage(cdi_df, stocks_df, stocks_info_df, stocks_historic_df)

if __name__ == "__main__":
    data_collector = DataCollector('sprint3-storage') # Nome do bucket
    data_collector.run() # Execucao do script