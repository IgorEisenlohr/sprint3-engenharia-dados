import pandas as pd
import pandas_gbq
import investpy
import yfinance as yf
import os
from google.cloud import storage
from google.oauth2 import service_account

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
        cdi['Variacao'] = cdi['valor'] / 100 # Criacao da coluna Close
        cdi = cdi.query('ano >= 2022') # Filtragem dos dados a partir de 2021
        cdi_df = cdi[['Date', 'Ticker','Variacao']] # Selecao das colunas

        return cdi_df # Retorno do dataframe

    def get_stocks(self):
        acoes_df = investpy.stocks.get_stocks(country='brazil') # Coleta de dados de acoes brasileiras
        acoes_df['symbol'] = acoes_df['symbol'].apply(lambda x: x+'.SA') # Adicao do sufixo .SA para acoes brasileiras
        return acoes_df # Retorno do dataframe

    def get_stocks_info(self, acoes_df):
        o_tickers = acoes_df['symbol'] # Lista de tickers

        tickers =['PETR4.SA', 'VALE3.SA']

        tickers_info_list = [] # Lista vazia para armazenar os dados
        for ticker in tickers: # Loop para coletar os dados de cada ticker
            try:
                tickers_info_list.append(yf.Ticker(ticker).info ) # Coleta dos dados
                tickers_info_df = pd.DataFrame(tickers_info_list) # Transformacao da lista em dataframe
            except:
                print(f"Ticker não possui informações disponiveis: {ticker}") # Caso o ticker nao exista, printa o ticker
        return tickers_info_df[['city', 'state', 'country', 'industry', 'sector', 'symbol']] # Retorno do dataframe

    def get_stocks_historic(self, tickers):
        historicos_list = []  # Lista vazia para armazenar os dados
        for ticker in tickers:  # Loop para coletar os dados de cada ticker
            historico = yf.Ticker(ticker).history(period='2y')  # Coleta dos dados
            historico['Ticker'] = ticker  # Adicao da coluna Ticker
            historicos_list.append(historico)  # Adicao do dataframe na lista

        historicos_df = pd.concat(historicos_list, axis=0).reset_index()  # Concatenacao dos dataframes
        return historicos_df  # Retorno do dataframe

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
        stocks_historic_df = self.get_stocks_historic(['PETR4.SA', 'VALE3.SA'])
        func_names = ['get_cdi', 'get_stocks', 'get_stocks_info', 'get_stocks_historic'] # Lista com os nomes das funcoes
        self.to_google_storage(cdi_df, stocks_df, stocks_info_df, stocks_historic_df, func_names=func_names) # Salvando os dataframes no bucket
        print("DATA EXTRACTED! FILES ON GOOGLE STORAGE!")

    def transform(self, cdi_file, stocks_file, stocks_info_file, stocks_historic_file):
        print("TRANSFORMING DATA...")
        cdi_df = pd.read_csv(cdi_file)
        cadastro_df = pd.read_csv(stocks_file)
        info_df = pd.read_csv(stocks_info_file)
        historic_df = pd.read_csv(stocks_historic_file)

        historic_df['Date'] = pd.to_datetime(historic_df['Date'], format='ISO8601').dt.strftime('%Y-%m-%d') # Conversao da coluna Date para datetime

        duplicate_symbols = cadastro_df[cadastro_df.duplicated(subset='symbol', keep=False)] # Verificacao de duplicatas
        if not duplicate_symbols.empty: # Caso existam duplicatas
            print(f'Duplicatas encontradas no cadastro: {duplicate_symbols["symbol"].tolist()}') # Printa os tickers duplicados
        cadastro_cleaned_df = cadastro_df.drop_duplicates(subset='symbol', keep='first') # Remocao das duplicatas

        cadastro_df = pd.merge(cadastro_cleaned_df, info_df, on='symbol', how='left') # Merge dos dataframes de cadastro e informações
        cadastro_df_cleaned = cadastro_df.drop(columns='country_y').rename(columns={'country_x': 'country'}) # Renomeacao da coluna country

        prices_df = pd.concat([historic_df, cdi_df], ignore_index=True) # Uniao dos dataframes de historico de stocks e CDI
        duplicate_rows = prices_df[prices_df.duplicated(subset=['Date', 'Ticker'], keep=False)]
        if not duplicate_rows.empty:
            print(f'Duplicatas encontradas na tabela de preços: {duplicate_rows[["Date", "Ticker"]].to_dict(orient="records")}')
        prices_df_cleaned = prices_df.drop_duplicates(subset=['Date', 'Ticker'], keep='first') # Remocao das duplicatas

        # Ordenando o DataFrame por Ticker e Date
        prices_df_cleaned['Date'] = pd.to_datetime(prices_df_cleaned['Date'])  # Conversao da coluna Date para datetime
        prices_df_cleaned.sort_values(by=['Date', 'Ticker'], inplace=True)

        # Calculo da variacao
        prices_df_cleaned['Variacao'] = prices_df_cleaned.groupby('Ticker')['Close'].ffill().pct_change()
        prices_df_cleaned['Fator_diario'] = 1 + prices_df_cleaned['Variacao']  # Calculo do fator diario
        prices_df_cleaned['Fator_diario'].fillna(1, inplace=True)  # Substituicao dos valores nulos por 1

        # Criando colunas de Ano e Mês
        prices_df_cleaned['Ano'] = prices_df_cleaned['Date'].dt.year
        prices_df_cleaned['Mes'] = prices_df_cleaned['Date'].dt.month

        # Calculo do acumulado mensal
        prices_df_cleaned['Acumulado_mensal'] = prices_df_cleaned.groupby(['Ticker', 'Ano', 'Mes'])['Fator_diario'].cumprod()
        prices_df_cleaned['Acumulado_mensal'] = (prices_df_cleaned['Acumulado_mensal'] - 1) * 100  # (%)

        # Calculo do acumulado anual
        prices_df_cleaned['Acumulado_anual'] = prices_df_cleaned.groupby(['Ticker', 'Ano'])['Fator_diario'].cumprod()
        prices_df_cleaned['Acumulado_anual'] = (prices_df_cleaned['Acumulado_anual'] - 1) * 100  # (%)

        prices_df_cleaned['Date'] = prices_df_cleaned['Date'].astype(str)  # Conversao da coluna Date para string

        print("DATA TRANSFORMED!")

        return cadastro_df_cleaned, prices_df_cleaned
    
    def load(self, cadastro_info_df, prices_df, dataset_name, cadastro_table_name, prices_table_name):
        print("LOADING DATA...")

        # Configuração das credenciais (substitua pelo caminho do seu arquivo de credenciais)
        credentials = service_account.Credentials.from_service_account_file(
            'sprint3-storage.json'
        )

        # Carregar o dataframe cadastro_info_df no BigQuery
        pandas_gbq.to_gbq(
            cadastro_info_df,
            f"{dataset_name}.{cadastro_table_name}",
            project_id='bigquery-sandbox-385813',  # Substitua pelo ID do seu projeto
            if_exists='replace',  # Se a tabela existir, substitua
            credentials=credentials
        )

        # Carregar o dataframe prices_df no BigQuery
        pandas_gbq.to_gbq(
            prices_df,
            f"{dataset_name}.{prices_table_name}",
            project_id='bigquery-sandbox-385813',  # Substitua pelo ID do seu projeto
            if_exists='replace',  # Se a tabela existir, substitua
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