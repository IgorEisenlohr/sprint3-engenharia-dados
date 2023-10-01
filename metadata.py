from google.cloud import bigquery
from google.oauth2 import service_account

class MetadataTableCreator:
    def __init__(self, project_id, credentials_path):
        self.project_id = project_id # Nome do projeto
        self.credentials = service_account.Credentials.from_service_account_file(credentials_path) # Credenciais do Google Cloud
        self.client = bigquery.Client(project=project_id, credentials=self.credentials) # Cliente do BigQuery

    def create_metadata_table(self, dataset_name, table_name):
        schema = [
            # Metadados da empresa
            bigquery.SchemaField("country", "STRING", mode="NULLABLE", description="País de negociação do ticket ('brazil')."),
            bigquery.SchemaField("name", "STRING", mode="NULLABLE", description="Nome da companhia."),
            bigquery.SchemaField("full_name", "STRING", mode="NULLABLE", description="Nome completo da companhia."),
            bigquery.SchemaField("isin", "STRING", mode="NULLABLE", description="Código isin (cadastro na b3)."),
            bigquery.SchemaField("currency", "STRING", mode="NULLABLE", description="Moeda."),
            bigquery.SchemaField("ticker", "STRING", mode="REQUIRED", description="Simbolo/Ticker da empresa na bolsa."),
            bigquery.SchemaField("city", "STRING", mode="NULLABLE", description="Cidade de localização da empresa."),
            bigquery.SchemaField("state", "STRING", mode="NULLABLE", description="Estado de localização da empresa."),
            bigquery.SchemaField("industry", "STRING", mode="NULLABLE", description="Indústria de atuação da empresa."),
            bigquery.SchemaField("sector", "STRING", mode="NULLABLE", description="Setor de atuação da empresa."),
            
            # Dados da cotação
            bigquery.SchemaField("date", "STRING", mode="REQUIRED", description="Data da cotação."),
            bigquery.SchemaField("year", "INTEGER", mode="NULLABLE", description="Ano da cotação."),
            bigquery.SchemaField("month", "INTEGER", mode="NULLABLE", description="Mês da cotação."),
            bigquery.SchemaField("day", "INTEGER", mode="NULLABLE", description="Dia do mês da cotação."),
            bigquery.SchemaField("close", "FLOAT", mode="REQUIRED", description="Preço de fechamento da cotação."),
            bigquery.SchemaField("volume", "FLOAT", mode="NULLABLE", description="Volume de negociação."),
            bigquery.SchemaField("daily_factor", "DOUBLE", mode="NULLABLE", description="Variação diária do ativo em número índice (1 + variação)."),
            bigquery.SchemaField("month_accumulated_factor", "FLOAT", mode="NULLABLE", description="Acumulado mensal da variação."),
            bigquery.SchemaField("year_accumulated_factor", "FLOAT", mode="NULLABLE", description="Acumulado anual da variação.")
        ]


        table = bigquery.Table(f"{self.project_id}.{dataset_name}.{table_name}", schema=schema) # Criacao da tabela
        table = self.client.create_table(table) # Criacao da tabela no BigQuery
        print(f"Table {table_name} created in dataset {dataset_name}.") # Mensagem de sucesso

if __name__ == "__main__":
    creator = MetadataTableCreator('bigquery-sandbox-385813', "./sprint3-storage.json") # Nome do projeto e credenciais
    creator.create_metadata_table('sprint3', "metadata") # Nome do dataset e nome da tabela
