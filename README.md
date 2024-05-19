# SteamReplayabilityFest
Coleta e Normalização dos dados do repositório: https://steamdb.info/sales/

## Bibliotecas utilizadas
- requests
- BeautifulSoup
- pandas
- google.cloud.bigquery

## Modulos utilizados
- re
- datetime
- os

## Variáveis de ambiente utilizadas
Foi criado uma variável de ambiente chamada "GOOGLE_APPLICATION_CREDENTIALS", essa variável indica o caminho do JSON de configuração de acesso do GCP.

## Etapa 01 (Script Crawler)
Foi realizado a coleta dos dados, utilizando python, diretamente do site https://steamdb.info/sales/, através da biblioteca requests. Após o acesso foi realizado a ingestão do dado para o pandas, via soup e sua ingestão em tabela BigQuery.

## Etapa 02 (Script ETL)
Nessa etapa eu capturo os dados da tabela stagin, que foi criada na etapa 01 e realizo as seguintes normalizações:
  - Coluna Ano_Lancamento = transformação da coluna em inteiro.
  - Coluna Dt_Lancamento = Unifica o mês e ano para gerar uma coluna data, ele também realiza um tratamento nos itens N/A para trazer uma data padrão e o pessoal do BI poder utilizar a coluna sem precisar realizar esse tratamento.
  - Coluna Reducoes_Preco = transformação da coluna em inteiro.
  - Coluna Preco = normalização da coluna para o tipo numeric.
  - Coluna Classificacao = normalização da coluna para o tipo numeric.
  - Reordenação das colunas do DF para preenchimento no BigQuery em um modelo mais legivel e parecido com a exibição que temos no site = ['Nome_Jogo', 'Reducoes_Preco', 'Tipo_Reducao_Preco', 'Tipo_Preco', 'Preco', 'Classificacao', 'Tipo_Classificacao', 'Dt_Lancamento', 'Ano_Lancamento', 'Mes_Lancamento']
