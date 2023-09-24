# Dag_Extract_PostgreSQL_To_BigQuery_loading_RZ_and_SZ

DAG destinada a extração dos dados do PostgreSQL diretamente para o BigQuery, sendo os dados depositados primeiramente na raw_zone, onde foi adicionado uma coluna de controle no timezone UTC-0 com o intuito de monitorar as atualizações da tabela no BigQuery, sendo também disponibilizado os dados na camada standard_zone, onde a coluna de controle é padronizada para o timezone UTC-3.

1. Necessário implementar a estrutura do Operador customizado "Extract_PostgreSQL_To_BigQuery" que se encontra no projeto "Customize_Operator_PostgreSQL_To_BigQuery" dentro do diretório responsável por armazenar os operadores que foram dessenvolvidos.
2. Necessário uma chave json para autenticação no ambiente da cloud.
3. Necessário a implementação dos diretórios "deployments" e "global_modules", sendo que o ambiente do airflow precisar estar estruturado

Tools used:
1. Python
2. PostgreSQL
3. Cloud Storage
4. BigQuery
5. Airflow
