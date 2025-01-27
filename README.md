Adventure Works Modern Data Analytics Infrastructure
Objetivo do Projeto
Este projeto visa criar e implementar uma infraestrutura moderna de analytics para a Adventure Works, utilizando as melhores práticas de engenharia e ciência de dados. O projeto engloba desde o desenho do data warehouse até a apresentação de relatórios e dashboards com insights estratégicos e operacionais.


1. Diagrama Conceitual do Data Warehouse

Um diagrama conceitual será entregue em formato PDF, destacando as tabelas de fatos e dimensões necessárias para responder às perguntas de negócio.
O diagrama também inclui uma tabela agregada de vendas por região e vendedor.
As tabelas fonte que alimentam as tabelas fato e dimensões serão indicadas (criado no draw.io).


2. Configuração do Data Warehouse
O projeto utiliza Google BigQuery como ambiente do Data Warehouse.
Conexões configuradas e prontas para ingestão de dados brutos.

3. Pipeline de E-L (Extract-Load)
 Desenvolvimento de um pipeline automatizado para ingestão dos dados brutos da Adventure Works utilizando a ferramenta de extração Stitch. 


4. Transformação de Dados com dbt
Modelagem dos dados brutos para criação de tabelas intermediárias e analíticas usando dbt core.
Documentação e testes de schema para todas as tabelas.


5. Orquestração do Pipeline com Apache Airflow
Configuração do Apache Airflow (versão standalone) para automação do pipeline.
Criação de uma DAG que:
Roda uma vez por dia em horário configurado.
Acessa o repositório do dbt e executa:
Testes de dados.
Transformações dos modelos.
Garante conexão correta com o Data Warehouse no perfil do dbt.
Roda com sucesso por pelo menos 3 vezes consecutivas.
