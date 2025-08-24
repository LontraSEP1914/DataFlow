# DataFlow Delivery Center

O **DataFlow** é uma poderosa ferramenta de ETL com interface gráfica, projetada para automatizar a consolidação e limpeza de múltiplos arquivos (Excel, CSV, TXT) com formatos e cabeçalhos inconsistentes. A aplicação resolve o desafio de unificar dados de fontes heterogêneas, melhorando em 100% a eficiência na disponibilização dos dados para análise, como destacado no [meu portfólio](https://lontrasep1914.github.io/).

## ✨ Funcionalidades Principais

* **Motor de Alto Desempenho:** Utiliza **Polars** como motor de processamento, garantindo alta performance na manipulação de grandes volumes de dados.
* **Detecção Inteligente de Cabeçalho:** O algoritmo analisa as primeiras linhas de cada arquivo para identificar automaticamente onde os cabeçalhos se encontram, ignorando linhas de título ou em branco.
* **Mapeamento e Agrupamento de Colunas:**
    * **Análise Inteligente:** A ferramenta agrupa automaticamente colunas com nomes semelhantes (ex: "CNPJ", "C.N.P.J.", "cnpj_cliente").
    * **Interface de Mapeamento:** Permite ao usuário revisar, dividir ou mesclar os grupos sugeridos, e definir um nome final para cada coluna.
    * **Filtros de Dados Avançados:** Crie regras de filtro complexas para refinar os dados a serem consolidados. A ferramenta combina filtros na mesma coluna com "OU" e filtros em colunas diferentes com "E".
    * **Suporte a Múltiplos Formatos:** Consolide arquivos `.xlsx`, `.xls`, `.csv` e `.txt`.
* **Saída Profissional:** Gera um arquivo de saída consolidado (XLSX, CSV ou Parquet) com uma coluna "Origem" para rastreabilidade e formatação profissional no caso do Excel.

## 🛠️ Tecnologias Utilizadas

* **Python:** Linguagem principal.
* **Polars:** Biblioteca de DataFrames de alta performance para o processamento dos dados.
* **PySide6:** Para a construção da interface gráfica moderna e responsiva.
* **Openpyxl & Xlrd:** Para a leitura de arquivos Excel.

## 🚀 Como Usar

1.  Clone o repositório.
2.  Instale as dependências: `pip install pyside6 polars openpyxl xlrd`.
3.  Execute `main.py` para iniciar o DataFlow.
4.  Selecione a pasta contendo os arquivos a serem processados.
5.  Clique em "Analisar/Mapear Cabeçalhos" para definir as regras de consolidação.
6.  (Opcional) Clique em "Definir Filtros" para refinar os dados.
7.  Escolha o formato e o local do arquivo de saída e inicie a consolidação.
