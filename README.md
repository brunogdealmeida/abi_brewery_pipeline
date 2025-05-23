# Pipeline de Dados da ABI Brewery

## Visão Geral

Este projeto implementa uma pipeline de dados para processamento de dados de cervejarias usando o Apache Airflow como motor de orquestração.

## Arquitetura

### Camadas de Dados

1. **Camada Bronze**
   - Ingestão de dados brutos
   - Validação básica (verificações de ID nulo)
   - Sem transformações

2. **Camada Silver**
   - Limpeza e transformação de dados
   - Remoção de duplicatas
   - Validação de URL
   - Verificações básicas de qualidade de dados

3. **Camada Gold**
   - Dados prontos para negócio
   - Transformações complexas
   - Verificações avançadas de qualidade
   - Validação final antes da análise

## Stack Tecnológica

### Tecnologias Principais

1. **Apache Airflow**
   - Motor de orquestração
   - Gerenciamento de fluxo de trabalho baseado em DAG
   - Agendamento e monitoramento de tarefas

2. **Apache Spark**
   - Motor de processamento de dados
   - Processamento distribuído
   - Verificações de qualidade de dados

3. **Streamlit**
   - Visualização de dados
   - Dashboard interativo
   - Monitoramento de qualidade

### Tecnologias Alternativas

1. **Orquestração**
   - **Apache Airflow** (Escolhido)
     - Prós: Maduro, suporte da comunidade, baseado em DAG, ecossistema rico
     - Contras: Recursos intensivos, configuração complexa
   - **Alternativa: Prefect**
     - Prós: Moderno, Pythonico, configuração fácil
     - Contras: Ecossistema menos maduro, menos integrações

2. **Processamento de Dados**
   - **Apache Spark** (Escolhido)
     - Prós: Escalável, processamento distribuído, API rica
     - Contras: Recursos intensivos, configuração complexa
   - **Alternativa: Dask**
     - Prós: Python nativo, configuração fácil
     - Contras: Menos escalável, recursos limitados

3. **Visualização**
   - **Streamlit** (Escolhido)
     - Prós: Simples, Pythonico, interativo
     - Contras: Escalabilidade limitada, recursos básicos
   - **Alternativa: Superset**
     - Prós: Recursos avançados, pronto para empresa
     - Contras: Configuração complexa, recursos intensivos

## Testes e Monitoramento de Qualidade de Dados

### Implementação Atual

1. **Testes de Qualidade de Dados**
   - **Camada Bronze**: Verificações de ID nulo
   - **Camada Silver**: Verificações de duplicados e validação de URL
   - **Camada Gold**: Validação de tipo de cervejaria

2. **Monitoramento**
   - Dashboard em tempo real usando Streamlit
   - Visualização de métricas de qualidade
   - Monitoramento por camada

3. **Estrutura de Testes**
   - **Testes Simples**: Verificações básicas de qualidade de dados
   - **Testes de Pipeline**: Validação completa do fluxo de dados
   - **Testes de Schema**: Validação de estrutura de dados

4. **Resultados dos Testes**
   - Métricas de resumo: total de testes, passados, falhas
   - Detalhes de cada teste: resultado e timestamp
   - Status visual (✅/❌) para cada teste

5. **Integração com Airflow**
   - DAG dedicado para testes de qualidade
   - Execução diária de testes
   - Salvamento automático dos resultados

### Melhorias Futuras no Monitoramento

1. **Sistema de Alertas**
   - Notificações no Slack para falhas
   - Alertas por email para problemas críticos
   - Integração com PagerDuty para monitoramento 24/7

2. **Métricas Avançadas**
   - Rastreamento de latência
   - Utilização de recursos
   - Detecção de drift de dados
   - Detecção de anomalias

3. **Ferramentas de Observabilidade**
   - **Prometheus + Grafana**

## Estrutura do Projeto

```
.
├── dags/                 # Definições de DAG do Airflow
│   └── brewery_pipeline.py  # Orquestração principal da pipeline
├── include/              # Arquivos adicionais do projeto
│   ├── data/            # Armazenamento de dados
│   ├── jobs/           # Jobs de processamento
│   │   ├── landing.py  # Processamento de dados brutos
│   │   ├── bronze.py   # Processamento da camada Bronze
│   │   ├── silver.py   # Processamento da camada Silver
│   │   └── gold.py     # Processamento da camada Gold
│   └── data_quality.py  # Verificações de qualidade de dados
└── README.md            # Documentação do projeto
```

## Começando

### Pré-requisitos

- Python 3.11+
- Docker
- CLI do Astronomer

### Instalação

1. Clone o repositório
2. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```
3. Inicie o Airflow localmente:
   ```bash
   astro dev start
   ```

### Executando a Pipeline

1. Acesse a UI do Airflow em http://localhost:8080/
2. Dispare o DAG `brewery_pipeline`

### Executando o Dashboard do Streamlit

1. Construa a imagem do Docker do Streamlit:
   ```bash
   docker build -t streamlit-brewery -f streamlit.Dockerfile .
   ```
2. Execute o contêiner do Streamlit:
   ```bash
   docker run -p 8501:8501 -v $(pwd):/app streamlit-brewery
   ```

## Licença

Este projeto está licenciado sob a Licença MIT - veja o arquivo LICENSE para detalhes.

## Contato

Para suporte ou dúvidas, por favor, abra uma issue no repositório.

---

# ABI Brewery Pipeline (English Version)

## Overview

This project implements a data pipeline for processing brewery data using Apache Airflow as the orchestration engine.

## Architecture

### Data Layers

1. **Bronze Layer**
   - Raw data ingestion
   - Basic validation (ID null checks)
   - No transformations

2. **Silver Layer**
   - Data cleaning and transformation
   - Duplicate removal
   - URL validation
   - Basic data quality checks

3. **Gold Layer**
   - Business-ready data
   - Complex transformations
   - Advanced data quality checks
   - Final validation before analytics

## Technology Stack

### Core Technologies

1. **Apache Airflow**
   - Orchestration engine
   - DAG-based workflow management
   - Task scheduling and monitoring

2. **Apache Spark**
   - Data processing engine
   - Distributed data processing
   - Data quality checks

3. **Streamlit**
   - Data visualization
   - Interactive dashboard
   - Quality monitoring

### Alternative Technologies

1. **Orchestration**
   - **Apache Airflow** (Chosen)
     - Pros: Mature, community support, DAG-based, rich ecosystem
     - Cons: Resource-intensive, complex setup
   - **Alternative: Prefect**
     - Pros: Modern, Pythonic, easier setup
     - Cons: Less mature ecosystem, fewer integrations

2. **Data Processing**
   - **Apache Spark** (Chosen)
     - Pros: Scalable, distributed processing, rich API
     - Cons: Resource-intensive, complex setup
   - **Alternative: Dask**
     - Pros: Native Python, easier setup
     - Cons: Less scalable, limited features

3. **Visualization**
   - **Streamlit** (Chosen)
     - Pros: Simple, Pythonic, interactive
     - Cons: Limited scalability, basic features
   - **Alternative: Superset**
     - Pros: Feature-rich, enterprise-ready
     - Cons: Complex setup, resource-intensive

## Data Quality & Monitoring

### Current Implementation

1. **Data Quality Checks**
   - Bronze Layer: ID null checks
   - Silver Layer: Duplicate checks, URL validation
   - Gold Layer: Brewery type validation

2. **Monitoring**
   - Real-time dashboard using Streamlit
   - Quality metrics visualization
   - Layer-by-layer status monitoring

3. **Testing Structure**
   - **Simple Tests**: Basic data quality checks
   - **Pipeline Tests**: Complete data flow validation
   - **Schema Tests**: Data structure validation

4. **Test Results**
   - Summary metrics: total tests, passed, failed
   - Detailed test results: result and timestamp
   - Visual status (✅/❌) for each test

5. **Airflow Integration**
   - Dedicated DAG for quality tests
   - Daily test execution
   - Automatic results saving

### Future Monitoring Enhancements

1. **Alerting System**
   - Slack notifications for failures
   - Email alerts for critical issues
   - PagerDuty integration for 24/7 monitoring

2. **Advanced Metrics**
   - Latency tracking
   - Resource utilization
   - Data drift detection
   - Anomaly detection

3. **Observability Tools**
   - **Prometheus + Grafana**

## Project Structure

```
.
├── dags/                 # Airflow DAG definitions
│   └── brewery_pipeline.py  # Main pipeline orchestration
├── include/              # Additional project files
│   ├── data/            # Data storage
│   ├── jobs/           # Processing jobs
│   │   ├── landing.py  # Raw data processing
│   │   ├── bronze.py   # Bronze layer processing
│   │   ├── silver.py   # Silver layer processing
│   │   └── gold.py     # Gold layer processing
│   └── data_quality.py  # Data quality checks
└── README.md            # Project documentation
```

## Getting Started

### Prerequisites

- Python 3.11+
- Docker
- Astronomer CLI

### Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Start Airflow locally:
   ```bash
   astro dev start
   ```

### Running the Pipeline

1. Access Airflow UI at http://localhost:8080/
2. Trigger the `brewery_pipeline` DAG

### Running the Streamlit Dashboard

1. Build the Streamlit Docker image:
   ```bash
   docker build -t streamlit-brewery -f streamlit.Dockerfile .
   ```
2. Run the Streamlit container:
   ```bash
   docker run -p 8501:8501 -v $(pwd):/app streamlit-brewery
   ```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contact

For support or questions, please open an issue in the repository. The pipeline follows a modern data engineering architecture with clear separation of concerns between different data layers (Bronze, Silver, and Gold).

## Arquitetura

### Camadas de Dados

1. **Camada Bronze**
   - Ingestão de dados brutos
   - Validação básica (verificações de ID nulo)
   - Sem transformações

2. **Camada Silver**
   - Limpeza e transformação de dados
   - Remoção de duplicatas
   - Validação de URL
   - Verificações básicas de qualidade de dados

3. **Camada Gold**
   - Dados prontos para negócio
   - Transformações complexas
   - Verificações avançadas de qualidade
   - Validação final antes da análise

---

## Architecture

### Data Layers

1. **Bronze Layer**
   - Raw data ingestion
   - Basic validation (ID null checks)
   - No transformations

2. **Silver Layer**
   - Data cleaning and transformation
   - Duplicate removal
   - URL validation
   - Basic data quality checks

3. **Gold Layer**
   - Business-ready data
   - Complex transformations
   - Advanced data quality checks
   - Final validation before analytics

## Stack Tecnológica

### Tecnologias Principais

1. **Apache Airflow**
   - Motor de orquestração
   - Gerenciamento de fluxo de trabalho baseado em DAG
   - Agendamento e monitoramento de tarefas

2. **Apache Spark**
   - Motor de processamento de dados
   - Processamento distribuído
   - Verificações de qualidade de dados

3. **Streamlit**
   - Visualização de dados
   - Dashboard interativo
   - Monitoramento de qualidade

---

## Technology Stack

### Core Technologies

1. **Apache Airflow**
   - Orchestration engine
   - DAG-based workflow management
   - Task scheduling and monitoring

2. **Apache Spark**
   - Data processing engine
   - Distributed data processing
   - Data quality checks

3. **Streamlit**
   - Data visualization
   - Interactive dashboard
   - Quality monitoring

### Alternative Technologies

1. **Orchestration**
   - **Apache Airflow** (Chosen)
     - Pros: Mature, community support, DAG-based, rich ecosystem
     - Cons: Resource-intensive, complex setup
   - **Alternative: Prefect**
     - Pros: Modern, Pythonic, easier setup
     - Cons: Less mature ecosystem, fewer integrations

2. **Data Processing**
   - **Apache Spark** (Chosen)
     - Pros: Scalable, distributed processing, rich API
     - Cons: Resource-intensive, complex setup
   - **Alternative: Dask**
     - Pros: Native Python, easier setup
     - Cons: Less scalable, limited features

3. **Visualization**
   - **Streamlit** (Chosen)
     - Pros: Simple, Pythonic, interactive
     - Cons: Limited scalability, basic features
   - **Alternative: Superset**
     - Pros: Feature-rich, enterprise-ready
     - Cons: Complex setup, resource-intensive

## Testes e Monitoramento de Qualidade de Dados

### Implementação Atual

1. **Testes de Qualidade de Dados**
   - **Camada Bronze**: Verificações de ID nulo
   - **Camada Silver**: Verificações de duplicados e validação de URL
   - **Camada Gold**: Validação de tipo de cervejaria

2. **Monitoramento**
   - Dashboard em tempo real usando Streamlit
   - Visualização de métricas de qualidade
   - Monitoramento por camada

3. **Estrutura de Testes**
   - **Testes Simples**: Verificações básicas de qualidade de dados
   - **Testes de Pipeline**: Validação completa do fluxo de dados
   - **Testes de Schema**: Validação de estrutura de dados

4. **Resultados dos Testes**
   - Métricas de resumo: total de testes, passados, falhas
   - Detalhes de cada teste: resultado e timestamp
   - Status visual (✅/❌) para cada teste

5. **Integração com Airflow**
   - DAG dedicado para testes de qualidade
   - Execução diária de testes
   - Salvamento automático dos resultados

### Melhorias Futuras no Monitoramento

1. **Sistema de Alertas**
   - Notificações no Slack para falhas
   - Alertas por email para problemas críticos
   - Integração com PagerDuty para monitoramento 24/7

2. **Métricas Avançadas**
   - Rastreamento de latência
   - Utilização de recursos
   - Detecção de drift de dados
   - Detecção de anomalias

3. **Ferramentas de Observabilidade**
   - **Prometheus + Grafana**

## Data Quality & Monitoring

### Current Implementation

1. **Data Quality Checks**
   - Bronze Layer: ID null checks
   - Silver Layer: Duplicate checks, URL validation
   - Gold Layer: Brewery type validation

2. **Monitoring**
   - Real-time dashboard using Streamlit
   - Quality metrics visualization
   - Layer-by-layer status monitoring

3. **Testing Structure**
   - **Simple Tests**: Basic data quality checks
   - **Pipeline Tests**: Complete data flow validation
   - **Schema Tests**: Data structure validation

4. **Test Results**
   - Summary metrics: total tests, passed, failed
   - Detailed test results: result and timestamp
   - Visual status (✅/❌) for each test

5. **Airflow Integration**
   - Dedicated DAG for quality tests
   - Daily test execution
   - Automatic results saving

### Future Monitoring Enhancements

1. **Alerting System**
   - Slack notifications for failures
   - Email alerts for critical issues
   - PagerDuty integration for 24/7 monitoring

2. **Advanced Metrics**
   - Latency tracking
   - Resource utilization
   - Data drift detection
   - Anomaly detection

3. **Observability Tools**
   - **Prometheus + Grafana**
     - Time-series metrics
     - Advanced visualizations
     - Alerting rules
   - **ELK Stack**
     - Log aggregation
     - Error tracking
     - Performance monitoring

4. **CI/CD Integration**
   - Automated testing
   - Quality gate checks
   - Deployment validation

## Estrutura do Projeto

```
.
├── dags/                 # Definições de DAG do Airflow
│   └── brewery_pipeline.py  # Orquestração principal da pipeline
├── include/              # Arquivos adicionais do projeto
│   ├── data/            # Armazenamento de dados
│   ├── jobs/           # Jobs de processamento
│   │   ├── landing.py  # Processamento de dados brutos
│   │   ├── bronze.py   # Processamento da camada Bronze
│   │   ├── silver.py   # Processamento da camada Silver
│   │   └── gold.py     # Processamento da camada Gold
│   └── data_quality.py  # Verificações de qualidade de dados
└── README.md            # Documentação do projeto
```

## Começando

### Pré-requisitos

- Python 3.11+
- Docker
- CLI do Astronomer

### Instalação

1. Clone o repositório
2. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```
3. Inicie o Airflow localmente:
   ```bash
   astro dev start
   ```

### Executando a Pipeline

1. Acesse a UI do Airflow em http://localhost:8080/
2. Dispare o DAG `brewery_pipeline`

### Executando o Dashboard do Streamlit

1. Construa a imagem do Docker do Streamlit:
   ```bash
   docker build -t streamlit-brewery -f streamlit.Dockerfile .
   ```
2. Execute o contêiner do Streamlit:
   ```bash
   docker run -p 8501:8501 -v $(pwd):/app streamlit-brewery
   ```
3. The dashboard will be available at:
   - http://0.0.0.0:8501/ (recommended)
   - http://localhost:8501/

The Streamlit dashboard provides:
- Real-time data quality monitoring
- Layer-by-layer validation results
- Interactive visualizations of pipeline metrics
- Easy-to-read success/failure indicators for each layer

Note: The `-v $(pwd):/app` flag mounts the current directory to the container, allowing the dashboard to access the latest data quality results.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contact

For support or questions, please open an issue in the repository.
