# 🚀 Apache Druid Real-Time & OLAP Analytics Ecosystem

Este diretório contém a especificação arquitetural, infraestrutura em container e especificações de ingestão de alto desempenho para uma plataforma analítica baseada no **Apache Druid**. O ecossistema é projetado para lidar com ingestão contínua de fluxo (Streaming via Kafka) e processamento em lote (Batch via Deep Storage), permitindo consultas analíticas complexas com latência sub-segundo em bilhões de linhas.

---

## 🏛️ 1. Arquitetura Multi-Nó do Apache Druid

O Apache Druid possui uma arquitetura altamente distribuída e desacoplada, separando as funções de ingestão, armazenamento e consulta em nós especializados para garantir resiliência e escalabilidade horizontal independente.

```text
                               +-----------------------------+
                               |     APLICAÇÃO CLIENTE       |
                               | (SQL/Superset/Console Web)  |
                               +--------------+--------------+
                                              |
                                              v
                               +--------------+--------------+
                               |        Router Node          |
                               +-------+--------------+------+
                                       |              |
                                       v              v
                               +-------+-----+  +-----+------+
                               | Broker Node |  | Coordinator| <-----+
                               +---+------+--+  +-----+------+       |
                                   |      |           |              |
                                   |      |           v              |
                     +-------------+      +-----+  +--+----------+   |
                     |                          |  | Overlord    | <-|---+
                     v                          v  +--+------+---+   |   |
             +-------+------+             +-----+--+---+     |       |   |
             | Historical   |             | Middle     |     |       |   |
             | Nodes        |             | Manager    |     |       |   |
             +-------+------+             +-----+------+     |       |   |
                     |                          |            |       |   |
                     |  +--------------------+  |            |       |   |
                     +->|    DEEP STORAGE    |<-+            |       |   |
                        | (MinIO S3-compat)  |               |       |   |
                        +--------------------+               |       |   |
                                                             v       |   |
     +-------------------+                             +-----+---+   |   |
     | METADATA STORE    |<----------------------------| Postgres|---+   |
     | (Segment metadata)|                             +---------+       |
     +-------------------+                                               |
                                                                         |
     +-------------------+                             +---------+       |
     | COORDINATION      |<----------------------------|ZooKeeper|<------+
     | (Leader & State)  |                             +---------+
     +-------------------+
```

### Componentes de Processamento (Services)
- **Router Node:** O roteador de entrada. Encaminha consultas para os Brokers e requisições administrativas para o Coordinator/Overlord. Oferece a interface Web do Druid.
- **Broker Node:** Recebe as consultas SQL/Native JSON, planeja a execução dividindo o trabalho em sub-consultas, envia-as para Historicals e MiddleManagers (Peons) e consolida os resultados finais de volta para o cliente.
- **Coordinator Node:** Gerencia os dados históricos no cluster. Ele decide quais Historicals devem carregar novos segmentos de dados ou remover segmentos antigos baseado nas regras de retenção.
- **Overlord Node:** Coordena a ingestão de dados. Atribui tarefas de ingestão a MiddleManagers e monitora o progresso das tarefas de ingestão (Supervisores).
- **Historical Node:** Armazena e atende consultas sobre segmentos históricos (dados imutáveis baixados do Deep Storage). Eles carregam dados de forma assíncrona do Deep Storage para caches de disco locais rápidos.
- **MiddleManager Node:** Executa as tarefas de ingestão direta. Ele spawna processos JVM isolados chamados **Peons** para realizar a ingestão real, gerando os segmentos que depois são persistidos no Deep Storage.

### Dependências Externas (Common Infrastructure)
- **Deep Storage (MinIO/S3):** O repositório central e imutável de todos os segmentos gerados. É o "Single Source of Truth". Os nós Historical baixam os arquivos de segmento desse local para servir consultas.
- **Metadata Store (PostgreSQL):** Armazena esquemas, regras de ciclo de vida (retention policies), detalhes dos segmentos de dados disponíveis e logs de auditoria das tarefas de ingestão.
- **Service Discovery & Coordination (ZooKeeper):** Facilita o monitoramento de liderança do cluster, detecção de nós ativos e mensagens internas (Handoffs, Segment load announcements).

---

## 📊 2. O Modelo de Dados do Druid

Para atingir velocidades OLAP sub-segundo, o Druid transforma e re-organiza dados estruturados em uma estrutura de armazenamento única orientada a colunas.

### Componentes de uma Tabela (Data Source)
1. **Timestamp Column (`__time`):** Toda tabela no Druid obrigatoriamente possui uma coluna de tempo primária. Ela é usada para particionar os arquivos físicos (Segmentos) e para poda de partição extremamente agressiva em tempo de consulta.
2. **Dimensions:** Colunas com valores discretos (normalmente Strings ou Arrays de Strings) utilizadas para filtragem, agrupamento (`GROUP BY`) e projeção. O Druid gera índices bitmap invertidos automáticos para cada dimensão para acelerar as buscas analíticas.
3. **Metrics:** Colunas numéricas destinadas a cálculos agregados (`sum`, `count`, `min`, `max`, `sketches`).

### Pré-agregação e Roll-up
O **Roll-up** é uma otimização crucial no Druid executada opcionalmente durante o processo de ingestão. Ele pré-agrega dados brutos em uma janela de granularidade de tempo especificada (`queryGranularity`).

*Se 10.000 cliques ocorrerem no mesmo minuto (`MINUTE`) vindos do mesmo usuário, país e dispositivo, o Roll-up agrupa essas 10.000 linhas brutas em **uma única linha física** no segmento, criando métricas como `cnt = 10000` e calculando o acumulado numérico correspondente.* Isso reduz drasticamente o volume do armazenamento e aumenta consideravelmente a velocidade de resposta.

### Segmentos: Os Arquivos Imutáveis Particionados por Tempo
O Druid divide os dados em partições temporais chamadas **Segmentos**. Cada segmento é um arquivo imutável e colunar que contém:
- Dados colunares comprimidos (usando dicionários de strings e algoritmos como LZ4).
- Índices de bitmaps Roaring para busca ultrarrápida de valores em colunas de dimensão.
- Metadados do intervalo temporal coberto (`segmentGranularity`).

---

## 🛠️ 3. Infraestrutura Containerizada

O ecossistema local é orchestrado via Docker Compose, unindo a infraestrutura do Druid a serviços de suporte fundamentais (PostgreSQL, Kafka, ZooKeeper e MinIO).

Para inicializar a stack de desenvolvimento, utilize o arquivo `docker-compose.yml` presente na raiz deste projeto.

### Inicialização Rápida
```bash
# Navegue até o diretório do projeto
cd druid-realtime-analytics

# Suba todos os containers em segundo plano
docker compose up -d
```

### Portas e Interfaces Disponíveis localmente:
- **Druid Web Console (Router):** http://localhost:8888 (Visualização do cluster, ingestões, tarefas e editor SQL)
- **MinIO Console:** http://localhost:9001 (Acesso aos Buckets de segmentos `druid-segments` e logs `druid-logs`, credenciais: `minioadmin` / `minioadmin`)
- **PostgreSQL Database:** `localhost:5432` (Banco de metadados `druid`, credenciais: `druid` / `druid_password`)
- **Kafka Bootstrap Server:** `localhost:29092` (Para conexões externas de produtores de eventos)

---

## 📥 4. Ingestão de Dados

Fornecemos duas especificações JSON estruturadas sob o diretório `ingestion/` para demonstrar os principais fluxos de ingestão do Druid.

### A. Ingestão Contínua (Streaming Ingestion Spec)
Localizada em `ingestion/streaming-supervisor-spec.json`, esta configuração cria um supervisor do tipo `kafka` conectado ao tópico `clickstream-events`.

**Destaques da Configuração:**
- **Roll-up Habilitado:** `rollup: true` com granularidade de agregação temporal por minuto (`queryGranularity: MINUTE`) e partições por hora (`segmentGranularity: HOUR`).
- **Métricas Avançadas:**
  - **`unique_users_sketch`:** Um `thetaSketch` (com tamanho de precisão `16384`) gerado diretamente no fluxo para fornecer cálculos extremamente rápidos de usuários únicos aproximados e permitir intersecções de conjuntos de coortes em tempo de consulta.
  - **`unique_sessions_hll`:** Um `hyperUnique` baseado em HyperLogLog para estatísticas de sessões únicas ultraleves.

#### Como submeter o Supervisor de Ingestão via API REST:
```bash
curl -X POST -H 'Content-Type: application/json' \
     -d @ingestion/streaming-supervisor-spec.json \
     http://localhost:8888/druid/indexer/v1/supervisor
```

### B. Ingestão em Lote (Batch Ingestion Spec)
Localizada em `ingestion/batch-ingestion-spec.json`, esta configuração executa uma carga histórica nativa paralela (`index_parallel`) lendo arquivos JSON persistidos no Deep Storage (MinIO).

#### Como submeter a Tarefa de Lote via API REST:
```bash
curl -X POST -H 'Content-Type: application/json' \
     -d @ingestion/batch-ingestion-spec.json \
     http://localhost:8888/druid/indexer/v1/task
```

---

## 🔍 5. Consultas Analíticas SQL Otimizadas

O arquivo `queries/analytical_queries.sql` contém exemplos práticos de como interagir com as fontes de dados criadas. Abaixo estão detalhadas as abordagens aplicadas:

### 1. Poda Temporal (`__time` pruning)
```sql
SELECT ... FROM "clickstream_realtime"
WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '2' HOUR
```
*O uso do filtro no `__time` limita a consulta apenas aos segmentos criados nas últimas duas horas. O Broker ignora totalmente os demais segmentos do cluster, evitando escaneamentos desnecessários.*

### 2. Uso de Data Sketches para Contagem Distinta Rápida
Fazer `COUNT(DISTINCT user_id)` sobre bilhões de registros brutos é inviável em bancos relacionais comuns. O Druid resolve isso calculando o **Theta Sketch** pré-agregado:
```sql
SELECT country, THETA_SKETCH_ESTIMATE(DS_THETA(unique_users_sketch)) AS approx_users
FROM "clickstream_realtime"
GROUP BY country;
```

### 3. Operações de Conjunto complexas (Intersecção de Cohorts)
A união, diferença e intersecção de usuários únicos entre duas ações (ex: adicionou ao carrinho VS comprou) podem ser calculadas em milissegundos combinando sketches prévios através de funções como `THETA_SKETCH_INTERSECT` e `THETA_SKETCH_DIFF`.

---

## ⚙️ 6. Tuning de Performance e Operações de Produção

### Guia de Dimensionamento de Memória JVM
Ao contrário de bancos tradicionais, o Druid faz uso massivo de **Memória Direta Off-Heap** (`MaxDirectMemorySize`) para processamento de consultas, ordenação e mesclagem de segmentos.

#### Configurações Recomendadas por Papel:
- **Historical (Data Node pesado):**
  - Alocar um heap moderado (para gerenciamento de nós e tarefas internas) e muita memória direta para o processamento de buffers.
  - **Fórmula de Buffers:** `druid.processing.buffer.sizeBytes` * (`druid.processing.numThreads` + `druid.query.search.numMergeBuffers` + 1).
  - *Exemplo de JVM do Historical em Produção:*
    ```text
    -Xms16g -Xmx16g -XX:MaxDirectMemorySize=24g
    druid.processing.numThreads=15
    druid.processing.buffer.sizeBytes=1073741824 (1GB buffer por thread)
    druid.query.groupBy.maxMergingDictionaryBytes=100000000
    ```

- **MiddleManager & Peons (Ingestão):**
  - O MiddleManager em si requer pouca memória (`-Xms128m -Xmx128m`). No entanto, ele spawna Peons (JVMs filhas).
  - Configure `druid.indexer.runner.javaOpts` para ditar a memória de cada Peon criado:
    ```text
    -server -Xms2g -Xmx2g -XX:MaxDirectMemorySize=2g -XX:+UseG1GC
    ```
  - `druid.worker.capacity` define quantos Peons simultâneos o MiddleManager pode hospedar. Certifique-se de que o hardware tem memória disponível para suportar `worker.capacity * (peon_heap + peon_direct_memory)`.

### Retenção de Dados e Compactação Automática (Compaction Tasks)
Conforme novas partições por hora ou minuto são geradas na ingestão em tempo real, o cluster pode acabar com múltiplos segmentos pequenos. Isso prejudica as consultas.

- **Auto-Compaction:** O Druid possui um coordenador integrado para rodar tarefas automáticas de compactação em background. Elas mesclam múltiplos segmentos fragmentados menores em segmentos consolidados de tamanho ideal (tamanho ideal de segmento Druid em disco: **300MB a 700MB**).
  - Ative a compactação no console web ou enviando uma regra de compactação ao Coordinator:
    ```json
    {
      "dataSource": "clickstream_realtime",
      "keepSegmentGranularity": true,
      "tuningConfig": {
        "type": "index_parallel",
        "maxRowsPerSegment": 5000000
      }
    }
    ```

- **Regras de Retenção (Retention Rules):**
  Configure regras automáticas no Coordinator para descartar dados antigos e liberar espaço no cluster de Historicals.
  - *Exemplo:* Manter em cache de Historicals ativos apenas dados dos últimos 90 dias, aplicando descarte automático (`Drop Rule`) para dados mais antigos (que permanecem protegidos no Deep Storage se necessário).

### Monitoramento de Lag no Kafka Ingestão
Durante a ingestão contínua, o Druid acompanha o offset do Kafka. Se o cluster apresentar gargalos, o lag de consumo pode subir.

- **Diagnóstico:** Use o endpoint administrativo `/druid/indexer/v1/supervisor/<supervisorId>/status` para recuperar o relatório completo do supervisor, incluindo o offset atual do Kafka comparado com o offset lido pelo Druid.
- **Tuning de Streaming:** Se o lag estiver crescendo:
  1. Aumente o número de partições no tópico do Kafka.
  2. Ajuste o número de tarefas simultâneas no supervisor do Druid (`taskCount` no `ioConfig`). O Druid pode escalar tarefas de leitura até o limite de partições do tópico Kafka.
  3. Incremente `maxRowsInMemory` e `maxBytesInMemory` na `tuningConfig` do supervisor para evitar que o Druid realize "spills" prematuros de dados intermediários para o disco local durante a ingestão ativa.
