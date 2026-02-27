"""
seed_logs.py — Gera e indexa logs falsos no OpenSearch local.

Analogia: pense neste script como um "gerador de dados de teste" igual ao
que você usaria com factories (factory_boy, Faker) num projeto Django/Flask —
mas em vez de popular um banco relacional, estamos populando um índice
no OpenSearch, que funciona como um banco de documentos JSON otimizado
para busca full-text.

Uso:
    python seed_logs.py              # indexa 500 logs (padrão)
    python seed_logs.py --count 2000 # indexa N logs
    python seed_logs.py --delete     # apaga o índice e recria do zero
"""

import argparse
import json
import random
import uuid
from datetime import datetime, timezone, timedelta

import httpx  # cliente HTTP moderno — análogo ao requests, mas com suporte a async


# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÃO
# Constantes globais do script. Em produção isso viria de variáveis de ambiente,
# mas aqui deixamos fixo para simplicidade educacional.
# ══════════════════════════════════════════════════════════════════════════════

OPENSEARCH_URL = "http://localhost:9200"   # porta padrão da API REST do OpenSearch
INDEX_NAME     = "container-logs-fake"    # nome do "índice" — análogo a uma tabela no Postgres
DEFAULT_COUNT  = 500                      # quantidade padrão de documentos a gerar


# ══════════════════════════════════════════════════════════════════════════════
# DADOS DO CENÁRIO FICTÍCIO
#
# Simulamos um ambiente Kubernetes com vários times/serviços.
# No Kubernetes real, cada serviço roda em "pods" (instâncias de container),
# agrupados em "namespaces" (como pastas/ambientes lógicos).
#
# Analogia: namespace = schema no Postgres; pod = instância de uma aplicação.
# ══════════════════════════════════════════════════════════════════════════════

# Namespaces = times/domínios de negócio isolados no cluster
NAMESPACES = ["payments", "checkout", "inventory", "auth", "gateway", "notifications"]

# Cada namespace tem seus próprios serviços (microserviços)
SERVICES = {
    "payments":      ["payments-worker", "payments-api", "payments-reconciler"],
    "checkout":      ["checkout-api", "checkout-cart", "checkout-promo"],
    "inventory":     ["inventory-sync", "inventory-api", "inventory-cache"],
    "auth":          ["auth-service", "token-issuer", "session-manager"],
    "gateway":       ["api-gateway", "rate-limiter", "lb-controller"],
    "notifications": ["email-sender", "push-worker", "sms-relay"],
}

# Níveis de log — igual ao Python logging (DEBUG < INFO < WARN < ERROR)
LOG_LEVELS   = ["INFO", "WARN", "ERROR", "DEBUG"]

# Pesos de probabilidade: num sistema saudável, INFO é maioria e ERROR é raro.
# random.choices() usa esses pesos para sortear com distribuição não-uniforme.
LEVEL_WEIGHTS = [0.60, 0.20, 0.12, 0.08]

# Templates de mensagem por nível.
# Os placeholders {ms}, {trace}, etc. serão preenchidos em render_template().
# Imitam mensagens reais de sistemas distribuídos.
LOG_TEMPLATES = {
    "INFO": [
        "Request processed successfully in {ms}ms | trace_id={trace}",
        "User {user_id} authenticated | method=JWT",
        "Cache hit for key {cache_key} | ttl_remaining={ttl}s",
        "Scheduled job completed | duration={ms}ms records_processed={n}",
        "Health check passed | upstream={upstream} latency={ms}ms",
        "Message published to topic {topic} | partition={part} offset={offset}",
        "Database connection pool: {active}/{max} active connections",
    ],
    "WARN": [
        "Slow query detected ({ms}ms > 500ms threshold) | query={query}",
        "Retry attempt {attempt}/3 for upstream {upstream} | trace_id={trace}",
        "Memory usage at {pct}% | pod={pod} namespace={ns}",
        "Rate limit approaching: {used}/{limit} requests/min | client={client}",
        "Deprecated endpoint called: {endpoint} | suggest={suggest}",
        "Circuit breaker half-open | service={upstream} failures={n}",
    ],
    "ERROR": [
        # OOMKilled = pod morto por falta de memória — cenário real do artigo
        "OOMKilled: container exceeded memory limit of {limit}Mi | pod={pod}",
        "Connection refused to {upstream}:{port} | trace_id={trace}",
        "Unhandled exception in {handler}: {exc} | trace_id={trace}",
        "Database query failed after {attempt} retries | error={error}",
        "Payment processing failed | amount={amount} currency=USD trace_id={trace}",
        "JWT validation error: token expired at {ts} | user={user_id}",
        "Kafka consumer lag exceeded threshold: {lag} messages behind",
    ],
    "DEBUG": [
        "Entering function {func} | args={args}",
        "SQL: {sql} | params={params} duration={ms}ms",
        "HTTP OUT → {method} {url} | headers={n} headers",
        "Feature flag '{flag}' evaluated: {val} for user {user_id}",
    ],
}

# Dependências externas que os serviços "chamam" (para simular erros de conexão)
UPSTREAMS  = ["postgres-primary", "redis-cluster", "kafka-broker", "elasticsearch", "s3-bucket"]

# Tipos de exceção que podem aparecer em logs ERROR
EXCEPTIONS = ["NullPointerException", "TimeoutException", "ConnectionResetError", "KeyError", "ValueError"]

# Mensagens de erro de banco de dados
ERRORS     = ["deadlock detected", "too many connections", "disk quota exceeded", "network timeout"]


# ══════════════════════════════════════════════════════════════════════════════
# GERAÇÃO DE DADOS FALSOS
# ══════════════════════════════════════════════════════════════════════════════

def rand_pod_id(service: str) -> str:
    """
    Gera um nome de pod realista: <serviço>-<hash aleatório>.

    No Kubernetes real, o sufixo é gerado pelo scheduler do cluster.
    Ex: "payments-worker-7b4f9" — exatamente o formato que aparece no artigo.
    """
    suffix = "".join(random.choices("abcdef0123456789", k=5))
    return f"{service}-{suffix}"


def render_template(template: str, ns: str, pod: str) -> str:
    """
    Preenche um template de mensagem com valores aleatórios.

    Funciona como str.format() do Python: substitui cada {placeholder}
    por um valor gerado na hora. Cada chamada produz uma mensagem única.

    Parâmetros recebidos do contexto (ns, pod) garantem que a mensagem
    seja consistente com o namespace e pod que a "gerou".
    """
    return template.format(
        ms=random.randint(5, 3000),                          # latência em milissegundos
        trace=str(uuid.uuid4()),                             # trace ID único — para rastrear requests entre serviços
        user_id=f"usr_{random.randint(1000, 9999)}",
        cache_key=f"key:{random.randint(1, 100)}",
        ttl=random.randint(10, 300),                         # time-to-live do cache em segundos
        n=random.randint(1, 10000),
        upstream=random.choice(UPSTREAMS),
        topic=f"events.{ns}.{random.choice(['created','updated','deleted'])}",  # tópico Kafka
        part=random.randint(0, 7),                           # partição Kafka (0–7)
        offset=random.randint(10000, 999999),                # posição da mensagem no tópico
        active=random.randint(1, 20),                        # conexões ativas no pool
        max=random.randint(20, 50),                          # tamanho máximo do pool
        query=f"SELECT * FROM {random.choice(['orders','users','products'])}",
        attempt=random.randint(1, 3),
        pct=random.randint(70, 98),                          # % de uso de memória
        pod=pod,
        ns=ns,
        used=random.randint(800, 999),
        limit=1000,
        client=f"client_{random.randint(1, 50)}",
        endpoint=f"/api/v1/{random.choice(['users','products','orders'])}",
        suggest=f"/api/v2/{random.choice(['users','products','orders'])}",
        port=random.choice([5432, 6379, 9092, 9200]),        # portas conhecidas: Postgres, Redis, Kafka, OpenSearch
        handler=f"{ns}.handlers.{random.choice(['process','handle','execute'])}",
        exc=random.choice(EXCEPTIONS),
        error=random.choice(ERRORS),
        amount=f"{random.uniform(10, 9999):.2f}",
        ts=datetime.now(timezone.utc).isoformat(),
        lag=random.randint(1000, 50000),                     # atraso no consumo de mensagens Kafka
        func=f"{random.choice(['process_request','handle_event','validate_token'])}",
        args=f"['{random.choice(['arg1','arg2'])}']",
        sql=f"SELECT id, name FROM {random.choice(['users','orders'])} WHERE id=$1",
        params=f"[{random.randint(1, 999)}]",
        method=random.choice(["GET", "POST", "PUT"]),
        url=f"http://{random.choice(UPSTREAMS)}/{random.choice(['health','data','sync'])}",
        flag=f"feature_{random.choice(['dark_mode','new_checkout','ab_test'])}",
        val=random.choice(["true", "false"]),
    )


def generate_log(minutes_ago_max: int = 60 * 24) -> dict:
    """
    Monta um documento de log completo — o "JSON" que vai para o OpenSearch.

    Estrutura do documento é idêntica à do projeto real do artigo:
    campos @timestamp, log, stream e o objeto aninhado 'kubernetes'.

    O timestamp é gerado aleatoriamente dentro de uma janela de tempo passada,
    simulando um histórico de logs (não todos "agora").

    Retorna um dict Python que será serializado para JSON na indexação.
    """
    # Sorteia contexto: qual namespace/serviço/pod "emitiu" este log
    ns      = random.choice(NAMESPACES)
    service = random.choice(SERVICES[ns])
    pod     = rand_pod_id(service)

    # Sorteia nível com distribuição realista (INFO >> ERROR)
    level    = random.choices(LOG_LEVELS, weights=LEVEL_WEIGHTS)[0]
    template = random.choice(LOG_TEMPLATES[level])
    message  = render_template(template, ns, pod)

    # Timestamp: momento aleatório nas últimas N horas (padrão: 24h)
    # Isso cria um histórico distribuído no tempo, não tudo no mesmo segundo
    delta = timedelta(seconds=random.randint(0, minutes_ago_max * 60))
    ts    = datetime.now(timezone.utc) - delta

    return {
        # Campo padrão de timestamp no OpenSearch — reconhecido automaticamente
        "@timestamp": ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",

        # Campo "log": mensagem bruta como apareceria num terminal (stdout/stderr)
        # Formato "key=value" é comum em logs estruturados (logfmt)
        "log": f"level={level.lower()} msg=\"{message}\"",

        # Campo separado para facilitar filtros por nível sem parsear o campo "log"
        "level": level,

        # stderr para erros (convenção Unix), stdout para o resto
        "stream": "stderr" if level == "ERROR" else "stdout",

        # Metadados do Kubernetes — campos que o Fluent Bit/Fluentd adicionam
        # automaticamente ao coletar logs dos containers no cluster real
        "kubernetes": {
            "namespace_name": ns,
            "pod_name":       pod,
            "container_name": service,
            "host":           f"node-{random.randint(1, 5)}",   # nó (VM) onde o pod está rodando
            "pod_ip":         f"10.0.{random.randint(0,255)}.{random.randint(1,254)}",
            "labels": {
                # Labels são metadados livres que o time de DevOps coloca nos pods
                "app":            service,
                "env":            "dev",
                "component_name": service,
            },
        },
    }


# ══════════════════════════════════════════════════════════════════════════════
# COMUNICAÇÃO COM O OPENSEARCH
#
# O OpenSearch expõe uma API REST — análoga a uma API HTTP normal.
# Cada "índice" é como uma tabela, cada "documento" é como uma linha.
#
# Operações principais que usamos:
#   PUT  /<índice>          → cria o índice com mapeamento (schema)
#   DELETE /<índice>        → apaga o índice inteiro
#   POST /_bulk             → insere muitos documentos de uma vez (batch insert)
# ══════════════════════════════════════════════════════════════════════════════

def create_index(client: httpx.Client):
    """
    Cria o índice com mapeamento explícito de tipos.

    Mapeamento no OpenSearch = schema no Postgres.
    Se você não definir, o OpenSearch infere os tipos automaticamente
    (igual ao ORM com auto-migration), mas definir explicitamente evita
    surpresas — principalmente para campos que serão usados em buscas.

    Tipo "keyword" = string exata (para filtros/aggregations)
    Tipo "text"    = string analisada (para full-text search — tokeniza, remove stopwords, etc.)
    Tipo "date"    = timestamp (reconhece ISO 8601 automaticamente)
    """
    mapping = {
        "mappings": {
            "properties": {
                "@timestamp": {"type": "date"},

                # "log" como "text" = campo full-text searchable
                # Isso significa que o OpenSearch vai tokenizar o conteúdo,
                # permitindo buscar por palavras individuais como "OOMKilled" ou "timeout"
                # (comportamento dos clusters onprem do artigo)
                "log":   {"type": "text"},

                # "level" como "keyword" = valor exato, ideal para filtros (level="ERROR")
                "level":  {"type": "keyword"},
                "stream": {"type": "keyword"},

                # Objeto aninhado — o OpenSearch indexa cada subcampo separadamente
                "kubernetes": {
                    "properties": {
                        # Todos "keyword" porque são usados em filtros exatos:
                        # "me mostre logs do namespace payments" (não full-text)
                        "namespace_name": {"type": "keyword"},
                        "pod_name":       {"type": "keyword"},
                        "container_name": {"type": "keyword"},
                        "host":           {"type": "keyword"},
                        "pod_ip":         {"type": "keyword"},
                        # Nota: "labels" não está no mapeamento explícito.
                        # O OpenSearch vai inferir os tipos dos subcampos de labels
                        # automaticamente (dynamic mapping).
                    }
                },
            }
        }
    }

    r = client.put(f"/{INDEX_NAME}", json=mapping)

    if r.status_code == 200:
        print(f"✓ Índice '{INDEX_NAME}' criado")
    elif r.status_code == 400 and "already_exists" in r.text:
        print(f"  Índice '{INDEX_NAME}' já existe — use --delete para recriar")
    else:
        print(f"  Aviso ao criar índice: {r.status_code} {r.text[:200]}")


def delete_index(client: httpx.Client):
    """
    Apaga o índice inteiro — equivalente a DROP TABLE no Postgres.
    Usado quando você quer recriar o índice do zero (ex: mudou o mapeamento).
    """
    r = client.delete(f"/{INDEX_NAME}")
    if r.status_code == 200:
        print(f"✓ Índice '{INDEX_NAME}' deletado")
    elif r.status_code == 404:
        print(f"  Índice '{INDEX_NAME}' não existia, nada a deletar")


def bulk_index(client: httpx.Client, logs: list[dict]) -> tuple[int, int]:
    """
    Indexa um lote de documentos usando a API _bulk.

    A API _bulk do OpenSearch usa um formato especial: NDJSON (Newline-Delimited JSON).
    Cada operação é definida por DOIS objetos JSON em linhas alternadas:
        linha ímpar: instrução (qual operação e em qual índice)
        linha par:   o documento em si

    Exemplo de payload para 2 documentos:
        {"index": {"_index": "container-logs-fake"}}   ← instrução
        {"@timestamp": "...", "log": "...", ...}        ← documento 1
        {"index": {"_index": "container-logs-fake"}}   ← instrução
        {"@timestamp": "...", "log": "...", ...}        ← documento 2

    Analogia: é como um executemany() do psycopg2 no Postgres —
    muito mais eficiente que INSERT um a um porque minimiza round-trips HTTP.

    Retorna: (sucessos, erros)
    """
    body_lines = []
    for doc in logs:
        # Linha de instrução: diz "insira este documento neste índice"
        body_lines.append(json.dumps({"index": {"_index": INDEX_NAME}}))
        # Linha de dados: o documento em si
        body_lines.append(json.dumps(doc))

    # O payload deve terminar com \n (requisito da API _bulk)
    payload = "\n".join(body_lines) + "\n"

    # Content-Type especial para NDJSON — diferente do application/json normal
    r = client.post("/_bulk", content=payload, headers={"Content-Type": "application/x-ndjson"})
    r.raise_for_status()

    result = r.json()

    # A resposta lista o resultado de cada operação individualmente.
    # Contamos quantas tiveram erro (campo "error" presente no item)
    errors = [item for item in result.get("items", []) if "error" in item.get("index", {})]

    sucessos = len(result.get("items", [])) - len(errors)
    return sucessos, len(errors)


# ══════════════════════════════════════════════════════════════════════════════
# PONTO DE ENTRADA
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Popula o OpenSearch local com logs falsos")
    parser.add_argument("--count",  type=int, default=DEFAULT_COUNT, help="Quantidade de logs a gerar")
    parser.add_argument("--delete", action="store_true",             help="Apaga e recria o índice antes de inserir")
    parser.add_argument("--url",    default=OPENSEARCH_URL,          help="URL do OpenSearch (padrão: localhost:9200)")
    args = parser.parse_args()

    # httpx.Client = sessão HTTP reutilizável — análogo ao requests.Session()
    # base_url: prefixo adicionado em todas as requisições
    # timeout: evita travar indefinidamente se o OpenSearch estiver lento
    client = httpx.Client(base_url=args.url, timeout=30.0)

    # Verifica conectividade antes de tentar qualquer coisa
    # GET /_cluster/health é o "ping" padrão do OpenSearch
    try:
        r = client.get("/_cluster/health")
        r.raise_for_status()
        status = r.json().get("status", "?")
        # status pode ser: "green" (tudo ok), "yellow" (réplicas pendentes), "red" (problema)
        print(f"✓ OpenSearch conectado | cluster status: {status}")
    except Exception as e:
        print(f"✗ Não foi possível conectar em {args.url}")
        print(f"  Erro: {e}")
        print(f"  → Suba o ambiente primeiro: docker compose up -d")
        return

    # Recriação limpa do índice (opcional, via --delete)
    if args.delete:
        delete_index(client)

    create_index(client)

    # Indexação em batches (lotes)
    # Por que batches? Para não sobrecarregar a memória local (se --count for grande)
    # e para dar feedback visual de progresso.
    # É o mesmo raciocínio de paginar INSERTs em migrações de banco de dados.
    batch_size = 100
    total_ok   = 0
    total_err  = 0

    print(f"\nIndexando {args.count} logs em batches de {batch_size}...")

    for i in range(0, args.count, batch_size):
        # Garante que o último batch não ultrapasse o --count total
        tamanho_batch = min(batch_size, args.count - i)
        batch = [generate_log() for _ in range(tamanho_batch)]

        ok, err = bulk_index(client, batch)
        total_ok  += ok
        total_err += err

        # Progresso: [  100/1000] ✓ 100 indexados  ✗ 0 erros
        print(f"  [{i + tamanho_batch:>5}/{args.count}] ✓ {ok} indexados  ✗ {err} erros")

    # Resumo final
    print(f"\n{'─' * 40}")
    print(f"Total indexado: {total_ok} logs")
    if total_err:
        print(f"Erros:          {total_err}")

    print(f"\nPróximos passos:")
    print(f"  Dashboard: http://localhost:5601  (crie um index pattern: {INDEX_NAME})")
    print(f"  API direta: {args.url}/{INDEX_NAME}/_search")
    print(f"  Contar erros: {args.url}/{INDEX_NAME}/_count?q=level:ERROR")


if __name__ == "__main__":
    main()