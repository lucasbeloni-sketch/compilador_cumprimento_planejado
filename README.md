# Compilador Cumprimento Planejado

Pipeline em Python que **consolida dados de múltiplas planilhas Google Sheets e arquivos CSV do Google Drive** em uma única planilha de destino, calculando métricas de cumprimento planejado. Roda de forma automática a cada 6 horas via **GitHub Actions** — sem intervenção manual.

---

## Sumário

- [Visão geral](#visão-geral)
- [Como funciona (os blocos)](#como-funciona-os-blocos)
- [Arquitetura do código](#arquitetura-do-código)
- [Execução automática (GitHub Actions)](#execução-automática-github-actions)
- [Configuração](#configuração)
- [Rodando localmente](#rodando-localmente)
- [Testes](#testes)
- [Resiliência e tratamento de erros](#resiliência-e-tratamento-de-erros)

---

## Visão geral

O processo lê várias planilhas de origem (cadastradas numa aba de configuração), extrai e transforma os dados, e grava o resultado consolidado em abas específicas da planilha de destino. Tudo é orquestrado em **quatro blocos** que rodam em ordem, pois um depende do resultado do outro.

```
   CSVs no Drive ─┐
                  ├──► [Bloco 0]  BANCO.csv  +  BD_ConsultaServ   (mapas de métricas)
   Planilhas      │
   de origem  ────┼──► [Bloco 2]  REPROGRAMADAS  (mapa de lookup)
   (Google        │
    Sheets)   ────┼──► [Bloco 3]  PLAN_PRINCIPAL (mapa de lookup)
                  │
                  └──► [Bloco 1]  GERAL  ◄── usa mapas dos blocos 0/2/3
                                            (escreve dados + métricas J:N + lookup O:Q)
                                  │
                                  └──► atualiza timestamp final
```

Os dados de origem e destino vivem todos no Google Workspace; o script apenas **lê, transforma e escreve** — não guarda estado próprio.

---

## Como funciona (os blocos)

A ordem de execução é **0 → 2 → 3 → 1**. Os blocos 0/2/3 produzem *mapas* (estruturas de lookup em memória) que o Bloco 1 consome para calcular a aba `GERAL`.

| Bloco | Entrada | Saída | O que faz |
|-------|---------|-------|-----------|
| **0** | CSVs de uma pasta do Drive | `BANCO.csv` (Drive) + aba `BD_ConsultaServ` | Lê todos os CSVs, normaliza datas (infere DMY/MDY por arquivo) e números pt-BR, monta colunas auxiliares H/I/J/K e os mapas de soma/busca usados para as métricas da `GERAL`. |
| **2** | Aba `Reprogramadas` de cada origem | Aba `REPROGRAMADAS` | Consolida todas as linhas, monta a chave de lookup (coluna L) e regrava a aba. |
| **3** | CSVs do Drive + aba `Plan_Principal` de cada origem | Aba `PLAN_PRINCIPAL` | Igual ao Bloco 2, mas também incorpora CSVs. Produz o mapa principal de lookup. |
| **1** | Aba `Plan_Principal` filtrada pela data de referência | Aba `GERAL` | Para a data de referência (célula `Config!B2`), substitui o bloco daquela data na `GERAL`, recalcula as métricas `J:N` (a partir dos mapas do Bloco 0) e o lookup `O:Q` (a partir dos mapas dos Blocos 2/3). |

### Detalhes que valem destacar

- **Chave de lookup**: cada linha gera uma chave concatenando colunas (`A & B & C & E`) usada para cruzar dados entre as abas.
- **Datas como serial**: datas são convertidas para o serial do Google Sheets (dias desde `1899-12-30`) para casar chaves de forma consistente.
- **Coluna G é percentual, não moeda**: gravada como número puro (ex.: `117`) com formato `0"%"` — o Sheets exibe `117%` sem multiplicar por 100. As colunas E e F são moeda (`R$`).
- **Escrita segura**: nos Blocos 2/3 o script grava por cima e **só então** limpa as sobras abaixo. Se a escrita falhar, a aba nunca fica vazia (evita quebrar os lookups num run parcial).

---

## Arquitetura do código

O projeto foi dividido de um monólito original em módulos com responsabilidades claras:

| Arquivo | Responsabilidade |
|---------|------------------|
| `compilador.py` | **Ponto de entrada.** Orquestra os blocos na ordem 0→2→3→1 e atualiza o timestamp final. |
| `config.py` | Todas as constantes: IDs de planilhas/pastas, ranges, índices de colunas, parâmetros de retry. |
| `parsers.py` | Funções **puras** de conversão e transformação (datas, números pt-BR, moeda, percentual, chaves, helpers do Bloco 0). Cobertas por testes. |
| `google_io.py` | Autenticação, *retry* com backoff e todas as chamadas às APIs do Google (Sheets e Drive). |
| `blocos.py` | Orquestração de cada bloco (a lógica de negócio de alto nível). |
| `util.py` | Utilitário de log (`print` com `flush`, para log em tempo real no Actions). |
| `tests/test_parsers.py` | Testes unitários das funções puras de `parsers.py`. |

A separação isola o que é **testável sem rede** (`parsers.py`) do que **toca a API** (`google_io.py`), o que mantém a suíte de testes rápida e confiável.

---

## Execução automática (GitHub Actions)

O workflow [`.github/workflows/run_compilador_cumprimento_planejado.yml`](.github/workflows/run_compilador_cumprimento_planejado.yml) roda em duas situações:

- **Agendado**: a cada 6 horas (cron `0 */6 * * *`) — às 00:00, 06:00, 12:00 e 18:00 **UTC**.
- **Manual**: pelo botão *Run workflow* (`workflow_dispatch`) na aba Actions.

> ℹ️ O cron do GitHub usa **UTC**. Para o horário de Brasília (UTC−3), os disparos são aproximadamente 21:00, 03:00, 09:00 e 15:00.

Cada run:

1. Faz checkout do repositório.
2. Configura Python 3.11.
3. Instala as dependências de `requirements.txt` + `pytest`.
4. **Roda os testes** (`pytest -q`) — se falharem, o run para antes de tocar nas planilhas.
5. Executa o compilador (`python -u compilador.py`).

O job tem `timeout-minutes: 90` e usa `concurrency` para garantir que **dois runs nunca rodem ao mesmo tempo** (evita corrida de escrita na planilha).

---

## Configuração

### Secret obrigatório

O script autentica via **conta de serviço** do Google. No repositório, configure o secret:

| Secret | Conteúdo |
|--------|----------|
| `GOOGLE_CREDENTIALS_B64` | JSON da conta de serviço **codificado em Base64**. |

Para gerar o valor:

```bash
base64 -w0 service_account.json    # Linux
base64 -i service_account.json     # macOS
```

> A conta de serviço precisa ter acesso de edição à planilha de destino e às planilhas/pastas de origem (compartilhe o e‑mail da conta de serviço nesses arquivos).

A função de autenticação aceita, em ordem de preferência:
1. `GOOGLE_CREDENTIALS_B64` (Base64) — usado no Actions.
2. `GOOGLE_CREDENTIALS` (JSON puro em variável de ambiente).
3. Arquivo local `service_account.json` (para rodar na sua máquina).

### Planilha de destino

Os IDs de planilhas/pastas e ranges ficam em `config.py`. A planilha de destino (`DESTINO_ID`) deve conter as abas: `Config`, `BD_ConsultaServ`, `REPROGRAMADAS`, `PLAN_PRINCIPAL` e `GERAL`.

Na aba **`Config`**:
- `B2` — **data de referência** usada pelo Bloco 1.
- `B4:B50` — **IDs das planilhas de origem** (um por linha; duplicados são ignorados).
- `B1` — preenchida automaticamente com o **timestamp** do último run (fuso `America/Sao_Paulo`).

---

## Rodando localmente

```bash
# 1. Instalar dependências
python -m pip install -r requirements.txt

# 2. Colocar o service_account.json na raiz do projeto
#    (ou exportar GOOGLE_CREDENTIALS_B64 / GOOGLE_CREDENTIALS)

# 3. Executar
python -u compilador.py
```

Requer **Python 3.11**. Dependências principais (`requirements.txt`):

```
google-api-python-client>=2.100,<3
gspread>=6.2,<7
google-auth>=2.40,<3
pandas>=2.2,<2.4
```

---

## Testes

A lógica de transformação pura é coberta por testes unitários (não tocam a rede):

```bash
pip install pytest
pytest -q
```

Os testes cobrem conversão de datas (BR/ISO/serial), números pt-BR, moeda, percentual, chaves de lookup e os helpers do Bloco 0.

---

## Resiliência e tratamento de erros

- **Retry com backoff exponencial**: chamadas à API que falham por erro temporário (429, 5xx, quota) são repetidas até `MAX_TENTATIVAS_API` (10) vezes, com espera crescente (15s → até 120s) e *jitter* aleatório. Erros não temporários falham na hora.
- **Origens com problema são ignoradas**: se uma planilha de origem ou um CSV der erro, o run loga o aviso e segue para a próxima — um arquivo ruim não derruba todo o processo.
- **Falha por etapa**: cada bloco roda dentro de `executar_etapa`, que loga **onde** falhou. Como os blocos são interdependentes, qualquer falha encerra o run com código de saída ≠ 0 (o Actions marca como vermelho).
- **Cache em memória**: planilhas e leituras de origem são cacheadas dentro de um run, evitando ler a mesma aba mais de uma vez.

---

*Mantido por Sirtec. Dúvidas sobre o processo: verifique os logs do run mais recente na aba **Actions** do GitHub.*
