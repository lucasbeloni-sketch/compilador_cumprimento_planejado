import os
import sys
import json
import base64
import re
import csv
import io
import time
import random
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

import gspread
from gspread.exceptions import APIError, WorksheetNotFound
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# Força logs em tempo real no GitHub Actions
try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass


# ==========================
# CONFIGURAÇÕES GERAIS
# ==========================

DESTINO_ID = "1x7-AjwlFgVmrjcHqFVypBdcN4_DoRaGYPy2ByxJvs1w"

CONFIG_ABA = "Config"
CELULA_DATA_REFERENCIA = "B2"
CELULA_TIMESTAMP_FINAL = "B1"

# Evita ler a coluna B inteira.
# Ajuste o B50 caso futuramente tenha mais de 47 IDs.
RANGE_IDS_ORIGEM = "B4:B50"

TIMEZONE = "America/Sao_Paulo"

PASTA_CSV_BLOCO_3_ID = "1f5Z0f73IZD4rBEssNb9OVtADLVZzttaF"

ORIGEM_RANGE = "B6:BE"

QTD_COLUNAS_ORIGEM_RANGE = 56

# Bloco 1: GERAL usa A:I
QTD_COLUNAS_DESTINO_GERAL = 9
DESTINO_RANGE_GERAL = "A4:I"

# Blocos 2 e 3 usam A:K
# J e K recebem os dados extras que antes estavam indo para O e P
QTD_COLUNAS_DESTINO_COMPLETO = 11
DESTINO_RANGE_COMPLETO = "A4:K"

TAMANHO_BLOCO_ESCRITA = 10000

PAUSA_APOS_LEITURA = 0.5
PAUSA_APOS_ESCRITA = 1.0

# Retry reforçado para erro 429/503 da API Google
MAX_TENTATIVAS_API = 10
ESPERA_INICIAL_429 = 15
ESPERA_MAXIMA_429 = 120

# Índices relativos ao intervalo base.
# Para Sheets: B:BE
# Para CSV: A:BD
COLUNAS_ORIGEM_SELECIONADAS = [
    0,   # Sheets: B  | CSV: A
    5,   # Sheets: G  | CSV: F
    6,   # Sheets: H  | CSV: G
    11,  # Sheets: M  | CSV: L
    36,  # Sheets: AL | CSV: AK
    37,  # Sheets: AM | CSV: AL
    38,  # Sheets: AN | CSV: AM
    46,  # Sheets: AV | CSV: AU
    55,  # Sheets: BE | CSV: BD
]

COLUNA_ORIGEM_EXTRA_1 = 45  # Sheets: AU | CSV: AT
COLUNA_ORIGEM_EXTRA_2 = 47  # Sheets: AW | CSV: AV

COLUNA_DATA_DESTINO = 0

COLUNAS_MOEDA_DESTINO = [
    4,  # E
    5,  # F
    6,  # G
]


# ==========================
# LOG
# ==========================

def log(msg):
    print(msg, flush=True)


# ==========================
# RETRY / CONTROLE DE COTA
# ==========================

def erro_temporario_api(erro):
    texto = str(erro).lower()

    status = None

    if isinstance(erro, APIError):
        response = getattr(erro, "response", None)
        status = getattr(response, "status_code", None)

    if isinstance(erro, HttpError):
        status = getattr(erro.resp, "status", None)

    if status in [429, 500, 502, 503, 504]:
        return True

    termos = [
        "quota exceeded",
        "read requests per minute",
        "write requests per minute",
        "rate limit",
        "backend error",
        "internal error",
        "service unavailable",
        "429",
        "500",
        "502",
        "503",
        "504",
    ]

    return any(termo in texto for termo in termos)


def executar_com_retry(funcao, descricao="operação Google API"):
    ultimo_erro = None

    for tentativa in range(1, MAX_TENTATIVAS_API + 1):
        try:
            return funcao()

        except Exception as erro:
            ultimo_erro = erro

            if not erro_temporario_api(erro):
                raise

            if tentativa == MAX_TENTATIVAS_API:
                log(
                    f"Erro temporário/API quota em '{descricao}' persistiu após "
                    f"{MAX_TENTATIVAS_API} tentativas."
                )
                break

            espera = min(
                ESPERA_MAXIMA_429,
                ESPERA_INICIAL_429 * (2 ** (tentativa - 1))
            )

            espera += random.uniform(3, 10)

            log(
                f"Aviso: erro temporário/API quota em '{descricao}'. "
                f"Tentativa {tentativa}/{MAX_TENTATIVAS_API}. "
                f"Aguardando {espera:.1f}s antes de tentar novamente. "
                f"Erro: {erro}"
            )

            time.sleep(espera)

    raise ultimo_erro


# ==========================
# AUTENTICAÇÃO
# ==========================

def autenticar_google():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    credenciais_b64 = os.getenv("GOOGLE_CREDENTIALS_B64")
    credenciais_json = os.getenv("GOOGLE_CREDENTIALS")

    if credenciais_b64:
        info = json.loads(base64.b64decode(credenciais_b64).decode("utf-8"))
    elif credenciais_json:
        info = json.loads(credenciais_json)
    elif os.path.exists("service_account.json"):
        with open("service_account.json", "r", encoding="utf-8") as f:
            info = json.load(f)
    else:
        raise Exception(
            "Credenciais não encontradas. Configure o secret GOOGLE_CREDENTIALS_B64 no GitHub Actions."
        )

    creds = Credentials.from_service_account_info(info, scopes=scopes)

    gc = gspread.authorize(creds)

    drive_service = build(
        "drive",
        "v3",
        credentials=creds,
        cache_discovery=False
    )

    return gc, drive_service


# ==========================
# FUNÇÕES AUXILIARES
# ==========================

def normalizar_linha(linha, qtd_colunas):
    linha = list(linha)

    if len(linha) < qtd_colunas:
        linha += [""] * (qtd_colunas - len(linha))

    return linha[:qtd_colunas]


def linha_tem_dados(linha):
    return any(str(celula).strip() != "" for celula in linha)


def remover_linhas_vazias_base(dados, nome_bloco=""):
    filtradas = []
    removidas = 0

    for linha in dados:
        base = linha[:QTD_COLUNAS_DESTINO_GERAL]

        if linha_tem_dados(base):
            filtradas.append(linha)
        else:
            removidas += 1

    if nome_bloco:
        log(f"Linhas vazias removidas no {nome_bloco}: {removidas}")
    else:
        log(f"Linhas vazias removidas: {removidas}")

    return filtradas


def converter_para_data(valor):
    if valor is None:
        return None

    if isinstance(valor, date) and not isinstance(valor, datetime):
        return valor

    if isinstance(valor, datetime):
        return valor.date()

    if isinstance(valor, (int, float)):
        try:
            return date(1899, 12, 30) + timedelta(days=int(valor))
        except Exception:
            return None

    texto = str(valor).strip()

    if not texto:
        return None

    texto = texto.replace("\u00a0", " ")
    texto_sem_hora = texto.split(" ")[0].strip()

    try:
        numero = float(texto_sem_hora.replace(",", "."))
        if numero > 30000:
            return date(1899, 12, 30) + timedelta(days=int(numero))
    except Exception:
        pass

    formatos = [
        "%d/%m/%Y",
        "%d/%m/%y",
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%Y/%m/%d",
    ]

    for formato in formatos:
        try:
            return datetime.strptime(texto_sem_hora, formato).date()
        except Exception:
            continue

    return None


def data_para_serial_google_sheets(data_valor):
    return (data_valor - date(1899, 12, 30)).days


def converter_moeda_para_numero(valor):
    if valor is None:
        return ""

    if isinstance(valor, (int, float)):
        return valor

    texto = str(valor).strip()

    if texto in ["", "-", "—"]:
        return ""

    negativo = False

    if texto.startswith("(") and texto.endswith(")"):
        negativo = True
        texto = texto[1:-1]

    texto = texto.replace("R$", "")
    texto = texto.replace(" ", "")
    texto = texto.replace("\u00a0", "")
    texto = re.sub(r"[^0-9,.\-]", "", texto)

    if not texto:
        return ""

    if texto.startswith("-"):
        negativo = True
        texto = texto.replace("-", "")

    if "," in texto and "." in texto:
        if texto.rfind(",") > texto.rfind("."):
            texto = texto.replace(".", "").replace(",", ".")
        else:
            texto = texto.replace(",", "")
    elif "," in texto:
        texto = texto.replace(".", "").replace(",", ".")
    elif "." in texto:
        partes = texto.split(".")
        if len(partes[-1]) == 3 and len(partes) > 1:
            texto = texto.replace(".", "")

    try:
        numero = float(texto)
        return -numero if negativo else numero
    except Exception:
        return valor


def eh_data_referencia(valor, data_referencia):
    data_valor = converter_para_data(valor)
    return data_valor == data_referencia


def selecionar_colunas_origem_base(linha):
    linha = normalizar_linha(linha, QTD_COLUNAS_ORIGEM_RANGE)

    return [
        linha[indice] if indice < len(linha) else ""
        for indice in COLUNAS_ORIGEM_SELECIONADAS
    ]


def selecionar_colunas_origem_com_extra(linha):
    linha = normalizar_linha(linha, QTD_COLUNAS_ORIGEM_RANGE)

    base_a_i = [
        linha[indice] if indice < len(linha) else ""
        for indice in COLUNAS_ORIGEM_SELECIONADAS
    ]

    extra_j_k = [
        linha[COLUNA_ORIGEM_EXTRA_1] if COLUNA_ORIGEM_EXTRA_1 < len(linha) else "",
        linha[COLUNA_ORIGEM_EXTRA_2] if COLUNA_ORIGEM_EXTRA_2 < len(linha) else "",
    ]

    return base_a_i + extra_j_k


def preparar_linha_para_envio(linha, qtd_colunas_destino):
    linha = normalizar_linha(linha, qtd_colunas_destino)

    data_valor = converter_para_data(linha[COLUNA_DATA_DESTINO])

    if data_valor:
        linha[COLUNA_DATA_DESTINO] = data_para_serial_google_sheets(data_valor)

    for indice in COLUNAS_MOEDA_DESTINO:
        if indice < len(linha):
            linha[indice] = converter_moeda_para_numero(linha[indice])

    return linha


def garantir_linhas_suficientes(aba, ultima_linha_necessaria):
    linhas_atuais = aba.row_count

    if ultima_linha_necessaria > linhas_atuais:
        executar_com_retry(
            lambda: aba.add_rows(ultima_linha_necessaria - linhas_atuais),
            descricao=f"adicionar linhas na aba {aba.title}"
        )


def escrever_em_blocos(
    aba,
    dados,
    linha_inicial=4,
    coluna_inicial="A",
    tamanho_bloco=TAMANHO_BLOCO_ESCRITA
):
    if not dados:
        return

    total = len(dados)

    for i in range(0, total, tamanho_bloco):
        bloco = dados[i:i + tamanho_bloco]
        linha_destino = linha_inicial + i
        celula_inicio = f"{coluna_inicial}{linha_destino}"

        log(
            f"Escrevendo bloco em {aba.title}!{celula_inicio} "
            f"({i + 1} até {min(i + tamanho_bloco, total)} de {total})"
        )

        executar_com_retry(
            lambda bloco=bloco, celula_inicio=celula_inicio: aba.update(
                values=bloco,
                range_name=celula_inicio,
                value_input_option="USER_ENTERED"
            ),
            descricao=f"escrita em {aba.title}!{celula_inicio}"
        )

        time.sleep(PAUSA_APOS_ESCRITA)


def aplicar_formatacao_destino(planilha_destino, aba_destino):
    sheet_id = aba_destino.id

    requests = []

    def adicionar_formatacao_coluna(coluna_inicio, coluna_fim, tipo, padrao):
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 3,
                    "startColumnIndex": coluna_inicio,
                    "endColumnIndex": coluna_fim
                },
                "cell": {
                    "userEnteredFormat": {
                        "numberFormat": {
                            "type": tipo,
                            "pattern": padrao
                        }
                    }
                },
                "fields": "userEnteredFormat.numberFormat"
            }
        })

    adicionar_formatacao_coluna(
        coluna_inicio=0,
        coluna_fim=1,
        tipo="DATE",
        padrao="dd/mm/yyyy"
    )

    for coluna in COLUNAS_MOEDA_DESTINO:
        adicionar_formatacao_coluna(
            coluna_inicio=coluna,
            coluna_fim=coluna + 1,
            tipo="CURRENCY",
            padrao='"R$" #,##0.00'
        )

    if requests:
        executar_com_retry(
            lambda: planilha_destino.batch_update({"requests": requests}),
            descricao=f"aplicar formatação na aba {aba_destino.title}"
        )


def atualizar_timestamp_final(aba_config):
    agora = datetime.now(ZoneInfo(TIMEZONE))
    timestamp = agora.strftime("%d/%m/%Y %H:%M:%S")

    log(f"Atualizando timestamp final em {CONFIG_ABA}!{CELULA_TIMESTAMP_FINAL}: {timestamp}")

    executar_com_retry(
        lambda: aba_config.update(
            range_name=CELULA_TIMESTAMP_FINAL,
            values=[[timestamp]],
            value_input_option="USER_ENTERED"
        ),
        descricao=f"atualizar timestamp em {CONFIG_ABA}!{CELULA_TIMESTAMP_FINAL}"
    )


# ==========================
# BLOCO 1 OTIMIZADO - GERAL
# ==========================

def localizar_bloco_data_geral(aba_destino, data_referencia):
    log("Lendo somente GERAL!A4:A para localizar a data de referência...")

    valores_coluna_a = executar_com_retry(
        lambda: aba_destino.get(
            "A4:A",
            value_render_option="FORMATTED_VALUE"
        ),
        descricao="ler GERAL!A4:A"
    )

    time.sleep(PAUSA_APOS_LEITURA)

    linhas_data_referencia = []
    primeira_linha_mais_antiga = None
    ultima_linha_com_data = 3

    for i, linha in enumerate(valores_coluna_a):
        numero_linha = 4 + i
        valor = linha[0] if linha else ""

        if str(valor).strip():
            ultima_linha_com_data = numero_linha

        data_linha = converter_para_data(valor)

        if data_linha == data_referencia:
            linhas_data_referencia.append(numero_linha)

        elif (
            primeira_linha_mais_antiga is None
            and data_linha is not None
            and data_linha < data_referencia
        ):
            primeira_linha_mais_antiga = numero_linha

    if linhas_data_referencia:
        linhas_ordenadas = sorted(linhas_data_referencia)

        linha_inicio = linhas_ordenadas[0]
        linha_fim = linhas_ordenadas[-1]
        quantidade = linha_fim - linha_inicio + 1

        quantidade_exata = len(linhas_ordenadas)

        if quantidade != quantidade_exata:
            log(
                "Aviso: a data de referência aparece em linhas não contínuas na GERAL. "
                "O script vai considerar o bloco entre a primeira e a última ocorrência."
            )

        log(
            f"Bloco encontrado na GERAL para a data: "
            f"linhas {linha_inicio} até {linha_fim} ({quantidade} linhas)."
        )

        return {
            "existe": True,
            "linha_inicio": linha_inicio,
            "linha_fim": linha_fim,
            "quantidade": quantidade,
            "linha_insercao": linha_inicio
        }

    if primeira_linha_mais_antiga:
        linha_insercao = primeira_linha_mais_antiga
    else:
        linha_insercao = ultima_linha_com_data + 1

    if linha_insercao < 4:
        linha_insercao = 4

    log(
        f"Nenhum bloco existente para a data. "
        f"Inserção será feita a partir da linha {linha_insercao}."
    )

    return {
        "existe": False,
        "linha_inicio": None,
        "linha_fim": None,
        "quantidade": 0,
        "linha_insercao": linha_insercao
    }


def inserir_intervalo_celulas(planilha_destino, aba_destino, linha_inicio, quantidade_linhas, qtd_colunas):
    if quantidade_linhas <= 0:
        return

    garantir_linhas_suficientes(
        aba_destino,
        linha_inicio + quantidade_linhas + 5
    )

    sheet_id = aba_destino.id

    requests = [{
        "insertRange": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": linha_inicio - 1,
                "endRowIndex": linha_inicio - 1 + quantidade_linhas,
                "startColumnIndex": 0,
                "endColumnIndex": qtd_colunas
            },
            "shiftDimension": "ROWS"
        }
    }]

    executar_com_retry(
        lambda: planilha_destino.batch_update({"requests": requests}),
        descricao=f"inserir {quantidade_linhas} linhas/células em {aba_destino.title}"
    )


def deletar_intervalo_celulas(planilha_destino, aba_destino, linha_inicio, linha_fim, qtd_colunas):
    if linha_inicio is None or linha_fim is None or linha_fim < linha_inicio:
        return

    sheet_id = aba_destino.id

    requests = [{
        "deleteRange": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": linha_inicio - 1,
                "endRowIndex": linha_fim,
                "startColumnIndex": 0,
                "endColumnIndex": qtd_colunas
            },
            "shiftDimension": "ROWS"
        }
    }]

    executar_com_retry(
        lambda: planilha_destino.batch_update({"requests": requests}),
        descricao=f"deletar células {aba_destino.title}!A{linha_inicio}:I{linha_fim}"
    )


def limpar_intervalo_geral(aba_destino, linha_inicio, linha_fim):
    if linha_inicio is None or linha_fim is None or linha_fim < linha_inicio:
        return

    intervalo = f"A{linha_inicio}:I{linha_fim}"

    executar_com_retry(
        lambda: aba_destino.batch_clear([intervalo]),
        descricao=f"limpar GERAL!{intervalo}"
    )


def substituir_bloco_data_geral(planilha_destino, aba_destino, dados_novos, data_referencia):
    info_bloco = localizar_bloco_data_geral(
        aba_destino=aba_destino,
        data_referencia=data_referencia
    )

    qtd_nova = len(dados_novos)
    qtd_antiga = info_bloco["quantidade"]

    log(f"GERAL - quantidade antiga da data: {qtd_antiga}")
    log(f"GERAL - quantidade nova da data: {qtd_nova}")

    if qtd_antiga == 0 and qtd_nova == 0:
        log("Nada para substituir na GERAL.")
        return

    if qtd_antiga > 0 and qtd_nova == 0:
        log("Removendo bloco antigo da data, pois não há dados novos.")
        deletar_intervalo_celulas(
            planilha_destino=planilha_destino,
            aba_destino=aba_destino,
            linha_inicio=info_bloco["linha_inicio"],
            linha_fim=info_bloco["linha_fim"],
            qtd_colunas=QTD_COLUNAS_DESTINO_GERAL
        )
        return

    if qtd_antiga == 0 and qtd_nova > 0:
        linha_insercao = info_bloco["linha_insercao"]

        log(f"Inserindo novo bloco com {qtd_nova} linhas na GERAL a partir da linha {linha_insercao}.")

        inserir_intervalo_celulas(
            planilha_destino=planilha_destino,
            aba_destino=aba_destino,
            linha_inicio=linha_insercao,
            quantidade_linhas=qtd_nova,
            qtd_colunas=QTD_COLUNAS_DESTINO_GERAL
        )

        escrever_em_blocos(
            aba=aba_destino,
            dados=dados_novos,
            linha_inicial=linha_insercao,
            coluna_inicial="A"
        )

        return

    linha_inicio = info_bloco["linha_inicio"]

    if qtd_nova > qtd_antiga:
        diferenca = qtd_nova - qtd_antiga
        linha_inserir = linha_inicio + qtd_antiga

        log(f"Bloco novo maior. Inserindo {diferenca} linhas/células extras na GERAL.")

        inserir_intervalo_celulas(
            planilha_destino=planilha_destino,
            aba_destino=aba_destino,
            linha_inicio=linha_inserir,
            quantidade_linhas=diferenca,
            qtd_colunas=QTD_COLUNAS_DESTINO_GERAL
        )

    elif qtd_nova < qtd_antiga:
        diferenca = qtd_antiga - qtd_nova
        linha_delete_inicio = linha_inicio + qtd_nova
        linha_delete_fim = linha_inicio + qtd_antiga - 1

        log(f"Bloco novo menor. Removendo {diferenca} linhas/células antigas da GERAL.")

        deletar_intervalo_celulas(
            planilha_destino=planilha_destino,
            aba_destino=aba_destino,
            linha_inicio=linha_delete_inicio,
            linha_fim=linha_delete_fim,
            qtd_colunas=QTD_COLUNAS_DESTINO_GERAL
        )

    if dados_novos:
        linha_fim_nova = linha_inicio + len(dados_novos) - 1
        limpar_intervalo_geral(aba_destino, linha_inicio, linha_fim_nova)

        log(f"Atualizando somente o bloco da data na GERAL a partir da linha {linha_inicio}.")

        escrever_em_blocos(
            aba=aba_destino,
            dados=dados_novos,
            linha_inicial=linha_inicio,
            coluna_inicial="A"
        )


# ==========================
# GOOGLE SHEETS ORIGEM
# ==========================

def obter_planilha_origem(gc, origem_id, cache_planilhas):
    if origem_id in cache_planilhas:
        return cache_planilhas[origem_id]

    planilha = executar_com_retry(
        lambda: gc.open_by_key(origem_id),
        descricao=f"abrir planilha origem {origem_id}"
    )

    cache_planilhas[origem_id] = planilha

    return planilha


def ler_dados_google_sheet(gc, origem_id, aba_origem_nome, cache_planilhas, cache_dados):
    chave_cache = (origem_id, aba_origem_nome)

    if chave_cache in cache_dados:
        log(f"Usando cache Google Sheets: {origem_id} | Aba: {aba_origem_nome}")
        return cache_dados[chave_cache]

    log(f"Lendo origem Google Sheets: {origem_id} | Aba: {aba_origem_nome}")

    try:
        planilha_origem = obter_planilha_origem(
            gc=gc,
            origem_id=origem_id,
            cache_planilhas=cache_planilhas
        )

        aba_origem = executar_com_retry(
            lambda: planilha_origem.worksheet(aba_origem_nome),
            descricao=f"abrir aba {aba_origem_nome} na origem {origem_id}"
        )

        dados_origem = executar_com_retry(
            lambda: aba_origem.get(
                ORIGEM_RANGE,
                value_render_option="FORMATTED_VALUE"
            ),
            descricao=f"ler {aba_origem_nome}!{ORIGEM_RANGE} da origem {origem_id}"
        )

        dados_origem = [
            normalizar_linha(linha, QTD_COLUNAS_ORIGEM_RANGE)
            for linha in dados_origem
            if linha_tem_dados(linha)
        ]

        cache_dados[chave_cache] = dados_origem

        time.sleep(PAUSA_APOS_LEITURA)

        return dados_origem

    except WorksheetNotFound:
        log(f"Aba não encontrada na origem {origem_id}: {aba_origem_nome}")
        cache_dados[chave_cache] = []
        return []

    except Exception as erro:
        log(f"Erro ao processar a origem {origem_id}, aba {aba_origem_nome}: {erro}")
        log("Essa origem será ignorada e o processo seguirá para a próxima.")
        cache_dados[chave_cache] = []
        return []


def ler_ids_planilhas_origem(aba_config):
    valores = executar_com_retry(
        lambda: aba_config.get(
            RANGE_IDS_ORIGEM,
            value_render_option="FORMATTED_VALUE"
        ),
        descricao=f"ler IDs em {CONFIG_ABA}!{RANGE_IDS_ORIGEM}"
    )

    ids = []

    for linha in valores:
        if not linha:
            continue

        id_planilha = str(linha[0]).strip()

        if id_planilha:
            ids.append(id_planilha)

    ids_unicos = []
    vistos = set()

    for id_planilha in ids:
        if id_planilha not in vistos:
            ids_unicos.append(id_planilha)
            vistos.add(id_planilha)

    return ids_unicos


def ler_dados_origem_com_filtro_data(
    gc,
    origem_id,
    aba_origem_nome,
    data_referencia,
    cache_planilhas,
    cache_dados
):
    dados_origem = ler_dados_google_sheet(
        gc=gc,
        origem_id=origem_id,
        aba_origem_nome=aba_origem_nome,
        cache_planilhas=cache_planilhas,
        cache_dados=cache_dados
    )

    dados_filtrados = [
        linha
        for linha in dados_origem
        if eh_data_referencia(linha[0], data_referencia)
    ]

    dados_selecionados = [
        selecionar_colunas_origem_base(linha)
        for linha in dados_filtrados
    ]

    log(f"Linhas encontradas nessa origem: {len(dados_selecionados)}")

    return dados_selecionados


def ler_dados_origem_sem_filtro_com_extra(
    gc,
    origem_id,
    aba_origem_nome,
    cache_planilhas,
    cache_dados
):
    dados_origem = ler_dados_google_sheet(
        gc=gc,
        origem_id=origem_id,
        aba_origem_nome=aba_origem_nome,
        cache_planilhas=cache_planilhas,
        cache_dados=cache_dados
    )

    dados = []

    for linha in dados_origem:
        dados.append(selecionar_colunas_origem_com_extra(linha))

    log(f"Linhas encontradas nessa origem: {len(dados)}")

    return dados


# ==========================
# CSV DRIVE - BLOCO 3
# ==========================

def listar_arquivos_csv_drive(drive_service, pasta_id):
    log(f"Buscando arquivos CSV na pasta Drive: {pasta_id}")

    arquivos = []
    page_token = None

    query = (
        f"'{pasta_id}' in parents "
        f"and trashed = false "
        f"and (mimeType = 'text/csv' or name contains '.csv' or name contains '.CSV')"
    )

    while True:
        resposta = executar_com_retry(
            lambda page_token=page_token: drive_service.files().list(
                q=query,
                spaces="drive",
                fields="nextPageToken, files(id, name, mimeType, modifiedTime)",
                pageToken=page_token,
                pageSize=1000,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True
            ).execute(),
            descricao="listar CSVs no Google Drive"
        )

        arquivos.extend(resposta.get("files", []))
        page_token = resposta.get("nextPageToken")

        if not page_token:
            break

    arquivos = sorted(arquivos, key=lambda x: x.get("name", ""))

    log(f"Quantidade de CSVs encontrados: {len(arquivos)}")

    return arquivos


def baixar_csv_drive(drive_service, arquivo_id):
    conteudo = executar_com_retry(
        lambda: drive_service.files().get_media(
            fileId=arquivo_id,
            supportsAllDrives=True
        ).execute(),
        descricao=f"baixar CSV {arquivo_id}"
    )

    if isinstance(conteudo, str):
        return conteudo

    for encoding in ["utf-8-sig", "utf-8", "latin1", "cp1252"]:
        try:
            return conteudo.decode(encoding)
        except Exception:
            continue

    return conteudo.decode("utf-8", errors="ignore")


def detectar_delimitador_csv(texto_csv):
    amostra = texto_csv[:5000]

    qtd_ponto_virgula = amostra.count(";")
    qtd_virgula = amostra.count(",")

    if qtd_ponto_virgula >= qtd_virgula:
        return ";"

    return ","


def ler_linhas_csv(texto_csv):
    delimitador = detectar_delimitador_csv(texto_csv)

    leitor = csv.reader(
        io.StringIO(texto_csv),
        delimiter=delimitador
    )

    linhas = list(leitor)

    if not linhas:
        return []

    linhas_dados = linhas[1:]

    linhas_dados = [
        normalizar_linha(linha, QTD_COLUNAS_ORIGEM_RANGE)
        for linha in linhas_dados
        if linha_tem_dados(linha)
    ]

    return linhas_dados


def ler_dados_csvs_bloco_3(drive_service):
    log("")
    log("Lendo CSVs do Drive para o Bloco 3...")

    arquivos_csv = listar_arquivos_csv_drive(
        drive_service=drive_service,
        pasta_id=PASTA_CSV_BLOCO_3_ID
    )

    dados = []

    for arquivo in arquivos_csv:
        arquivo_id = arquivo.get("id")
        arquivo_nome = arquivo.get("name")

        log(f"Lendo CSV: {arquivo_nome}")

        try:
            texto_csv = baixar_csv_drive(
                drive_service=drive_service,
                arquivo_id=arquivo_id
            )

            linhas_csv = ler_linhas_csv(texto_csv)

            linhas_aproveitadas = 0
            linhas_vazias_selecao = 0

            for linha in linhas_csv:
                linha_destino = selecionar_colunas_origem_com_extra(linha)

                if not linha_tem_dados(linha_destino[:QTD_COLUNAS_DESTINO_GERAL]):
                    linhas_vazias_selecao += 1
                    continue

                dados.append(linha_destino)
                linhas_aproveitadas += 1

            log(f"Linhas lidas do CSV {arquivo_nome}: {len(linhas_csv)}")
            log(f"Linhas aproveitadas do CSV {arquivo_nome}: {linhas_aproveitadas}")
            log(f"Linhas vazias ignoradas do CSV {arquivo_nome}: {linhas_vazias_selecao}")

        except Exception as erro:
            log(f"Erro ao processar CSV {arquivo_nome}: {erro}")
            log("Esse CSV será ignorado e o processo seguirá para o próximo.")

    log(f"Total de linhas vindas dos CSVs no Bloco 3: {len(dados)}")

    return dados


# ==========================
# BLOCO 1
# ==========================

def executar_bloco_1(
    gc,
    planilha_destino,
    aba_config,
    ids_origem,
    cache_planilhas,
    cache_dados
):
    log("")
    log("======================================")
    log("INICIANDO BLOCO 1 - PLAN_PRINCIPAL > GERAL")
    log("======================================")

    aba_destino = executar_com_retry(
        lambda: planilha_destino.worksheet("GERAL"),
        descricao="abrir aba GERAL"
    )

    valor_data_referencia = executar_com_retry(
        lambda: aba_config.acell(
            CELULA_DATA_REFERENCIA,
            value_render_option="FORMATTED_VALUE"
        ).value,
        descricao=f"ler data em {CONFIG_ABA}!{CELULA_DATA_REFERENCIA}"
    )

    data_referencia = converter_para_data(valor_data_referencia)

    if not data_referencia:
        raise Exception(
            f"Não foi possível identificar uma data válida na célula {CELULA_DATA_REFERENCIA} "
            f"da aba {CONFIG_ABA}. Valor encontrado: {valor_data_referencia}"
        )

    log(f"Data de referência considerada no Bloco 1: {data_referencia.strftime('%d/%m/%Y')}")

    dados_data_referencia = []

    for origem_id in ids_origem:
        dados_origem = ler_dados_origem_com_filtro_data(
            gc=gc,
            origem_id=origem_id,
            aba_origem_nome="Plan_Principal",
            data_referencia=data_referencia,
            cache_planilhas=cache_planilhas,
            cache_dados=cache_dados
        )

        dados_data_referencia.extend(dados_origem)

    log(f"Total bruto de linhas consolidadas no Bloco 1: {len(dados_data_referencia)}")

    dados_data_referencia = [
        linha
        for linha in dados_data_referencia
        if linha_tem_dados(linha)
    ]

    log(f"Total final de linhas úteis no Bloco 1: {len(dados_data_referencia)}")

    dados_data_referencia = [
        preparar_linha_para_envio(linha, QTD_COLUNAS_DESTINO_GERAL)
        for linha in dados_data_referencia
    ]

    log("Aplicando formatação na aba GERAL...")

    aplicar_formatacao_destino(planilha_destino, aba_destino)

    substituir_bloco_data_geral(
        planilha_destino=planilha_destino,
        aba_destino=aba_destino,
        dados_novos=dados_data_referencia,
        data_referencia=data_referencia
    )

    log("Bloco 1 finalizado com sucesso.")


# ==========================
# BLOCO 2
# ==========================

def executar_bloco_2(
    gc,
    planilha_destino,
    ids_origem,
    cache_planilhas,
    cache_dados
):
    log("")
    log("======================================")
    log("INICIANDO BLOCO 2 - REPROGRAMADAS > REPROGRAMADAS")
    log("======================================")

    aba_destino = executar_com_retry(
        lambda: planilha_destino.worksheet("REPROGRAMADAS"),
        descricao="abrir aba REPROGRAMADAS"
    )

    dados = []

    for origem_id in ids_origem:
        dados_origem = ler_dados_origem_sem_filtro_com_extra(
            gc=gc,
            origem_id=origem_id,
            aba_origem_nome="Reprogramadas",
            cache_planilhas=cache_planilhas,
            cache_dados=cache_dados
        )

        dados.extend(dados_origem)

    log(f"Total bruto de linhas consolidadas no Bloco 2: {len(dados)}")

    dados = remover_linhas_vazias_base(
        dados=dados,
        nome_bloco="Bloco 2"
    )

    log(f"Total final de linhas úteis no Bloco 2: {len(dados)}")

    dados = [
        preparar_linha_para_envio(linha, QTD_COLUNAS_DESTINO_COMPLETO)
        for linha in dados
    ]

    log("Limpando A4:K da aba REPROGRAMADAS...")

    executar_com_retry(
        lambda: aba_destino.batch_clear([DESTINO_RANGE_COMPLETO]),
        descricao="limpar REPROGRAMADAS!A4:K"
    )

    log("Aplicando formatação na aba REPROGRAMADAS...")

    aplicar_formatacao_destino(planilha_destino, aba_destino)

    if dados:
        ultima_linha_necessaria = 3 + len(dados)
        garantir_linhas_suficientes(aba_destino, ultima_linha_necessaria)

        log("Gravando dados A:K na aba REPROGRAMADAS...")

        escrever_em_blocos(
            aba=aba_destino,
            dados=dados,
            linha_inicial=4,
            coluna_inicial="A"
        )
    else:
        log("Nenhum dado para gravar na aba REPROGRAMADAS.")

    log("Bloco 2 finalizado com sucesso.")


# ==========================
# BLOCO 3
# ==========================

def executar_bloco_3(
    gc,
    drive_service,
    planilha_destino,
    ids_origem,
    cache_planilhas,
    cache_dados
):
    log("")
    log("======================================")
    log("INICIANDO BLOCO 3 - CSVs + PLAN_PRINCIPAL > PLAN_PRINCIPAL")
    log("======================================")

    aba_destino = executar_com_retry(
        lambda: planilha_destino.worksheet("PLAN_PRINCIPAL"),
        descricao="abrir aba PLAN_PRINCIPAL"
    )

    dados = []

    dados_csv = ler_dados_csvs_bloco_3(
        drive_service=drive_service
    )

    dados.extend(dados_csv)

    log("")
    log("Lendo planilhas Google Sheets para o Bloco 3...")

    for origem_id in ids_origem:
        dados_origem = ler_dados_origem_sem_filtro_com_extra(
            gc=gc,
            origem_id=origem_id,
            aba_origem_nome="Plan_Principal",
            cache_planilhas=cache_planilhas,
            cache_dados=cache_dados
        )

        dados.extend(dados_origem)

    log(f"Total bruto de linhas consolidadas no Bloco 3: {len(dados)}")

    dados = remover_linhas_vazias_base(
        dados=dados,
        nome_bloco="Bloco 3"
    )

    log(f"Total final de linhas úteis no Bloco 3: {len(dados)}")

    dados = [
        preparar_linha_para_envio(linha, QTD_COLUNAS_DESTINO_COMPLETO)
        for linha in dados
    ]

    log("Limpando A4:K da aba PLAN_PRINCIPAL...")

    executar_com_retry(
        lambda: aba_destino.batch_clear([DESTINO_RANGE_COMPLETO]),
        descricao="limpar PLAN_PRINCIPAL!A4:K"
    )

    log("Aplicando formatação na aba PLAN_PRINCIPAL...")

    aplicar_formatacao_destino(planilha_destino, aba_destino)

    if dados:
        ultima_linha_necessaria = 3 + len(dados)
        garantir_linhas_suficientes(aba_destino, ultima_linha_necessaria)

        log("Gravando dados A:K na aba PLAN_PRINCIPAL...")

        escrever_em_blocos(
            aba=aba_destino,
            dados=dados,
            linha_inicial=4,
            coluna_inicial="A"
        )
    else:
        log("Nenhum dado para gravar na aba PLAN_PRINCIPAL.")

    log("Bloco 3 finalizado com sucesso.")


# ==========================
# PROCESSO PRINCIPAL
# ==========================

def main():
    log("Iniciando compilador...")

    gc, drive_service = autenticar_google()

    cache_planilhas = {}
    cache_dados = {}

    planilha_destino = executar_com_retry(
        lambda: gc.open_by_key(DESTINO_ID),
        descricao="abrir planilha destino"
    )

    aba_config = executar_com_retry(
        lambda: planilha_destino.worksheet(CONFIG_ABA),
        descricao="abrir aba Config"
    )

    ids_origem = ler_ids_planilhas_origem(aba_config)

    if not ids_origem:
        raise Exception(
            f"Nenhum ID de planilha de origem encontrado no intervalo {CONFIG_ABA}!{RANGE_IDS_ORIGEM}."
        )

    log(f"Quantidade de planilhas de origem encontradas: {len(ids_origem)}")

    executar_bloco_1(
        gc=gc,
        planilha_destino=planilha_destino,
        aba_config=aba_config,
        ids_origem=ids_origem,
        cache_planilhas=cache_planilhas,
        cache_dados=cache_dados
    )

    executar_bloco_2(
        gc=gc,
        planilha_destino=planilha_destino,
        ids_origem=ids_origem,
        cache_planilhas=cache_planilhas,
        cache_dados=cache_dados
    )

    executar_bloco_3(
        gc=gc,
        drive_service=drive_service,
        planilha_destino=planilha_destino,
        ids_origem=ids_origem,
        cache_planilhas=cache_planilhas,
        cache_dados=cache_dados
    )

    atualizar_timestamp_final(aba_config)

    log("")
    log("Processo completo finalizado com sucesso.")


if __name__ == "__main__":
    main()
