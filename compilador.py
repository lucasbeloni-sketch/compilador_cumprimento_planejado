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

import pandas as pd
import gspread
from gspread.exceptions import APIError, WorksheetNotFound
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload


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

RANGE_IDS_ORIGEM = "B4:B50"

TIMEZONE = "America/Sao_Paulo"

PASTA_CSV_BLOCO_3_ID = "1f5Z0f73IZD4rBEssNb9OVtADLVZzttaF"

ORIGEM_RANGE = "B6:BE"

QTD_COLUNAS_ORIGEM_RANGE = 56

QTD_COLUNAS_DESTINO_GERAL = 9

QTD_COLUNAS_DESTINO_COMPLETO = 11
QTD_COLUNAS_DESTINO_COMPLETO_COM_CHAVE = 12

DESTINO_RANGE_COMPLETO_COM_CHAVE = "A4:L"

TAMANHO_BLOCO_ESCRITA = 10000

PAUSA_APOS_LEITURA = 0.5
PAUSA_APOS_ESCRITA = 1.0

MAX_TENTATIVAS_API = 10
ESPERA_INICIAL_429 = 15
ESPERA_MAXIMA_429 = 120

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
# CONFIGURAÇÕES BLOCO 0
# ==========================

BLOCO0_NEW_FOLDER_ID = "1QHtqMNCcIzNihwnu3copkNmBZnaL6Z6z"
BLOCO0_OUTPUT_CSV_NAME = "BANCO.csv"
BLOCO0_UPLOAD_FOLDER_ID = "17IobcQeVLs83rUCqWKTi18yXiAPbupjf"
BLOCO0_SPREADSHEET_ID = DESTINO_ID
BLOCO0_SHEET_NAME = "BD_ConsultaServ"
BLOCO0_UPLOAD_BANCO_PARA_DRIVE = True

BLOCO0_READ_CSV_KWARGS = dict(
    dtype=str,
    encoding="utf-8-sig",
    sep=None,
    engine="python"
)

BLOCO0_KEEP_COL_POS_1BASED = [47, 6, 27, 50, 52, 68, 70]

BLOCO0_DATE_REGEX = r"(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4}|\d{4}[\/\-.]\d{1,2}[\/\-.]\d{1,2})"


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

    sheets_service = build(
        "sheets",
        "v4",
        credentials=creds,
        cache_discovery=False
    )

    return gc, drive_service, sheets_service


# ==========================
# FUNÇÕES AUXILIARES GERAIS
# ==========================

def normalizar_linha(linha, qtd_colunas):
    linha = list(linha)

    if len(linha) < qtd_colunas:
        linha += [""] * (qtd_colunas - len(linha))

    return linha[:qtd_colunas]


def linha_tem_dados(linha):
    return any(str(celula).strip() != "" for celula in linha)


def valor_para_chave(valor):
    if valor is None:
        return ""

    if isinstance(valor, float):
        if valor.is_integer():
            return str(int(valor))
        return str(valor)

    if isinstance(valor, int):
        return str(valor)

    return str(valor)


def calcular_chave_linha(linha):
    linha = normalizar_linha(linha, max(len(linha), 5))

    if str(linha[0]).strip() == "":
        return ""

    return (
        valor_para_chave(linha[0])
        + valor_para_chave(linha[1])
        + valor_para_chave(linha[2])
        + valor_para_chave(linha[4])
    )


def adicionar_chave_l(dados):
    dados_com_chave = []

    for linha in dados:
        chave = calcular_chave_linha(linha)
        dados_com_chave.append(linha + [chave])

    return dados_com_chave


def construir_mapa_lookup(dados):
    mapa = {}

    for linha in dados:
        linha = normalizar_linha(linha, QTD_COLUNAS_DESTINO_COMPLETO)

        chave = calcular_chave_linha(linha)

        if not chave:
            continue

        if chave not in mapa:
            mapa[chave] = [
                linha[9] if len(linha) > 9 else "",
                linha[10] if len(linha) > 10 else "",
            ]

    return mapa


def calcular_extras_geral(dados_geral, mapa_plan_principal, mapa_reprogramadas):
    extras = []

    for linha in dados_geral:
        chave = calcular_chave_linha(linha)

        if not chave:
            extras.append(["", "", ""])
            continue

        valores = mapa_plan_principal.get(chave)

        if valores is None:
            valores = mapa_reprogramadas.get(chave)

        if valores is None:
            valores = ["", ""]

        extras.append([
            valores[0] if len(valores) > 0 else "",
            valores[1] if len(valores) > 1 else "",
            chave
        ])

    return extras


def atualizar_lookup_geral_todas_linhas(
    aba_destino,
    mapa_plan_principal,
    mapa_reprogramadas
):
    log("Atualizando GERAL!O:Q para todas as linhas da aba GERAL...")

    dados_geral = executar_com_retry(
        lambda: aba_destino.get(
            "A4:E",
            value_render_option="UNFORMATTED_VALUE"
        ),
        descricao="ler GERAL!A4:E para atualizar O:Q"
    )

    time.sleep(PAUSA_APOS_LEITURA)

    dados_geral = [
        normalizar_linha(linha, 5)
        for linha in dados_geral
    ]

    ultima_linha_util = 0

    for i, linha in enumerate(dados_geral):
        if linha_tem_dados(linha):
            ultima_linha_util = i + 1

    if ultima_linha_util == 0:
        log("Nenhuma linha útil encontrada na GERAL para atualizar O:Q.")

        executar_com_retry(
            lambda: aba_destino.batch_clear(["O4:Q"]),
            descricao="limpar GERAL!O4:Q"
        )

        return

    dados_geral = dados_geral[:ultima_linha_util]

    extras_o_p_q = calcular_extras_geral(
        dados_geral=dados_geral,
        mapa_plan_principal=mapa_plan_principal,
        mapa_reprogramadas=mapa_reprogramadas
    )

    log(f"Total de linhas que serão atualizadas em GERAL!O:Q: {len(extras_o_p_q)}")

    executar_com_retry(
        lambda: aba_destino.batch_clear(["O4:Q"]),
        descricao="limpar GERAL!O4:Q"
    )

    escrever_em_blocos(
        aba=aba_destino,
        dados=extras_o_p_q,
        linha_inicial=4,
        coluna_inicial="O"
    )

    log("Atualização completa de GERAL!O:Q finalizada.")


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
# BLOCO 0 - BANCO.csv / BD_ConsultaServ
# ==========================

def bloco0_list_files(drive_service, folder_id, drive_id=None):
    query = f"'{folder_id}' in parents and trashed = false"
    files = []
    token = None

    while True:
        params = {
            "q": query,
            "pageToken": token,
            "pageSize": 1000,
            "fields": "nextPageToken, files(id,name,mimeType)",
            "supportsAllDrives": True,
            "includeItemsFromAllDrives": True,
        }

        if drive_id:
            params["corpora"] = "drive"
            params["driveId"] = drive_id
        else:
            params["corpora"] = "allDrives"

        resp = executar_com_retry(
            lambda params=params: drive_service.files().list(**params).execute(),
            descricao=f"listar arquivos da pasta {folder_id}"
        )

        files.extend(resp.get("files", []))
        token = resp.get("nextPageToken")

        if not token:
            break

    return files


def bloco0_download_file(drive_service, file_id, filename):
    request = drive_service.files().get_media(
        fileId=file_id,
        supportsAllDrives=True
    )

    with open(filename, "wb") as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False

        while not done:
            _, done = executar_com_retry(
                lambda: downloader.next_chunk(),
                descricao=f"baixar arquivo {filename}"
            )


def bloco0_find_file_in_folder(drive_service, folder_id, drive_id, filename):
    nome_seguro = filename.replace("'", "\\'")
    query = f"'{folder_id}' in parents and trashed = false and name = '{nome_seguro}'"

    params = {
        "q": query,
        "fields": "files(id,name)",
        "pageSize": 10,
        "supportsAllDrives": True,
        "includeItemsFromAllDrives": True,
    }

    if drive_id:
        params["corpora"] = "drive"
        params["driveId"] = drive_id
    else:
        params["corpora"] = "allDrives"

    resp = executar_com_retry(
        lambda: drive_service.files().list(**params).execute(),
        descricao=f"procurar {filename} no Drive"
    )

    files = resp.get("files", [])

    return files[0]["id"] if files else None


def bloco0_upload_or_update_banco(drive_service, folder_id, drive_id, local_path, filename):
    media = MediaFileUpload(
        local_path,
        mimetype="text/csv",
        resumable=True
    )

    existing_id = bloco0_find_file_in_folder(
        drive_service=drive_service,
        folder_id=folder_id,
        drive_id=drive_id,
        filename=filename
    )

    if existing_id:
        executar_com_retry(
            lambda: drive_service.files().update(
                fileId=existing_id,
                media_body=media,
                supportsAllDrives=True
            ).execute(),
            descricao=f"atualizar {filename} no Drive"
        )
        return "updated"

    executar_com_retry(
        lambda: drive_service.files().create(
            body={"name": filename, "parents": [folder_id]},
            media_body=media,
            supportsAllDrives=True
        ).execute(),
        descricao=f"criar {filename} no Drive"
    )

    return "created"


def bloco0_keep_only_columns_by_position(df, positions_1based):
    idx = [p - 1 for p in positions_1based]
    return df.iloc[:, idx]


def bloco0_to_number_ptbr(value):
    if value is None:
        return 0.0

    s = str(value).strip()

    if s == "" or s.lower() in ("nan", "none"):
        return 0.0

    s = s.replace(" ", "")

    if "," in s:
        s = s.replace(".", "").replace(",", ".")

    try:
        return float(s)
    except Exception:
        return 0.0


def bloco0_normalizar_texto(valor):
    if valor is None:
        return ""

    if isinstance(valor, float):
        if valor.is_integer():
            return str(int(valor)).strip()
        return str(valor).strip()

    if isinstance(valor, int):
        return str(valor).strip()

    texto = str(valor).strip()

    if texto.lower() in ("nan", "none"):
        return ""

    return texto


def bloco0_numero_para_chave(valor):
    """
    Simula o a*1 da fórmula:
    chave = c & a*1 & b
    """
    if valor is None:
        return ""

    if isinstance(valor, (int, float)):
        numero = float(valor)
        return str(int(numero)) if numero.is_integer() else str(numero)

    texto = str(valor).strip()

    if texto == "":
        return ""

    data_valor = converter_para_data(texto)

    if data_valor:
        serial = data_para_serial_google_sheets(data_valor)
        return str(serial)

    texto_num = texto.replace(" ", "")

    if "," in texto_num:
        texto_num = texto_num.replace(".", "").replace(",", ".")

    try:
        numero = float(texto_num)
        return str(int(numero)) if numero.is_integer() else str(numero)
    except Exception:
        return texto


def bloco0_montar_mapa_bd_serv_gpm(sheets_service):
    """
    Lê BD_Serv_GPM e monta mapas para simular SOMASES e PROCX.

    Base usada:
    BD_Serv_GPM!D:K

    Índices relativos:
    D = 0
    E = 1
    F = 2
    G = 3
    H = 4
    I = 5
    J = 6
    K = 7
    """

    log("[BLOCO 0] Lendo BD_Serv_GPM!D2:K para calcular J:N...")

    resp = executar_com_retry(
        lambda: sheets_service.spreadsheets().values().get(
            spreadsheetId=BLOCO0_SPREADSHEET_ID,
            range="BD_Serv_GPM!D2:K",
            valueRenderOption="FORMATTED_VALUE"
        ).execute(),
        descricao="ler BD_Serv_GPM!D2:K"
    )

    linhas = resp.get("values", [])

    soma_por_h_d_f = {}
    soma_por_j_d_f = {}
    soma_por_d_f = {}
    procx_i_para_e = {}
    procx_k_para_e = {}

    for linha in linhas:
        linha = normalizar_linha(linha, 8)

        valor_d = bloco0_normalizar_texto(linha[0])
        valor_e = linha[1]
        valor_f = bloco0_normalizar_texto(linha[2])
        valor_g = bloco0_to_number_ptbr(linha[3])
        valor_h = bloco0_normalizar_texto(linha[4])
        valor_i = bloco0_normalizar_texto(linha[5])
        valor_j = bloco0_normalizar_texto(linha[6])
        valor_k = bloco0_normalizar_texto(linha[7])

        chave_h = (valor_h, valor_d, valor_f)
        chave_j = (valor_j, valor_d, valor_f)
        chave_df = (valor_d, valor_f)

        soma_por_h_d_f[chave_h] = soma_por_h_d_f.get(chave_h, 0.0) + valor_g
        soma_por_j_d_f[chave_j] = soma_por_j_d_f.get(chave_j, 0.0) + valor_g
        soma_por_d_f[chave_df] = soma_por_d_f.get(chave_df, 0.0) + valor_g

        if valor_i and valor_i not in procx_i_para_e:
            procx_i_para_e[valor_i] = valor_e

        if valor_k and valor_k not in procx_k_para_e:
            procx_k_para_e[valor_k] = valor_e

    log(f"[BLOCO 0] Linhas lidas da BD_Serv_GPM: {len(linhas)}")

    return {
        "soma_por_h_d_f": soma_por_h_d_f,
        "soma_por_j_d_f": soma_por_j_d_f,
        "soma_por_d_f": soma_por_d_f,
        "procx_i_para_e": procx_i_para_e,
        "procx_k_para_e": procx_k_para_e,
    }


def bloco0_calcular_colunas_j_n(valores_a_i, mapas_bd_serv_gpm):
    """
    Calcula J:N como valores, simulando as fórmulas informadas.

    Entrada:
    valores_a_i = linhas com A:I

    Saída:
    lista com J:N
    """

    resultado = []

    soma_por_h_d_f = mapas_bd_serv_gpm["soma_por_h_d_f"]
    soma_por_j_d_f = mapas_bd_serv_gpm["soma_por_j_d_f"]
    soma_por_d_f = mapas_bd_serv_gpm["soma_por_d_f"]
    procx_i_para_e = mapas_bd_serv_gpm["procx_i_para_e"]
    procx_k_para_e = mapas_bd_serv_gpm["procx_k_para_e"]

    for linha in valores_a_i:
        linha = normalizar_linha(linha, 9)

        a = bloco0_normalizar_texto(linha[0])
        b = bloco0_normalizar_texto(linha[1])
        c = bloco0_normalizar_texto(linha[2])
        e_txt = linha[4]
        f_txt = linha[5]
        h = bloco0_normalizar_texto(linha[7])

        if a == "":
            resultado.append(["", "", "", "", ""])
            continue

        e_num = bloco0_to_number_ptbr(e_txt)
        f_num = bloco0_to_number_ptbr(f_txt)

        # Coluna J
        if e_num == 0:
            valor_j_calc = 0.0
        else:
            valor_j_calc = (
                soma_por_h_d_f.get((c, b, a), 0.0)
                + soma_por_j_d_f.get((c, b, a), 0.0)
            )

        # Coluna K
        if valor_j_calc <= 0:
            valor_k_calc = ""
        elif h != "" and h != c:
            valor_k_calc = 0
        elif e_num > 0:
            try:
                valor_k_calc = valor_j_calc / e_num
            except Exception:
                valor_k_calc = 0
        else:
            valor_k_calc = 1

        # Coluna L
        valor_l_calc = soma_por_d_f.get((b, a), 0.0)

        # Coluna M
        try:
            valor_m_calc = valor_l_calc / f_num if f_num != 0 else 0
        except Exception:
            valor_m_calc = 0

        # Coluna N
        chave_n = c + bloco0_numero_para_chave(a) + b
        valor_n_calc = procx_i_para_e.get(chave_n, "-")

        if valor_n_calc == "-":
            valor_n_calc = procx_k_para_e.get(chave_n, "-")

        resultado.append([
            valor_j_calc,
            valor_k_calc,
            valor_l_calc,
            valor_m_calc,
            valor_n_calc
        ])

    return resultado


def bloco0_extrair_data_string(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()

    s = (
        s.str.replace("\u200b", "", regex=False)
         .str.replace("\xa0", " ", regex=False)
         .str.replace(r"\s+", " ", regex=True)
         .str.replace("None", "", regex=False)
         .str.replace("nan", "", regex=False)
    )

    extracted = s.str.extract(BLOCO0_DATE_REGEX, expand=False)
    extracted = extracted.str.replace("-", "/", regex=False).str.replace(".", "/", regex=False)

    return extracted


def bloco0_inferir_formato_por_arquivo(extracted_dates: pd.Series) -> str:
    parts = extracted_dates.dropna().str.split("/", expand=True)

    if parts.empty or parts.shape[1] < 3:
        return "DMY"

    a = pd.to_numeric(parts[0], errors="coerce")
    b = pd.to_numeric(parts[1], errors="coerce")

    dmy_votes = ((a > 12) & (b <= 12)).sum()
    mdy_votes = ((b > 12) & (a <= 12)).sum()

    if dmy_votes == 0 and mdy_votes == 0:
        return "DMY"

    return "DMY" if dmy_votes >= mdy_votes else "MDY"


def bloco0_parse_date_por_arquivo(df: pd.DataFrame, col_data: str, col_arquivo: str) -> pd.Series:
    extracted = bloco0_extrair_data_string(df[col_data])

    def normalizar_ano(x: str) -> str:
        if not isinstance(x, str) or x.strip() == "":
            return x

        p = x.split("/")

        if len(p) != 3:
            return x

        if len(p[0]) == 4:
            return x

        if len(p[2]) == 2:
            return f"{p[0]}/{p[1]}/20{p[2]}"

        return x

    extracted = extracted.apply(normalizar_ano)

    parsed_final = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")

    for arquivo, idxs in df.groupby(col_arquivo).groups.items():
        ext_grp = extracted.loc[idxs]

        formato = bloco0_inferir_formato_por_arquivo(ext_grp)

        iso_mask = ext_grp.str.match(r"^\d{4}/\d{1,2}/\d{1,2}$", na=False)

        if iso_mask.any():
            parsed_final.loc[iso_mask.index[iso_mask]] = pd.to_datetime(
                ext_grp.loc[iso_mask.index[iso_mask]],
                errors="coerce",
                format="%Y/%m/%d"
            )

        rest_idx = ext_grp.index[~iso_mask]

        if len(rest_idx) > 0:
            dayfirst = True if formato == "DMY" else False
            parsed_final.loc[rest_idx] = pd.to_datetime(
                ext_grp.loc[rest_idx],
                errors="coerce",
                dayfirst=dayfirst
            )

        validas = pd.to_datetime(
            ext_grp,
            errors="coerce",
            dayfirst=(formato == "DMY")
        ).notna().sum()

        log(
            f"[BLOCO 0][DATA] arquivo_origem={arquivo} | "
            f"formato_inferido={formato} | amostras_validas={validas}"
        )

    return parsed_final


def bloco0_clear_range(sheets_service, spreadsheet_id, range_):
    executar_com_retry(
        lambda: sheets_service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=range_
        ).execute(),
        descricao=f"limpar range {range_}"
    )


def bloco0_upload_to_sheets(sheets_service, df):
    """
    Cola os dados do Bloco 0 em BD_ConsultaServ!A4:I
    e calcula J:N como valores, sem fórmulas.
    """

    df_sheets = df.copy()
    df_sheets = df_sheets.fillna("")

    # Garante 9 colunas para colar em A:I
    while df_sheets.shape[1] < 9:
        df_sheets[f"col_extra_{df_sheets.shape[1] + 1}"] = ""

    df_sheets = df_sheets.iloc[:, :9].copy()

    valores_a_i = df_sheets.values.tolist()

    mapas_bd_serv_gpm = bloco0_montar_mapa_bd_serv_gpm(sheets_service)

    valores_j_n = bloco0_calcular_colunas_j_n(
        valores_a_i=valores_a_i,
        mapas_bd_serv_gpm=mapas_bd_serv_gpm
    )

    bloco0_clear_range(
        sheets_service,
        BLOCO0_SPREADSHEET_ID,
        f"{BLOCO0_SHEET_NAME}!A4:N"
    )

    if valores_a_i:
        executar_com_retry(
            lambda: sheets_service.spreadsheets().values().update(
                spreadsheetId=BLOCO0_SPREADSHEET_ID,
                range=f"{BLOCO0_SHEET_NAME}!A4",
                valueInputOption="USER_ENTERED",
                body={"values": valores_a_i}
            ).execute(),
            descricao=f"gravar {BLOCO0_SHEET_NAME}!A4:I"
        )

        executar_com_retry(
            lambda: sheets_service.spreadsheets().values().update(
                spreadsheetId=BLOCO0_SPREADSHEET_ID,
                range=f"{BLOCO0_SHEET_NAME}!J4",
                valueInputOption="USER_ENTERED",
                body={"values": valores_j_n}
            ).execute(),
            descricao=f"gravar {BLOCO0_SHEET_NAME}!J4:N"
        )

    timestamp = datetime.now(ZoneInfo(TIMEZONE)).strftime("%d/%m/%Y %H:%M:%S")

    executar_com_retry(
        lambda: sheets_service.spreadsheets().values().update(
            spreadsheetId=BLOCO0_SPREADSHEET_ID,
            range=f"{BLOCO0_SHEET_NAME}!B1",
            valueInputOption="USER_ENTERED",
            body={"values": [[timestamp]]}
        ).execute(),
        descricao=f"atualizar timestamp {BLOCO0_SHEET_NAME}!B1"
    )


def executar_bloco_0(drive_service, sheets_service):
    log("")
    log("======================================")
    log("INICIANDO BLOCO 0 - BANCO.csv > BD_ConsultaServ")
    log("======================================")

    folder = executar_com_retry(
        lambda: drive_service.files().get(
            fileId=BLOCO0_NEW_FOLDER_ID,
            fields="id,name,driveId",
            supportsAllDrives=True
        ).execute(),
        descricao="abrir pasta de origem do Bloco 0"
    )

    drive_id_origem = folder.get("driveId")

    log(f"[BLOCO 0][OK] Pasta origem: {folder.get('name', BLOCO0_NEW_FOLDER_ID)}")

    files = bloco0_list_files(
        drive_service=drive_service,
        folder_id=BLOCO0_NEW_FOLDER_ID,
        drive_id=drive_id_origem
    )

    csv_files = [
        f for f in files
        if f["name"].lower().endswith(".csv")
        and f["name"] != BLOCO0_OUTPUT_CSV_NAME
    ]

    log(f"[BLOCO 0][INFO] CSVs encontrados: {len(csv_files)}")

    dfs = []
    temp_files = []

    for f in csv_files:
        nome_base = f["name"].replace("/", "_").replace("\\", "_")
        local_name = f"tmp_bloco0_{f['id']}_{nome_base}"

        bloco0_download_file(
            drive_service=drive_service,
            file_id=f["id"],
            filename=local_name
        )

        temp_files.append(local_name)

        try:
            df = pd.read_csv(local_name, **BLOCO0_READ_CSV_KWARGS)
            df["arquivo_origem"] = nome_base
            dfs.append(df)
            log(f"[BLOCO 0][OK] CSV lido: {nome_base} | linhas: {len(df)}")
        except Exception as e:
            log(f"[BLOCO 0][ERRO] {nome_base}: {e}")

    for f in temp_files:
        try:
            os.remove(f)
        except Exception:
            pass

    if not dfs:
        log("[BLOCO 0][ERRO] Nenhum CSV válido.")
        log("Bloco 0 finalizado sem dados.")
        return

    banco_df = pd.concat(dfs, ignore_index=True).drop_duplicates()

    origem_col = banco_df["arquivo_origem"].copy()

    banco_df = bloco0_keep_only_columns_by_position(
        banco_df,
        BLOCO0_KEEP_COL_POS_1BASED
    )

    banco_df.columns = [
        "centro_servico",
        "Nota",
        "cod_pep_obra",
        "equipe",
        "obs_servico",
        "dta_exec_srv",
        "total_servicos"
    ]

    banco_df["arquivo_origem"] = origem_col.values

    banco_df["cod_pep_obra"] = banco_df["cod_pep_obra"].fillna("").astype(str).str.upper()
    banco_df["total_servicos"] = banco_df["total_servicos"].apply(bloco0_to_number_ptbr)

    banco_df["dta_exec_srv"] = bloco0_parse_date_por_arquivo(
        banco_df,
        "dta_exec_srv",
        "arquivo_origem"
    )

    total = len(banco_df)
    validas = banco_df["dta_exec_srv"].notna().sum()
    invalidas = total - validas

    log(f"[BLOCO 0][DATA] Total: {total} | Válidas: {validas} | Inválidas: {invalidas}")

    banco_df = banco_df.sort_values(
        by="dta_exec_srv",
        ascending=True,
        kind="mergesort"
    ).reset_index(drop=True)

    banco_df["dta_exec_srv"] = banco_df["dta_exec_srv"].dt.strftime("%d/%m/%Y")

    banco_df.to_csv(
        BLOCO0_OUTPUT_CSV_NAME,
        index=False,
        encoding="utf-8-sig",
        sep=";",
        decimal=",",
        float_format="%.2f"
    )

    bloco0_upload_to_sheets(sheets_service, banco_df)

    if BLOCO0_UPLOAD_BANCO_PARA_DRIVE:
        upload_folder = executar_com_retry(
            lambda: drive_service.files().get(
                fileId=BLOCO0_UPLOAD_FOLDER_ID,
                fields="id,name,driveId",
                supportsAllDrives=True
            ).execute(),
            descricao="abrir pasta de upload do BANCO.csv"
        )

        drive_id_upload = upload_folder.get("driveId") or drive_id_origem

        action = bloco0_upload_or_update_banco(
            drive_service=drive_service,
            folder_id=BLOCO0_UPLOAD_FOLDER_ID,
            drive_id=drive_id_upload,
            local_path=BLOCO0_OUTPUT_CSV_NAME,
            filename=BLOCO0_OUTPUT_CSV_NAME
        )

        log(f"[BLOCO 0][OK] BANCO.csv enviado ao Drive ({action}).")

    log("Bloco 0 finalizado com sucesso.")


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
        descricao=f"deletar células {aba_destino.title}!A{linha_inicio}:Q{linha_fim}"
    )


def limpar_intervalos_geral(aba_destino, linha_inicio, linha_fim):
    if linha_inicio is None or linha_fim is None or linha_fim < linha_inicio:
        return

    intervalo_dados = f"A{linha_inicio}:I{linha_fim}"
    intervalo_lookup = f"O{linha_inicio}:Q{linha_fim}"

    executar_com_retry(
        lambda: aba_destino.batch_clear([intervalo_dados, intervalo_lookup]),
        descricao=f"limpar GERAL!{intervalo_dados} e GERAL!{intervalo_lookup}"
    )


def substituir_bloco_data_geral(
    planilha_destino,
    aba_destino,
    dados_novos,
    extras_o_p_q,
    data_referencia
):
    info_bloco = localizar_bloco_data_geral(
        aba_destino=aba_destino,
        data_referencia=data_referencia
    )

    qtd_nova = len(dados_novos)
    qtd_antiga = info_bloco["quantidade"]

    log(f"GERAL - quantidade antiga da data: {qtd_antiga}")
    log(f"GERAL - quantidade nova da data: {qtd_nova}")

    qtd_colunas_shift = 17

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
            qtd_colunas=qtd_colunas_shift
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
            qtd_colunas=qtd_colunas_shift
        )

        escrever_em_blocos(
            aba=aba_destino,
            dados=dados_novos,
            linha_inicial=linha_insercao,
            coluna_inicial="A"
        )

        escrever_em_blocos(
            aba=aba_destino,
            dados=extras_o_p_q,
            linha_inicial=linha_insercao,
            coluna_inicial="O"
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
            qtd_colunas=qtd_colunas_shift
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
            qtd_colunas=qtd_colunas_shift
        )

    if dados_novos:
        linha_fim_nova = linha_inicio + len(dados_novos) - 1

        limpar_intervalos_geral(aba_destino, linha_inicio, linha_fim_nova)

        log(f"Atualizando somente o bloco da data na GERAL a partir da linha {linha_inicio}.")

        escrever_em_blocos(
            aba=aba_destino,
            dados=dados_novos,
            linha_inicial=linha_inicio,
            coluna_inicial="A"
        )

        escrever_em_blocos(
            aba=aba_destino,
            dados=extras_o_p_q,
            linha_inicial=linha_inicio,
            coluna_inicial="O"
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

    mapa_reprogramadas = construir_mapa_lookup(dados)

    dados_com_chave = adicionar_chave_l(dados)

    log("Limpando A4:L da aba REPROGRAMADAS...")

    executar_com_retry(
        lambda: aba_destino.batch_clear([DESTINO_RANGE_COMPLETO_COM_CHAVE]),
        descricao="limpar REPROGRAMADAS!A4:L"
    )

    log("Aplicando formatação na aba REPROGRAMADAS...")

    aplicar_formatacao_destino(planilha_destino, aba_destino)

    if dados_com_chave:
        ultima_linha_necessaria = 3 + len(dados_com_chave)
        garantir_linhas_suficientes(aba_destino, ultima_linha_necessaria)

        log("Gravando dados A:L na aba REPROGRAMADAS...")

        escrever_em_blocos(
            aba=aba_destino,
            dados=dados_com_chave,
            linha_inicial=4,
            coluna_inicial="A"
        )
    else:
        log("Nenhum dado para gravar na aba REPROGRAMADAS.")

    log("Bloco 2 finalizado com sucesso.")

    return mapa_reprogramadas


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

    mapa_plan_principal = construir_mapa_lookup(dados)

    dados_com_chave = adicionar_chave_l(dados)

    log("Limpando A4:L da aba PLAN_PRINCIPAL...")

    executar_com_retry(
        lambda: aba_destino.batch_clear([DESTINO_RANGE_COMPLETO_COM_CHAVE]),
        descricao="limpar PLAN_PRINCIPAL!A4:L"
    )

    log("Aplicando formatação na aba PLAN_PRINCIPAL...")

    aplicar_formatacao_destino(planilha_destino, aba_destino)

    if dados_com_chave:
        ultima_linha_necessaria = 3 + len(dados_com_chave)
        garantir_linhas_suficientes(aba_destino, ultima_linha_necessaria)

        log("Gravando dados A:L na aba PLAN_PRINCIPAL...")

        escrever_em_blocos(
            aba=aba_destino,
            dados=dados_com_chave,
            linha_inicial=4,
            coluna_inicial="A"
        )
    else:
        log("Nenhum dado para gravar na aba PLAN_PRINCIPAL.")

    log("Bloco 3 finalizado com sucesso.")

    return mapa_plan_principal


# ==========================
# BLOCO 1
# ==========================

def executar_bloco_1(
    gc,
    planilha_destino,
    aba_config,
    ids_origem,
    cache_planilhas,
    cache_dados,
    mapa_plan_principal,
    mapa_reprogramadas
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

    extras_o_p_q = calcular_extras_geral(
        dados_geral=dados_data_referencia,
        mapa_plan_principal=mapa_plan_principal,
        mapa_reprogramadas=mapa_reprogramadas
    )

    log("Aplicando formatação na aba GERAL...")

    aplicar_formatacao_destino(planilha_destino, aba_destino)

    substituir_bloco_data_geral(
        planilha_destino=planilha_destino,
        aba_destino=aba_destino,
        dados_novos=dados_data_referencia,
        extras_o_p_q=extras_o_p_q,
        data_referencia=data_referencia
    )

    atualizar_lookup_geral_todas_linhas(
        aba_destino=aba_destino,
        mapa_plan_principal=mapa_plan_principal,
        mapa_reprogramadas=mapa_reprogramadas
    )

    log("Bloco 1 finalizado com sucesso.")


# ==========================
# PROCESSO PRINCIPAL
# ==========================

def main():
    log("Iniciando compilador...")

    gc, drive_service, sheets_service = autenticar_google()

    cache_planilhas = {}
    cache_dados = {}

    executar_bloco_0(
        drive_service=drive_service,
        sheets_service=sheets_service
    )

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

    mapa_reprogramadas = executar_bloco_2(
        gc=gc,
        planilha_destino=planilha_destino,
        ids_origem=ids_origem,
        cache_planilhas=cache_planilhas,
        cache_dados=cache_dados
    )

    mapa_plan_principal = executar_bloco_3(
        gc=gc,
        drive_service=drive_service,
        planilha_destino=planilha_destino,
        ids_origem=ids_origem,
        cache_planilhas=cache_planilhas,
        cache_dados=cache_dados
    )

    executar_bloco_1(
        gc=gc,
        planilha_destino=planilha_destino,
        aba_config=aba_config,
        ids_origem=ids_origem,
        cache_planilhas=cache_planilhas,
        cache_dados=cache_dados,
        mapa_plan_principal=mapa_plan_principal,
        mapa_reprogramadas=mapa_reprogramadas
    )

    atualizar_timestamp_final(aba_config)

    log("")
    log("Processo completo finalizado com sucesso.")


if __name__ == "__main__":
    main()
