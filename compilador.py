import os
import json
import base64
import re
import csv
import io
import time
import random
from datetime import datetime, date, timedelta

import gspread
from gspread.exceptions import APIError, WorksheetNotFound
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# ==========================
# CONFIGURAÇÕES GERAIS
# ==========================

DESTINO_ID = "1x7-AjwlFgVmrjcHqFVypBdcN4_DoRaGYPy2ByxJvs1w"

CONFIG_ABA = "Config"
CELULA_DATA_REFERENCIA = "B2"
RANGE_IDS_ORIGEM = "B4:B"

PASTA_CSV_BLOCO_3_ID = "1f5Z0f73IZD4rBEssNb9OVtADLVZzttaF"

ORIGEM_RANGE = "B6:BE"

QTD_COLUNAS_ORIGEM_RANGE = 56

QTD_COLUNAS_DESTINO_BLOCO_1 = 9
QTD_COLUNAS_DESTINO_BASE = 9
QTD_COLUNAS_DESTINO_EXTRA = 2

DESTINO_RANGE_LIMPAR_BLOCO_1 = "A4:I"

# Blocos 2 e 3:
# NÃO limpar J:N, pois possuem fórmulas.
DESTINO_RANGE_LIMPAR_BASE = "A4:I"
DESTINO_RANGE_LIMPAR_EXTRA = "O4:P"

# Escreve blocos maiores para reduzir requisições na API.
TAMANHO_BLOCO_ESCRITA = 10000

# Pequenas pausas para reduzir risco de 429.
PAUSA_APOS_LEITURA = 1.5
PAUSA_APOS_ESCRITA = 2.5

# Retry para erro de quota / instabilidade.
MAX_TENTATIVAS_API = 7
ESPERA_INICIAL_429 = 20
ESPERA_MAXIMA_429 = 90

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

# No destino:
# A = Data
# E, F, G = Moeda
COLUNA_DATA_DESTINO = 0

COLUNAS_MOEDA_DESTINO = [
    4,  # E
    5,  # F
    6,  # G
]


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

            espera = min(
                ESPERA_MAXIMA_429,
                ESPERA_INICIAL_429 * (2 ** (tentativa - 1))
            )

            espera += random.uniform(1, 5)

            print(
                f"Aviso: erro temporário/API quota em '{descricao}'. "
                f"Tentativa {tentativa}/{MAX_TENTATIVAS_API}. "
                f"Aguardando {espera:.1f}s antes de tentar novamente."
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


def remover_linhas_vazias_pareadas(dados_base_a_i, dados_extra_o_p, nome_bloco=""):
    """
    Remove linhas que ficariam vazias no destino.

    A validação principal é feita em A:I.
    Se A:I ficar totalmente vazio depois da seleção das colunas,
    a linha é removida junto com o par correspondente de O:P.
    """

    base_filtrada = []
    extra_filtrada = []
    removidas = 0

    for i, base in enumerate(dados_base_a_i):
        extra = dados_extra_o_p[i] if i < len(dados_extra_o_p) else ["", ""]

        if linha_tem_dados(base):
            base_filtrada.append(base)
            extra_filtrada.append(extra)
        else:
            removidas += 1

    if nome_bloco:
        print(f"Linhas vazias removidas no {nome_bloco}: {removidas}")
    else:
        print(f"Linhas vazias removidas: {removidas}")

    return base_filtrada, extra_filtrada


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

    extra_o_p = [
        linha[COLUNA_ORIGEM_EXTRA_1] if COLUNA_ORIGEM_EXTRA_1 < len(linha) else "",
        linha[COLUNA_ORIGEM_EXTRA_2] if COLUNA_ORIGEM_EXTRA_2 < len(linha) else "",
    ]

    return base_a_i, extra_o_p


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

        print(
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
        print(f"Usando cache Google Sheets: {origem_id} | Aba: {aba_origem_nome}")
        return cache_dados[chave_cache]

    print(f"Lendo origem Google Sheets: {origem_id} | Aba: {aba_origem_nome}")

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
        print(f"Aba não encontrada na origem {origem_id}: {aba_origem_nome}")
        cache_dados[chave_cache] = []
        return []

    except Exception as erro:
        print(f"Erro ao processar a origem {origem_id}, aba {aba_origem_nome}: {erro}")
        print("Essa origem será ignorada e o processo seguirá para a próxima.")
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


# ==========================
# LEITURA GOOGLE SHEETS
# ==========================

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

    print(f"Linhas encontradas nessa origem: {len(dados_selecionados)}")

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

    dados_base_a_i = []
    dados_extra_o_p = []

    for linha in dados_origem:
        base_a_i, extra_o_p = selecionar_colunas_origem_com_extra(linha)

        dados_base_a_i.append(base_a_i)
        dados_extra_o_p.append(extra_o_p)

    print(f"Linhas encontradas nessa origem: {len(dados_base_a_i)}")

    return dados_base_a_i, dados_extra_o_p


# ==========================
# LEITURA CSV DRIVE - BLOCO 3
# ==========================

def listar_arquivos_csv_drive(drive_service, pasta_id):
    print(f"Buscando arquivos CSV na pasta Drive: {pasta_id}")

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

    print(f"Quantidade de CSVs encontrados: {len(arquivos)}")

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

    # Equivalente ao intervalo A2:BD:
    # ignora a primeira linha e lê 56 colunas.
    linhas_dados = linhas[1:]

    linhas_dados = [
        normalizar_linha(linha, QTD_COLUNAS_ORIGEM_RANGE)
        for linha in linhas_dados
        if linha_tem_dados(linha)
    ]

    return linhas_dados


def ler_dados_csvs_bloco_3(drive_service):
    print("")
    print("Lendo CSVs do Drive para o Bloco 3...")

    arquivos_csv = listar_arquivos_csv_drive(
        drive_service=drive_service,
        pasta_id=PASTA_CSV_BLOCO_3_ID
    )

    dados_base_a_i = []
    dados_extra_o_p = []

    for arquivo in arquivos_csv:
        arquivo_id = arquivo.get("id")
        arquivo_nome = arquivo.get("name")

        print(f"Lendo CSV: {arquivo_nome}")

        try:
            texto_csv = baixar_csv_drive(
                drive_service=drive_service,
                arquivo_id=arquivo_id
            )

            linhas_csv = ler_linhas_csv(texto_csv)

            linhas_aproveitadas = 0
            linhas_vazias_selecao = 0

            for linha in linhas_csv:
                base_a_i, extra_o_p = selecionar_colunas_origem_com_extra(linha)

                # Evita enviar linha que ficará vazia no destino.
                if not linha_tem_dados(base_a_i):
                    linhas_vazias_selecao += 1
                    continue

                dados_base_a_i.append(base_a_i)
                dados_extra_o_p.append(extra_o_p)
                linhas_aproveitadas += 1

            print(f"Linhas lidas do CSV {arquivo_nome}: {len(linhas_csv)}")
            print(f"Linhas aproveitadas do CSV {arquivo_nome}: {linhas_aproveitadas}")
            print(f"Linhas vazias ignoradas do CSV {arquivo_nome}: {linhas_vazias_selecao}")

        except Exception as erro:
            print(f"Erro ao processar CSV {arquivo_nome}: {erro}")
            print("Esse CSV será ignorado e o processo seguirá para o próximo.")

    print(f"Total de linhas vindas dos CSVs no Bloco 3: {len(dados_base_a_i)}")

    return dados_base_a_i, dados_extra_o_p


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
    print("")
    print("======================================")
    print("INICIANDO BLOCO 1 - PLAN_PRINCIPAL > GERAL")
    print("======================================")

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

    print(f"Data de referência considerada no Bloco 1: {data_referencia.strftime('%d/%m/%Y')}")

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

    print(f"Total de linhas consolidadas no Bloco 1: {len(dados_data_referencia)}")

    print("Lendo dados atuais da aba GERAL...")

    dados_destino = executar_com_retry(
        lambda: aba_destino.get(
            DESTINO_RANGE_LIMPAR_BLOCO_1,
            value_render_option="FORMATTED_VALUE"
        ),
        descricao=f"ler destino GERAL!{DESTINO_RANGE_LIMPAR_BLOCO_1}"
    )

    time.sleep(PAUSA_APOS_LEITURA)

    dados_destino = [
        normalizar_linha(linha, QTD_COLUNAS_DESTINO_BLOCO_1)
        for linha in dados_destino
        if linha_tem_dados(linha)
    ]

    dados_destino_sem_data_referencia = [
        linha
        for linha in dados_destino
        if not eh_data_referencia(linha[0], data_referencia)
    ]

    print(f"Linhas antigas mantidas na GERAL: {len(dados_destino_sem_data_referencia)}")
    print(
        f"Linhas removidas da GERAL por serem da data de referência: "
        f"{len(dados_destino) - len(dados_destino_sem_data_referencia)}"
    )

    dados_finais = dados_data_referencia + dados_destino_sem_data_referencia

    dados_finais = [
        preparar_linha_para_envio(linha, QTD_COLUNAS_DESTINO_BLOCO_1)
        for linha in dados_finais
    ]

    print("Limpando intervalo da aba GERAL...")

    executar_com_retry(
        lambda: aba_destino.batch_clear([DESTINO_RANGE_LIMPAR_BLOCO_1]),
        descricao=f"limpar GERAL!{DESTINO_RANGE_LIMPAR_BLOCO_1}"
    )

    print("Aplicando formatação na aba GERAL...")

    aplicar_formatacao_destino(planilha_destino, aba_destino)

    if dados_finais:
        ultima_linha_necessaria = 3 + len(dados_finais)
        garantir_linhas_suficientes(aba_destino, ultima_linha_necessaria)

        print("Gravando dados atualizados na aba GERAL...")

        escrever_em_blocos(
            aba=aba_destino,
            dados=dados_finais,
            linha_inicial=4,
            coluna_inicial="A"
        )
    else:
        print("Nenhum dado para gravar na aba GERAL.")

    print("Bloco 1 finalizado com sucesso.")


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
    print("")
    print("======================================")
    print("INICIANDO BLOCO 2 - REPROGRAMADAS > REPROGRAMADAS")
    print("======================================")

    aba_destino = executar_com_retry(
        lambda: planilha_destino.worksheet("REPROGRAMADAS"),
        descricao="abrir aba REPROGRAMADAS"
    )

    dados_base_a_i = []
    dados_extra_o_p = []

    for origem_id in ids_origem:
        base_origem, extra_origem = ler_dados_origem_sem_filtro_com_extra(
            gc=gc,
            origem_id=origem_id,
            aba_origem_nome="Reprogramadas",
            cache_planilhas=cache_planilhas,
            cache_dados=cache_dados
        )

        dados_base_a_i.extend(base_origem)
        dados_extra_o_p.extend(extra_origem)

    print(f"Total bruto de linhas consolidadas no Bloco 2: {len(dados_base_a_i)}")

    dados_base_a_i, dados_extra_o_p = remover_linhas_vazias_pareadas(
        dados_base_a_i=dados_base_a_i,
        dados_extra_o_p=dados_extra_o_p,
        nome_bloco="Bloco 2"
    )

    print(f"Total final de linhas úteis no Bloco 2: {len(dados_base_a_i)}")

    dados_base_a_i = [
        preparar_linha_para_envio(linha, QTD_COLUNAS_DESTINO_BASE)
        for linha in dados_base_a_i
    ]

    dados_extra_o_p = [
        normalizar_linha(linha, QTD_COLUNAS_DESTINO_EXTRA)
        for linha in dados_extra_o_p
    ]

    print("Limpando somente A:I e O:P da aba REPROGRAMADAS...")
    print("As colunas J:N serão preservadas.")

    executar_com_retry(
        lambda: aba_destino.batch_clear([
            DESTINO_RANGE_LIMPAR_BASE,
            DESTINO_RANGE_LIMPAR_EXTRA
        ]),
        descricao="limpar REPROGRAMADAS A:I e O:P"
    )

    print("Aplicando formatação na aba REPROGRAMADAS...")

    aplicar_formatacao_destino(planilha_destino, aba_destino)

    if dados_base_a_i:
        ultima_linha_necessaria = 3 + len(dados_base_a_i)
        garantir_linhas_suficientes(aba_destino, ultima_linha_necessaria)

        print("Gravando dados A:I na aba REPROGRAMADAS...")

        escrever_em_blocos(
            aba=aba_destino,
            dados=dados_base_a_i,
            linha_inicial=4,
            coluna_inicial="A"
        )

        print("Gravando dados O:P na aba REPROGRAMADAS...")

        escrever_em_blocos(
            aba=aba_destino,
            dados=dados_extra_o_p,
            linha_inicial=4,
            coluna_inicial="O"
        )
    else:
        print("Nenhum dado para gravar na aba REPROGRAMADAS.")

    print("Bloco 2 finalizado com sucesso.")


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
    print("")
    print("======================================")
    print("INICIANDO BLOCO 3 - CSVs + PLAN_PRINCIPAL > PLAN_PRINCIPAL")
    print("======================================")

    aba_destino = executar_com_retry(
        lambda: planilha_destino.worksheet("PLAN_PRINCIPAL"),
        descricao="abrir aba PLAN_PRINCIPAL"
    )

    dados_base_a_i = []
    dados_extra_o_p = []

    # 1º - Lê primeiro os CSVs do Drive
    base_csv, extra_csv = ler_dados_csvs_bloco_3(
        drive_service=drive_service
    )

    dados_base_a_i.extend(base_csv)
    dados_extra_o_p.extend(extra_csv)

    # 2º - Depois lê as planilhas Google Sheets
    print("")
    print("Lendo planilhas Google Sheets para o Bloco 3...")

    for origem_id in ids_origem:
        base_origem, extra_origem = ler_dados_origem_sem_filtro_com_extra(
            gc=gc,
            origem_id=origem_id,
            aba_origem_nome="Plan_Principal",
            cache_planilhas=cache_planilhas,
            cache_dados=cache_dados
        )

        dados_base_a_i.extend(base_origem)
        dados_extra_o_p.extend(extra_origem)

    print(f"Total bruto de linhas consolidadas no Bloco 3: {len(dados_base_a_i)}")

    dados_base_a_i, dados_extra_o_p = remover_linhas_vazias_pareadas(
        dados_base_a_i=dados_base_a_i,
        dados_extra_o_p=dados_extra_o_p,
        nome_bloco="Bloco 3"
    )

    print(f"Total final de linhas úteis no Bloco 3: {len(dados_base_a_i)}")

    dados_base_a_i = [
        preparar_linha_para_envio(linha, QTD_COLUNAS_DESTINO_BASE)
        for linha in dados_base_a_i
    ]

    dados_extra_o_p = [
        normalizar_linha(linha, QTD_COLUNAS_DESTINO_EXTRA)
        for linha in dados_extra_o_p
    ]

    print("Limpando somente A:I e O:P da aba PLAN_PRINCIPAL...")
    print("As colunas J:N serão preservadas.")

    executar_com_retry(
        lambda: aba_destino.batch_clear([
            DESTINO_RANGE_LIMPAR_BASE,
            DESTINO_RANGE_LIMPAR_EXTRA
        ]),
        descricao="limpar PLAN_PRINCIPAL A:I e O:P"
    )

    print("Aplicando formatação na aba PLAN_PRINCIPAL...")

    aplicar_formatacao_destino(planilha_destino, aba_destino)

    if dados_base_a_i:
        ultima_linha_necessaria = 3 + len(dados_base_a_i)
        garantir_linhas_suficientes(aba_destino, ultima_linha_necessaria)

        print("Gravando dados A:I na aba PLAN_PRINCIPAL...")

        escrever_em_blocos(
            aba=aba_destino,
            dados=dados_base_a_i,
            linha_inicial=4,
            coluna_inicial="A"
        )

        print("Gravando dados O:P na aba PLAN_PRINCIPAL...")

        escrever_em_blocos(
            aba=aba_destino,
            dados=dados_extra_o_p,
            linha_inicial=4,
            coluna_inicial="O"
        )
    else:
        print("Nenhum dado para gravar na aba PLAN_PRINCIPAL.")

    print("Bloco 3 finalizado com sucesso.")


# ==========================
# PROCESSO PRINCIPAL
# ==========================

def main():
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

    print(f"Quantidade de planilhas de origem encontradas: {len(ids_origem)}")

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

    print("")
    print("Processo completo finalizado com sucesso.")


if __name__ == "__main__":
    main()
