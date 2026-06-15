"""Módulo google_io — gerado a partir do monólito compilador.py."""

from gspread.exceptions import APIError, WorksheetNotFound
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from zoneinfo import ZoneInfo
import base64
from googleapiclient.discovery import build
from datetime import datetime
import gspread
import json
import os
import random
import time

from config import (
    BLOCO0_SHEET_NAME,
    BLOCO0_SPREADSHEET_ID,
    CELULA_TIMESTAMP_FINAL,
    COLUNAS_MOEDA_DESTINO,
    CONFIG_ABA,
    ESPERA_INICIAL_429,
    ESPERA_MAXIMA_429,
    MAX_TENTATIVAS_API,
    ORIGEM_RANGE,
    PASTA_CSV_BLOCO_3_ID,
    PAUSA_APOS_ESCRITA,
    PAUSA_APOS_LEITURA,
    QTD_COLUNAS_DESTINO_GERAL,
    QTD_COLUNAS_ORIGEM_RANGE,
    RANGE_IDS_ORIGEM,
    TAMANHO_BLOCO_ESCRITA,
    TIMEZONE,
)
from util import log
from parsers import (
    bloco0_montar_colunas_h_i_j_k,
    eh_data_referencia,
    ler_linhas_csv,
    linha_tem_dados,
    normalizar_linha,
    selecionar_colunas_origem_base,
    selecionar_colunas_origem_com_extra,
)


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

    # Apenas frases específicas. Números soltos ("500" etc.) foram removidos:
    # casavam com texto não relacionado (ex.: "row 5002 invalid") e disparavam
    # retries inúteis. O status code acima já cobre os erros HTTP reais.
    termos = [
        "quota exceeded",
        "read requests per minute",
        "write requests per minute",
        "rate limit",
        "backend error",
        "internal error",
        "service unavailable",
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


def executar_etapa(nome, funcao):
    """
    Executa uma etapa de alto nível do processo registrando início e falha.

    Em caso de erro, loga o nome da etapa e o tipo do erro (para o log do
    GitHub Actions deixar claro ONDE falhou) e re-lança. As etapas são
    interdependentes (o Bloco 1 depende dos mapas dos Blocos 0/2/3), então
    não há como "continuar"; a propagação encerra o run com código != 0.
    """
    log("")
    log(f">>> Etapa: {nome}")

    try:
        return funcao()
    except Exception as erro:
        log(f"!!! FALHA na etapa '{nome}': {type(erro).__name__}: {erro}")
        raise


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


def bloco0_clear_range(sheets_service, spreadsheet_id, range_):
    executar_com_retry(
        lambda: sheets_service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=range_
        ).execute(),
        descricao=f"limpar range {range_}"
    )


def bloco0_upload_to_sheets(sheets_service, df):
    df_sheets = df.iloc[:, :7].copy()
    df_sheets = df_sheets.fillna("")

    df_sheets = bloco0_montar_colunas_h_i_j_k(df_sheets)

    values = df_sheets.values.tolist()

    bloco0_clear_range(
        sheets_service,
        BLOCO0_SPREADSHEET_ID,
        f"{BLOCO0_SHEET_NAME}!A3:K"
    )

    if not values:
        log("[BLOCO 0] Nenhum dado para gravar em BD_ConsultaServ.")
    else:
        total = len(values)
        tamanho_bloco = TAMANHO_BLOCO_ESCRITA

        for i in range(0, total, tamanho_bloco):
            bloco = values[i:i + tamanho_bloco]
            linha_inicio = 3 + i
            range_inicio = f"{BLOCO0_SHEET_NAME}!A{linha_inicio}"

            log(
                f"[BLOCO 0] Gravando BD_ConsultaServ em blocos: "
                f"{i + 1} até {min(i + tamanho_bloco, total)} de {total}"
            )

            executar_com_retry(
                lambda bloco=bloco, range_inicio=range_inicio: sheets_service.spreadsheets().values().update(
                    spreadsheetId=BLOCO0_SPREADSHEET_ID,
                    range=range_inicio,
                    valueInputOption="USER_ENTERED",
                    body={"values": bloco}
                ).execute(),
                descricao=f"gravar {range_inicio}"
            )

            time.sleep(PAUSA_APOS_ESCRITA)

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
