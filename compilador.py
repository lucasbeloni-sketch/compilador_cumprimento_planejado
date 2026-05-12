import os
import json
import base64
import re
from datetime import datetime, date, timedelta

import gspread
from google.oauth2.service_account import Credentials


# ==========================
# CONFIGURAÇÕES
# ==========================

DESTINO_ID = "1x7-AjwlFgVmrjcHqFVypBdcN4_DoRaGYPy2ByxJvs1w"

DESTINO_ABA = "GERAL"
CONFIG_ABA = "Config"

CELULA_DATA_REFERENCIA = "B2"
RANGE_IDS_ORIGEM = "B4:B"

ORIGEM_ABA = "Plan_Principal"

# Lê somente até BE, pois a última coluna necessária é BE.
ORIGEM_RANGE = "B6:BE"

# Serão coladas apenas 9 colunas no destino: A:I
QTD_COLUNAS_DESTINO = 9
DESTINO_RANGE_LIMPAR = "A4:I"

# Índices relativos ao intervalo B:BE
# B=0, C=1, D=2...
COLUNAS_ORIGEM_SELECIONADAS = [
    0,   # B
    5,   # G
    6,   # H
    11,  # M
    36,  # AL
    37,  # AM
    38,  # AN
    46,  # AV
    55,  # BE
]

# No destino compacto:
# A = Data
# E, F, G = colunas vindas de AL, AM, AN
COLUNA_DATA_DESTINO = 0

COLUNAS_MOEDA_DESTINO = [
    4,  # E
    5,  # F
    6,  # G
]


# ==========================
# AUTENTICAÇÃO
# ==========================

def autenticar_google_sheets():
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
    return gspread.authorize(creds)


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


def selecionar_colunas_origem(linha):
    """
    Seleciona apenas:
    B, G, H, M, AL, AM, AN, AV e BE
    do intervalo origem B:BE.
    """
    linha = normalizar_linha(linha, 56)

    return [
        linha[indice] if indice < len(linha) else ""
        for indice in COLUNAS_ORIGEM_SELECIONADAS
    ]


def preparar_linha_para_envio(linha):
    linha = normalizar_linha(linha, QTD_COLUNAS_DESTINO)

    # Coluna A: Data
    data_valor = converter_para_data(linha[COLUNA_DATA_DESTINO])

    if data_valor:
        linha[COLUNA_DATA_DESTINO] = data_para_serial_google_sheets(data_valor)

    # Colunas E, F e G: Moeda
    for indice in COLUNAS_MOEDA_DESTINO:
        linha[indice] = converter_moeda_para_numero(linha[indice])

    return linha


def garantir_linhas_suficientes(aba, ultima_linha_necessaria):
    linhas_atuais = aba.row_count

    if ultima_linha_necessaria > linhas_atuais:
        aba.add_rows(ultima_linha_necessaria - linhas_atuais)


def escrever_em_blocos(aba, dados, linha_inicial=4, coluna_inicial="A", tamanho_bloco=1000):
    if not dados:
        return

    for i in range(0, len(dados), tamanho_bloco):
        bloco = dados[i:i + tamanho_bloco]
        linha_destino = linha_inicial + i
        celula_inicio = f"{coluna_inicial}{linha_destino}"

        aba.update(
            values=bloco,
            range_name=celula_inicio,
            value_input_option="USER_ENTERED"
        )


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

    # Coluna A: Data
    adicionar_formatacao_coluna(
        coluna_inicio=0,
        coluna_fim=1,
        tipo="DATE",
        padrao="dd/mm/yyyy"
    )

    # Colunas E, F e G: Moeda
    for coluna in COLUNAS_MOEDA_DESTINO:
        adicionar_formatacao_coluna(
            coluna_inicio=coluna,
            coluna_fim=coluna + 1,
            tipo="CURRENCY",
            padrao='"R$" #,##0.00'
        )

    if requests:
        planilha_destino.batch_update({"requests": requests})


def ler_ids_planilhas_origem(aba_config):
    valores = aba_config.get(
        RANGE_IDS_ORIGEM,
        value_render_option="FORMATTED_VALUE"
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


def ler_dados_de_uma_origem(gc, origem_id, data_referencia):
    print(f"Lendo origem: {origem_id}")

    try:
        planilha_origem = gc.open_by_key(origem_id)
        aba_origem = planilha_origem.worksheet(ORIGEM_ABA)

        dados_origem = aba_origem.get(
            ORIGEM_RANGE,
            value_render_option="FORMATTED_VALUE"
        )

        dados_origem = [
            normalizar_linha(linha, 56)
            for linha in dados_origem
            if linha_tem_dados(linha)
        ]

        # Filtra pela coluna B da origem.
        # Como o intervalo começa em B, a coluna B é o índice 0.
        dados_filtrados = [
            linha
            for linha in dados_origem
            if eh_data_referencia(linha[0], data_referencia)
        ]

        dados_selecionados = [
            selecionar_colunas_origem(linha)
            for linha in dados_filtrados
        ]

        print(f"Linhas encontradas nessa origem: {len(dados_selecionados)}")

        return dados_selecionados

    except Exception as erro:
        print(f"Erro ao processar a origem {origem_id}: {erro}")
        print("Essa origem será ignorada e o processo seguirá para a próxima.")
        return []


# ==========================
# PROCESSO PRINCIPAL
# ==========================

def main():
    gc = autenticar_google_sheets()

    planilha_destino = gc.open_by_key(DESTINO_ID)
    aba_destino = planilha_destino.worksheet(DESTINO_ABA)
    aba_config = planilha_destino.worksheet(CONFIG_ABA)

    valor_data_referencia = aba_config.acell(
        CELULA_DATA_REFERENCIA,
        value_render_option="FORMATTED_VALUE"
    ).value

    data_referencia = converter_para_data(valor_data_referencia)

    if not data_referencia:
        raise Exception(
            f"Não foi possível identificar uma data válida na célula {CELULA_DATA_REFERENCIA} "
            f"da aba {CONFIG_ABA}. Valor encontrado: {valor_data_referencia}"
        )

    print(f"Data de referência considerada: {data_referencia.strftime('%d/%m/%Y')}")

    ids_origem = ler_ids_planilhas_origem(aba_config)

    if not ids_origem:
        raise Exception(
            f"Nenhum ID de planilha de origem encontrado no intervalo {CONFIG_ABA}!{RANGE_IDS_ORIGEM}."
        )

    print(f"Quantidade de planilhas de origem encontradas: {len(ids_origem)}")

    dados_data_referencia = []

    for origem_id in ids_origem:
        dados_origem = ler_dados_de_uma_origem(
            gc=gc,
            origem_id=origem_id,
            data_referencia=data_referencia
        )

        dados_data_referencia.extend(dados_origem)

    print(f"Total de linhas consolidadas das origens: {len(dados_data_referencia)}")

    print("Lendo dados atuais do destino...")

    dados_destino = aba_destino.get(
        DESTINO_RANGE_LIMPAR,
        value_render_option="FORMATTED_VALUE"
    )

    dados_destino = [
        normalizar_linha(linha, QTD_COLUNAS_DESTINO)
        for linha in dados_destino
        if linha_tem_dados(linha)
    ]

    # Remove do destino as linhas onde a coluna A for igual à data de referência.
    dados_destino_sem_data_referencia = [
        linha
        for linha in dados_destino
        if not eh_data_referencia(linha[0], data_referencia)
    ]

    print(f"Linhas antigas mantidas no destino: {len(dados_destino_sem_data_referencia)}")
    print(
        f"Linhas removidas do destino por serem da data de referência: "
        f"{len(dados_destino) - len(dados_destino_sem_data_referencia)}"
    )

    dados_finais = dados_data_referencia + dados_destino_sem_data_referencia

    dados_finais = [
        preparar_linha_para_envio(linha)
        for linha in dados_finais
    ]

    print("Limpando intervalo do destino...")

    aba_destino.batch_clear([DESTINO_RANGE_LIMPAR])

    print("Aplicando formatação no destino...")

    aplicar_formatacao_destino(planilha_destino, aba_destino)

    if dados_finais:
        ultima_linha_necessaria = 3 + len(dados_finais)
        garantir_linhas_suficientes(aba_destino, ultima_linha_necessaria)

        print("Gravando dados atualizados no destino...")

        escrever_em_blocos(
            aba=aba_destino,
            dados=dados_finais,
            linha_inicial=4,
            coluna_inicial="A",
            tamanho_bloco=1000
        )
    else:
        print("Nenhum dado para gravar no destino.")

    print("Processo finalizado com sucesso.")


if __name__ == "__main__":
    main()
