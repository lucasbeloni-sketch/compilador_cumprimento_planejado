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

ORIGEM_ID = "1OTHF2ytEOjGgfE49paARXkz9GjaklOQC_UhiXwUjC2E"
ORIGEM_ABA = "Plan_Principal"
ORIGEM_RANGE = "B6:BX"

DESTINO_ID = "1x7-AjwlFgVmrjcHqFVypBdcN4_DoRaGYPy2ByxJvs1w"
DESTINO_ABA = "BARREIRAS"

CELULA_DATA_REFERENCIA = "B2"

# B:BX possui 75 colunas.
# Ao colar a partir de A, o intervalo final será A:BW.
QTD_COLUNAS = 75
DESTINO_RANGE_LIMPAR = "A4:BW"

# Índices das colunas no destino, base zero.
# A = 0, B = 1, C = 2...
COLUNA_DATA = 0

COLUNAS_MOEDA = [
    36,  # AK
    37,  # AL
    39,  # AN
]

COLUNAS_DURACAO = [
    62,  # BK
    63,  # BL
    64,  # BM
    65,  # BN
    66,  # BO
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

def normalizar_linha(linha, qtd_colunas=QTD_COLUNAS):
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
    """
    Converte date para número serial usado pelo Google Sheets.
    """
    return (data_valor - date(1899, 12, 30)).days


def converter_moeda_para_numero(valor):
    """
    Converte valores como:
    R$ 1.234,56
    1.234,56
    1234,56
    1234.56
    para número decimal.
    """
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
        # Padrão brasileiro: 1.234,56
        if texto.rfind(",") > texto.rfind("."):
            texto = texto.replace(".", "").replace(",", ".")
        else:
            texto = texto.replace(",", "")
    elif "," in texto:
        texto = texto.replace(".", "").replace(",", ".")
    elif "." in texto:
        partes = texto.split(".")
        # Caso seja 1.234, interpreta como milhar
        if len(partes[-1]) == 3 and len(partes) > 1:
            texto = texto.replace(".", "")

    try:
        numero = float(texto)
        return -numero if negativo else numero
    except Exception:
        return valor


def converter_duracao_para_numero(valor):
    """
    Converte duração para o formato numérico do Google Sheets.

    Exemplos:
    08:00:00 -> 0,333333...
    08:30 -> 0,354166...
    30:00:00 -> 1,25
    8 -> 8 horas -> 0,333333...
    """
    if valor is None:
        return ""

    if isinstance(valor, (int, float)):
        if valor == 0:
            return 0

        # Se for maior que 1, interpreta como horas.
        # Exemplo: 8 = 8 horas.
        if valor > 1:
            return valor / 24

        # Se for menor ou igual a 1, já pode ser fração de dia.
        return valor

    texto = str(valor).strip()

    if texto in ["", "-", "—"]:
        return ""

    texto = texto.replace("\u00a0", " ").strip()

    if ":" in texto:
        partes = texto.split(":")

        try:
            if len(partes) == 3:
                horas = int(partes[0])
                minutos = int(partes[1])
                segundos = float(partes[2].replace(",", "."))
            elif len(partes) == 2:
                horas = int(partes[0])
                minutos = int(partes[1])
                segundos = 0
            else:
                return valor

            total_segundos = (horas * 3600) + (minutos * 60) + segundos
            return total_segundos / 86400

        except Exception:
            return valor

    texto_numero = texto.replace(",", ".")

    try:
        numero = float(texto_numero)

        if numero > 1:
            return numero / 24

        return numero

    except Exception:
        return valor


def preparar_linha_para_envio(linha):
    linha = normalizar_linha(linha)

    # Coluna A: Data
    data_valor = converter_para_data(linha[COLUNA_DATA])
    if data_valor:
        linha[COLUNA_DATA] = data_para_serial_google_sheets(data_valor)

    # Colunas de moeda
    for indice in COLUNAS_MOEDA:
        linha[indice] = converter_moeda_para_numero(linha[indice])

    # Colunas de duração
    for indice in COLUNAS_DURACAO:
        linha[indice] = converter_duracao_para_numero(linha[indice])

    return linha


def eh_data_referencia(valor, data_referencia):
    data_valor = converter_para_data(valor)
    return data_valor == data_referencia


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
    """
    Aplica formatação a partir da linha 4.
    A = Data
    AK, AL, AN = Moeda
    BK:BO = Duração
    """

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

    # AK, AL, AN: Moeda
    for coluna in COLUNAS_MOEDA:
        adicionar_formatacao_coluna(
            coluna_inicio=coluna,
            coluna_fim=coluna + 1,
            tipo="CURRENCY",
            padrao='"R$" #,##0.00'
        )

    # BK:BO: Duração
    for coluna in COLUNAS_DURACAO:
        adicionar_formatacao_coluna(
            coluna_inicio=coluna,
            coluna_fim=coluna + 1,
            tipo="NUMBER",
            padrao="[h]:mm:ss"
        )

    if requests:
        planilha_destino.batch_update({"requests": requests})


# ==========================
# PROCESSO PRINCIPAL
# ==========================

def main():
    gc = autenticar_google_sheets()

    planilha_origem = gc.open_by_key(ORIGEM_ID)
    aba_origem = planilha_origem.worksheet(ORIGEM_ABA)

    planilha_destino = gc.open_by_key(DESTINO_ID)
    aba_destino = planilha_destino.worksheet(DESTINO_ABA)

    valor_data_referencia = aba_destino.acell(
        CELULA_DATA_REFERENCIA,
        value_render_option="FORMATTED_VALUE"
    ).value

    data_referencia = converter_para_data(valor_data_referencia)

    if not data_referencia:
        raise Exception(
            f"Não foi possível identificar uma data válida na célula {CELULA_DATA_REFERENCIA} da aba {DESTINO_ABA}. "
            f"Valor encontrado: {valor_data_referencia}"
        )

    print(f"Data de referência considerada: {data_referencia.strftime('%d/%m/%Y')}")

    print("Lendo dados da origem...")

    dados_origem = aba_origem.get(
        ORIGEM_RANGE,
        value_render_option="FORMATTED_VALUE"
    )

    dados_origem = [
        normalizar_linha(linha)
        for linha in dados_origem
        if linha_tem_dados(linha)
    ]

    dados_data_referencia = [
        linha
        for linha in dados_origem
        if eh_data_referencia(linha[0], data_referencia)
    ]

    print(f"Linhas encontradas para a data de referência na origem: {len(dados_data_referencia)}")

    print("Lendo dados atuais do destino...")

    dados_destino = aba_destino.get(
        DESTINO_RANGE_LIMPAR,
        value_render_option="FORMATTED_VALUE"
    )

    dados_destino = [
        normalizar_linha(linha)
        for linha in dados_destino
        if linha_tem_dados(linha)
    ]

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
