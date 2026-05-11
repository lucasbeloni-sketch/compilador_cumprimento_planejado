import os
import json
import base64
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

# A data será lida desta célula da aba destino
CELULA_DATA_REFERENCIA = "B2"

# B:BX possui 75 colunas.
# Ao colar a partir de A, o intervalo final será A:BW.
QTD_COLUNAS = 75
DESTINO_RANGE_LIMPAR = "A4:BW"


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
    """
    Converte valores possíveis do Google Sheets para date:
    - 08/05/2026
    - 2026-05-08
    - 08/05/2026 00:00:00
    - número serial de data do Google Sheets
    """

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

    # Como o intervalo começa em B, a coluna B da origem vira índice 0.
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

    # Dados da data de referência entram primeiro a partir de A4.
    dados_finais = dados_data_referencia + dados_destino_sem_data_referencia

    print("Limpando intervalo do destino...")

    aba_destino.batch_clear([DESTINO_RANGE_LIMPAR])

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
