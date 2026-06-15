"""Módulo config — gerado a partir do monólito compilador.py."""


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


COLUNA_ORIGEM_EXTRA_1 = 45


COLUNA_ORIGEM_EXTRA_2 = 47


COLUNA_DATA_DESTINO = 0


COLUNAS_MOEDA_DESTINO = [
    4,  # E
    5,  # F
]


# G é porcentagem (ex.: "117%"), não moeda. Fica fora de COLUNAS_MOEDA_DESTINO
# para não ter o "%" removido por converter_moeda_para_numero; o valor cru
# "117%" é gravado com USER_ENTERED e o Sheets interpreta como percentual.
COLUNAS_PERCENTUAL_DESTINO = [
    6,  # G
]


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
