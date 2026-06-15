"""Módulo parsers — gerado a partir do monólito compilador.py."""

import csv
from datetime import datetime, date, timedelta
import io
import pandas as pd
import re

from config import (
    BLOCO0_DATE_REGEX,
    COLUNAS_MOEDA_DESTINO,
    COLUNAS_ORIGEM_SELECIONADAS,
    COLUNA_DATA_DESTINO,
    COLUNA_ORIGEM_EXTRA_1,
    COLUNA_ORIGEM_EXTRA_2,
    QTD_COLUNAS_DESTINO_COMPLETO,
    QTD_COLUNAS_DESTINO_GERAL,
    QTD_COLUNAS_ORIGEM_RANGE,
)
from util import log


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


def texto_chave(valor):
    if valor is None:
        return ""

    try:
        if pd.isna(valor):
            return ""
    except Exception:
        pass

    if isinstance(valor, float):
        if valor.is_integer():
            return str(int(valor))
        return str(valor)

    if isinstance(valor, int):
        return str(valor)

    return str(valor).strip()


def _normalizar_separadores_ptbr(texto):
    """
    Normaliza separadores de n\u00famero pt-BR para uma string parse\u00e1vel por float().

    Regras:
    - v\u00edrgula e ponto: o separador mais \u00e0 direita \u00e9 o decimal;
    - s\u00f3 v\u00edrgula: v\u00edrgula \u00e9 decimal, ponto vira milhar;
    - s\u00f3 ponto: trata como milhar se o \u00faltimo grupo tiver 3 d\u00edgitos
      ("1.234" -> "1234"); caso contr\u00e1rio \u00e9 decimal ("1.50" preservado).

    N\u00e3o remove "R$", espa\u00e7os ou sinais \u2014 isso \u00e9 responsabilidade de quem chama.
    Core \u00fanico compartilhado por numero_calculo, converter_moeda_para_numero
    e bloco0_to_number_ptbr.
    """
    if "," in texto and "." in texto:
        if texto.rfind(",") > texto.rfind("."):
            texto = texto.replace(".", "").replace(",", ".")
        else:
            texto = texto.replace(",", "")
    elif "," in texto:
        texto = texto.replace(".", "").replace(",", ".")
    elif "." in texto:
        partes = texto.split(".")
        if len(partes) > 1 and len(partes[-1]) == 3:
            texto = texto.replace(".", "")

    return texto


def numero_calculo(valor):
    if valor is None:
        return 0.0

    try:
        if pd.isna(valor):
            return 0.0
    except Exception:
        pass

    if isinstance(valor, (int, float)):
        return float(valor)

    texto = str(valor).strip()

    if texto == "":
        return 0.0

    texto = texto.replace("R$", "")
    texto = texto.replace(" ", "")
    texto = texto.replace("\u00a0", "")

    texto = _normalizar_separadores_ptbr(texto)

    try:
        return float(texto)
    except Exception:
        return 0.0


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
    """
    Calcula O:P e Q da aba GERAL como valores.

    Q = A & B & C & E
    O:P = busca Q primeiro no PLAN_PRINCIPAL, depois em REPROGRAMADAS.
    Se não encontrar, retorna mensagem em O e P.
    """

    extras = []

    mensagem_nao_encontrado = "Não encontrado nas abas Plan_Principal e Reprogramadas"

    for linha in dados_geral:
        chave = calcular_chave_linha(linha)

        if not chave:
            extras.append(["", "", ""])
            continue

        valores = mapa_plan_principal.get(chave)

        if valores is None:
            valores = mapa_reprogramadas.get(chave)

        if valores is None:
            valores = [
                mensagem_nao_encontrado,
                mensagem_nao_encontrado
            ]

        extras.append([
            valores[0] if len(valores) > 0 else mensagem_nao_encontrado,
            valores[1] if len(valores) > 1 else mensagem_nao_encontrado,
            chave
        ])

    return extras


def data_para_chave_serial(valor):
    if valor is None:
        return ""

    try:
        if pd.isna(valor):
            return ""
    except Exception:
        pass

    if isinstance(valor, (int, float)):
        if float(valor).is_integer():
            return str(int(valor))
        return str(valor)

    data_valor = converter_para_data(valor)

    if data_valor:
        return str(data_para_serial_google_sheets(data_valor))

    texto = str(valor).strip()

    try:
        numero = float(texto.replace(",", "."))

        if numero.is_integer():
            return str(int(numero))

        return str(numero)
    except Exception:
        return texto


def bloco0_construir_mapas_bd_consulta_serv(df_sheets):
    """
    Monta os mapas usados para calcular GERAL!J:N.
    """

    mapas = {
        "soma_h_data_equipe": {},
        "soma_j_data_equipe": {},
        "soma_data_equipe": {},
        "busca_i": {},
        "busca_k": {},
    }

    for _, row in df_sheets.iterrows():
        equipe = texto_chave(row.iloc[3]) if len(row) > 3 else ""
        obs_servico = texto_chave(row.iloc[4]) if len(row) > 4 else ""
        data_exec = row.iloc[5] if len(row) > 5 else ""
        total_servicos = numero_calculo(row.iloc[6]) if len(row) > 6 else 0.0

        h = texto_chave(row.iloc[7]) if len(row) > 7 else ""
        j = texto_chave(row.iloc[9]) if len(row) > 9 else ""

        data_serial = data_para_chave_serial(data_exec)

        if not data_serial or not equipe:
            continue

        chave_data_equipe = (data_serial, equipe)

        mapas["soma_data_equipe"][chave_data_equipe] = (
            mapas["soma_data_equipe"].get(chave_data_equipe, 0.0) + total_servicos
        )

        if h:
            chave_h = (h, equipe, data_serial)

            mapas["soma_h_data_equipe"][chave_h] = (
                mapas["soma_h_data_equipe"].get(chave_h, 0.0) + total_servicos
            )

            chave_i = h + data_serial + equipe

            if chave_i not in mapas["busca_i"]:
                mapas["busca_i"][chave_i] = obs_servico

        if j:
            chave_j = (j, equipe, data_serial)

            mapas["soma_j_data_equipe"][chave_j] = (
                mapas["soma_j_data_equipe"].get(chave_j, 0.0) + total_servicos
            )

            chave_k = j + data_serial + equipe

            if chave_k not in mapas["busca_k"]:
                mapas["busca_k"][chave_k] = obs_servico

    return mapas


def calcular_metricas_geral_j_n(dados_geral, mapas_bd):
    """
    Calcula GERAL!J:N como valores fixos.
    """

    resultados = []

    soma_h = mapas_bd.get("soma_h_data_equipe", {})
    soma_j = mapas_bd.get("soma_j_data_equipe", {})
    soma_total = mapas_bd.get("soma_data_equipe", {})
    busca_i = mapas_bd.get("busca_i", {})
    busca_k = mapas_bd.get("busca_k", {})

    for linha in dados_geral:
        linha = normalizar_linha(linha, 9)

        a = linha[0]
        b = texto_chave(linha[1])
        c = texto_chave(linha[2])
        e = numero_calculo(linha[4])
        f = numero_calculo(linha[5])
        h = texto_chave(linha[7])

        if texto_chave(a) == "":
            resultados.append(["", "", "", "", ""])
            continue

        data_serial = data_para_chave_serial(a)

        # J
        if e == 0:
            valor_j = 0.0
        else:
            valor_j = (
                soma_h.get((c, b, data_serial), 0.0)
                + soma_j.get((c, b, data_serial), 0.0)
            )

        # K
        if valor_j <= 0:
            valor_k = ""
        else:
            if h != "" and h != c:
                valor_k = 0
            else:
                if e > 0:
                    valor_k = valor_j / e
                else:
                    valor_k = 1

        # L
        valor_l = soma_total.get((data_serial, b), 0.0)

        # M
        if f == 0:
            valor_m = 0
        else:
            valor_m = valor_l / f

        # N
        chave_busca = c + data_serial + b

        valor_n = busca_i.get(chave_busca)

        if valor_n is None:
            valor_n = busca_k.get(chave_busca, "-")

        resultados.append([
            valor_j,
            valor_k,
            valor_l,
            valor_m,
            valor_n
        ])

    return resultados


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

    texto = _normalizar_separadores_ptbr(texto)

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


def bloco0_keep_only_columns_by_position(df, positions_1based):
    idx = [p - 1 for p in positions_1based]
    return df.iloc[:, idx]


def bloco0_to_number_ptbr(value):
    if value is None:
        return 0.0

    s = str(value).strip()

    if s == "" or s.lower() in ("nan", "none"):
        return 0.0

    s = s.replace(" ", "").replace(" ", "")

    s = _normalizar_separadores_ptbr(s)

    try:
        return float(s)
    except Exception:
        return 0.0


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


def bloco0_valor_texto(valor):
    if valor is None:
        return ""

    try:
        if pd.isna(valor):
            return ""
    except Exception:
        pass

    if isinstance(valor, float):
        if valor.is_integer():
            return str(int(valor))
        return str(valor)

    if isinstance(valor, int):
        return str(valor)

    return str(valor).strip()


def bloco0_extrair_coluna_h(valor_c):
    texto = bloco0_valor_texto(valor_c).replace("B-", "").strip()

    if not texto:
        return ""

    match = re.search(r"^\d+", texto)

    if match:
        return match.group(0)

    try:
        numero = float(texto.replace(",", "."))

        if numero.is_integer():
            return str(int(numero))

        return str(numero)

    except Exception:
        return ""


def bloco0_formatar_coluna_j(valor_h):
    h = bloco0_valor_texto(valor_h)

    if not h:
        return ""

    tamanho = len(h)

    if tamanho == 7:
        return f"B-{h}"

    if tamanho == 6:
        return f"B-0{h}"

    if tamanho == 5:
        return f"B-00{h}"

    return ""


def bloco0_converter_data_para_serial(valor_data):
    if valor_data is None:
        return ""

    try:
        if pd.isna(valor_data):
            return ""
    except Exception:
        pass

    if isinstance(valor_data, (int, float)):
        if float(valor_data).is_integer():
            return str(int(valor_data))
        return str(valor_data)

    texto = bloco0_valor_texto(valor_data)

    if not texto:
        return ""

    data_convertida = converter_para_data(texto)

    if data_convertida:
        serial = data_para_serial_google_sheets(data_convertida)
        return str(serial)

    try:
        numero = float(texto.replace(",", "."))

        if numero.is_integer():
            return str(int(numero))

        return str(numero)

    except Exception:
        return ""


def bloco0_montar_colunas_h_i_j_k(df_sheets):
    col_h = []
    col_i = []
    col_j = []
    col_k = []

    for _, row in df_sheets.iterrows():
        valor_c = row.iloc[2] if len(row) > 2 else ""
        valor_d = row.iloc[3] if len(row) > 3 else ""
        valor_f = row.iloc[5] if len(row) > 5 else ""

        h = bloco0_extrair_coluna_h(valor_c)

        d_txt = bloco0_valor_texto(valor_d)
        f_txt = bloco0_valor_texto(valor_f)

        if h == "" and f_txt == "" and d_txt == "":
            i = ""
        else:
            i = h + f_txt + d_txt

        j = bloco0_formatar_coluna_j(h)

        if h == "":
            k = ""
        else:
            serial_f = bloco0_converter_data_para_serial(valor_f)
            k = j + serial_f + d_txt

        col_h.append(h)
        col_i.append(i)
        col_j.append(j)
        col_k.append(k)

    df_sheets["H"] = col_h
    df_sheets["I"] = col_i
    df_sheets["J"] = col_j
    df_sheets["K"] = col_k

    return df_sheets


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
