"""Módulo blocos — gerado a partir do monólito compilador.py."""

import os
import pandas as pd
import time

from config import (
    BLOCO0_KEEP_COL_POS_1BASED,
    BLOCO0_NEW_FOLDER_ID,
    BLOCO0_OUTPUT_CSV_NAME,
    BLOCO0_READ_CSV_KWARGS,
    BLOCO0_UPLOAD_BANCO_PARA_DRIVE,
    BLOCO0_UPLOAD_FOLDER_ID,
    CELULA_DATA_REFERENCIA,
    CONFIG_ABA,
    DESTINO_RANGE_COMPLETO_COM_CHAVE,
    PAUSA_APOS_LEITURA,
    QTD_COLUNAS_DESTINO_COMPLETO,
    QTD_COLUNAS_DESTINO_GERAL,
)
from util import log
from parsers import (
    adicionar_chave_l,
    bloco0_construir_mapas_bd_consulta_serv,
    bloco0_keep_only_columns_by_position,
    bloco0_montar_colunas_h_i_j_k,
    bloco0_parse_date_por_arquivo,
    bloco0_to_number_ptbr,
    calcular_extras_geral,
    calcular_metricas_geral_j_n,
    construir_mapa_lookup,
    converter_para_data,
    linha_tem_dados,
    normalizar_linha,
    preparar_linha_para_envio,
    remover_linhas_vazias_base,
)
from google_io import (
    aplicar_formatacao_destino,
    bloco0_download_file,
    bloco0_list_files,
    bloco0_upload_or_update_banco,
    bloco0_upload_to_sheets,
    escrever_em_blocos,
    executar_com_retry,
    garantir_linhas_suficientes,
    ler_dados_csvs_bloco_3,
    ler_dados_origem_com_filtro_data,
    ler_dados_origem_sem_filtro_com_extra,
)


def atualizar_metricas_geral_j_n_todas_linhas(
    aba_destino,
    mapas_bd
):
    """
    Atualiza GERAL!J:N para todas as linhas existentes da aba GERAL.
    Tudo é gravado como valor, sem fórmula.
    """

    log("Atualizando GERAL!J:N para todas as linhas da aba GERAL...")

    dados_geral = executar_com_retry(
        lambda: aba_destino.get(
            "A4:I",
            value_render_option="UNFORMATTED_VALUE"
        ),
        descricao="ler GERAL!A4:I para atualizar J:N"
    )

    time.sleep(PAUSA_APOS_LEITURA)

    dados_geral = [
        normalizar_linha(linha, 9)
        for linha in dados_geral
    ]

    ultima_linha_util = 0

    for i, linha in enumerate(dados_geral):
        if linha_tem_dados(linha):
            ultima_linha_util = i + 1

    if ultima_linha_util == 0:
        log("Nenhuma linha útil encontrada na GERAL para atualizar J:N.")

        executar_com_retry(
            lambda: aba_destino.batch_clear(["J4:N"]),
            descricao="limpar GERAL!J4:N"
        )

        return

    dados_geral = dados_geral[:ultima_linha_util]

    valores_j_n = calcular_metricas_geral_j_n(
        dados_geral=dados_geral,
        mapas_bd=mapas_bd
    )

    log(f"Total de linhas que serão atualizadas em GERAL!J:N: {len(valores_j_n)}")

    executar_com_retry(
        lambda: aba_destino.batch_clear(["J4:N"]),
        descricao="limpar GERAL!J4:N"
    )

    escrever_em_blocos(
        aba=aba_destino,
        dados=valores_j_n,
        linha_inicial=4,
        coluna_inicial="J"
    )

    log("Atualização completa de GERAL!J:N finalizada.")


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


def executar_bloco_0(drive_service, sheets_service):
    log("")
    log("======================================")
    log("INICIANDO BLOCO 0 - BANCO.csv > BD_ConsultaServ")
    log("======================================")

    mapas_vazios = {
        "soma_h_data_equipe": {},
        "soma_j_data_equipe": {},
        "soma_data_equipe": {},
        "busca_i": {},
        "busca_k": {},
    }

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
        return mapas_vazios

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

    df_sheets_bloco0 = banco_df.iloc[:, :7].copy()
    df_sheets_bloco0 = df_sheets_bloco0.fillna("")
    df_sheets_bloco0 = bloco0_montar_colunas_h_i_j_k(df_sheets_bloco0)

    mapas_bd_consulta_serv = bloco0_construir_mapas_bd_consulta_serv(df_sheets_bloco0)

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

    return mapas_bd_consulta_serv


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
    intervalo_metricas = f"J{linha_inicio}:N{linha_fim}"
    intervalo_lookup = f"O{linha_inicio}:Q{linha_fim}"

    executar_com_retry(
        lambda: aba_destino.batch_clear([intervalo_dados, intervalo_metricas, intervalo_lookup]),
        descricao=f"limpar GERAL!{intervalo_dados}, GERAL!{intervalo_metricas} e GERAL!{intervalo_lookup}"
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


def executar_bloco_1(
    gc,
    planilha_destino,
    aba_config,
    ids_origem,
    cache_planilhas,
    cache_dados,
    mapa_plan_principal,
    mapa_reprogramadas,
    mapas_bd_consulta_serv
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

    atualizar_metricas_geral_j_n_todas_linhas(
        aba_destino=aba_destino,
        mapas_bd=mapas_bd_consulta_serv
    )

    atualizar_lookup_geral_todas_linhas(
        aba_destino=aba_destino,
        mapa_plan_principal=mapa_plan_principal,
        mapa_reprogramadas=mapa_reprogramadas
    )

    log("Bloco 1 finalizado com sucesso.")
