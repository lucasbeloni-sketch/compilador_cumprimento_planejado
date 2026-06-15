"""
Compilador Cumprimento Planejado — ponto de entrada.

Orquestra os blocos 0/2/3/1. A lógica foi dividida em módulos:
  config     - constantes
  util       - log
  parsers    - conversões e transformações puras (cobertas por testes)
  google_io  - autenticação, retry e chamadas à API Google
  blocos     - orquestração de cada bloco
"""

import sys

from config import (
    CONFIG_ABA,
    DESTINO_ID,
    RANGE_IDS_ORIGEM,
)
from util import log
from google_io import (
    atualizar_timestamp_final,
    autenticar_google,
    executar_com_retry,
    executar_etapa,
    ler_ids_planilhas_origem,
)
from blocos import (
    executar_bloco_0,
    executar_bloco_1,
    executar_bloco_2,
    executar_bloco_3,
)


# Força logs em tempo real no GitHub Actions
try:
    sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass


def main():
    log("Iniciando compilador...")

    gc, drive_service, sheets_service = executar_etapa(
        "Autenticação Google",
        lambda: autenticar_google()
    )

    cache_planilhas = {}
    cache_dados = {}

    mapas_bd_consulta_serv = executar_etapa(
        "Bloco 0 - BANCO.csv > BD_ConsultaServ",
        lambda: executar_bloco_0(
            drive_service=drive_service,
            sheets_service=sheets_service
        )
    )

    def abrir_destino_e_ids():
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

        return planilha_destino, aba_config, ids_origem

    planilha_destino, aba_config, ids_origem = executar_etapa(
        "Abrir destino e ler IDs de origem",
        abrir_destino_e_ids
    )

    mapa_reprogramadas = executar_etapa(
        "Bloco 2 - REPROGRAMADAS",
        lambda: executar_bloco_2(
            gc=gc,
            planilha_destino=planilha_destino,
            ids_origem=ids_origem,
            cache_planilhas=cache_planilhas,
            cache_dados=cache_dados
        )
    )

    mapa_plan_principal = executar_etapa(
        "Bloco 3 - PLAN_PRINCIPAL",
        lambda: executar_bloco_3(
            gc=gc,
            drive_service=drive_service,
            planilha_destino=planilha_destino,
            ids_origem=ids_origem,
            cache_planilhas=cache_planilhas,
            cache_dados=cache_dados
        )
    )

    executar_etapa(
        "Bloco 1 - GERAL",
        lambda: executar_bloco_1(
            gc=gc,
            planilha_destino=planilha_destino,
            aba_config=aba_config,
            ids_origem=ids_origem,
            cache_planilhas=cache_planilhas,
            cache_dados=cache_dados,
            mapa_plan_principal=mapa_plan_principal,
            mapa_reprogramadas=mapa_reprogramadas,
            mapas_bd_consulta_serv=mapas_bd_consulta_serv
        )
    )

    executar_etapa(
        "Atualizar timestamp final",
        lambda: atualizar_timestamp_final(aba_config)
    )

    log("")
    log("Processo completo finalizado com sucesso.")


if __name__ == "__main__":
    try:
        main()
    except Exception as erro:
        log("")
        log("======================================")
        log(f"PROCESSO ABORTADO: {type(erro).__name__}: {erro}")
        log("======================================")
        sys.exit(1)
