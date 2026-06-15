"""
Testes das funções puras de parsing/transformação do compilador.

Cobrem a lógica de negócio que não depende de chamadas à API Google:
datas, números pt-BR, moeda, chaves e helpers do Bloco 0.

Rodar: pytest -q
"""

import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import compilador as c


# ---------------------------------------------------------------------------
# converter_para_data
# ---------------------------------------------------------------------------

class TestConverterParaData:
    def test_formato_br(self):
        assert c.converter_para_data("15/06/2026") == date(2026, 6, 15)

    def test_formato_iso(self):
        assert c.converter_para_data("2026-06-15") == date(2026, 6, 15)

    def test_ano_dois_digitos(self):
        assert c.converter_para_data("15/06/26") == date(2026, 6, 15)

    def test_com_hora_ignora_hora(self):
        assert c.converter_para_data("15/06/2026 13:45:00") == date(2026, 6, 15)

    def test_serial_google_sheets(self):
        # 45000 dias após 1899-12-30
        assert c.converter_para_data(45000) == date(2023, 3, 15)

    def test_objeto_date(self):
        d = date(2026, 1, 1)
        assert c.converter_para_data(d) == d

    def test_vazio_retorna_none(self):
        assert c.converter_para_data("") is None
        assert c.converter_para_data(None) is None

    def test_lixo_retorna_none(self):
        assert c.converter_para_data("não é data") is None


# ---------------------------------------------------------------------------
# data_para_serial_google_sheets (round-trip com converter_para_data)
# ---------------------------------------------------------------------------

class TestSerial:
    def test_round_trip(self):
        d = date(2026, 6, 15)
        serial = c.data_para_serial_google_sheets(d)
        assert c.converter_para_data(serial) == d

    def test_epoca(self):
        assert c.data_para_serial_google_sheets(date(1899, 12, 31)) == 1


# ---------------------------------------------------------------------------
# numero_calculo
# ---------------------------------------------------------------------------

class TestNumeroCalculo:
    def test_decimal_br(self):
        assert c.numero_calculo("1.234,56") == 1234.56

    def test_so_virgula(self):
        assert c.numero_calculo("12,5") == 12.5

    def test_moeda(self):
        assert c.numero_calculo("R$ 1.000,00") == 1000.0

    def test_numerico_passa_direto(self):
        assert c.numero_calculo(42) == 42.0
        assert c.numero_calculo(3.14) == 3.14

    def test_vazio_zero(self):
        assert c.numero_calculo("") == 0.0
        assert c.numero_calculo(None) == 0.0

    def test_lixo_zero(self):
        assert c.numero_calculo("abc") == 0.0


# ---------------------------------------------------------------------------
# converter_moeda_para_numero
# ---------------------------------------------------------------------------

class TestConverterMoeda:
    def test_moeda_br(self):
        assert c.converter_moeda_para_numero("R$ 1.234,56") == 1234.56

    def test_parenteses_negativo(self):
        assert c.converter_moeda_para_numero("(1.000,00)") == -1000.0

    def test_negativo_sinal(self):
        assert c.converter_moeda_para_numero("-50,00") == -50.0

    def test_traco_vira_vazio(self):
        assert c.converter_moeda_para_numero("-") == ""
        assert c.converter_moeda_para_numero("") == ""

    def test_milhar_sem_decimal(self):
        # "1.234" => 1234 (3 dígitos no último grupo = milhar)
        assert c.converter_moeda_para_numero("1.234") == 1234.0

    def test_numerico_passa_direto(self):
        assert c.converter_moeda_para_numero(10) == 10


# ---------------------------------------------------------------------------
# bloco0_to_number_ptbr  (alvo do fix de corrupção de milhar)
# ---------------------------------------------------------------------------

class TestBloco0ToNumber:
    def test_milhar_so_ponto(self):
        # Regressão do bug: antes retornava 1.234
        assert c.bloco0_to_number_ptbr("1.234") == 1234.0

    def test_milhar_grande(self):
        assert c.bloco0_to_number_ptbr("1.000.000") == 1000000.0

    def test_milhar_decimal(self):
        assert c.bloco0_to_number_ptbr("1.234,56") == 1234.56

    def test_decimal_dois_digitos_preservado(self):
        assert c.bloco0_to_number_ptbr("1.50") == 1.5

    def test_so_virgula(self):
        assert c.bloco0_to_number_ptbr("1234,56") == 1234.56

    def test_inteiro(self):
        assert c.bloco0_to_number_ptbr("12") == 12.0

    def test_vazio_e_nan(self):
        assert c.bloco0_to_number_ptbr("") == 0.0
        assert c.bloco0_to_number_ptbr("nan") == 0.0
        assert c.bloco0_to_number_ptbr(None) == 0.0


# ---------------------------------------------------------------------------
# calcular_chave_linha  (Q = A & B & C & E, pula índice 3)
# ---------------------------------------------------------------------------

class TestChave:
    def test_concatena_a_b_c_e(self):
        linha = ["A", "B", "C", "D", "E"]
        assert c.calcular_chave_linha(linha) == "ABCE"

    def test_primeira_coluna_vazia_retorna_vazio(self):
        assert c.calcular_chave_linha(["", "B", "C", "D", "E"]) == ""

    def test_floats_inteiros_sem_ponto_zero(self):
        linha = [1.0, 2.0, 3.0, 9.0, 5.0]
        assert c.calcular_chave_linha(linha) == "1235"


# ---------------------------------------------------------------------------
# normalizar_linha
# ---------------------------------------------------------------------------

class TestNormalizarLinha:
    def test_preenche_faltantes(self):
        assert c.normalizar_linha(["a"], 3) == ["a", "", ""]

    def test_trunca_excedente(self):
        assert c.normalizar_linha(["a", "b", "c", "d"], 2) == ["a", "b"]


# ---------------------------------------------------------------------------
# helpers Bloco 0: coluna H e J
# ---------------------------------------------------------------------------

class TestBloco0HJ:
    def test_extrair_h_remove_prefixo_b(self):
        assert c.bloco0_extrair_coluna_h("B-1234567") == "1234567"

    def test_extrair_h_so_digitos_iniciais(self):
        assert c.bloco0_extrair_coluna_h("123abc") == "123"

    def test_extrair_h_vazio(self):
        assert c.bloco0_extrair_coluna_h("") == ""

    def test_formatar_j_7_digitos(self):
        assert c.bloco0_formatar_coluna_j("1234567") == "B-1234567"

    def test_formatar_j_6_digitos_zero_pad(self):
        assert c.bloco0_formatar_coluna_j("123456") == "B-0123456"

    def test_formatar_j_5_digitos_zero_pad(self):
        assert c.bloco0_formatar_coluna_j("12345") == "B-0012345"

    def test_formatar_j_tamanho_invalido(self):
        assert c.bloco0_formatar_coluna_j("12") == ""


# ---------------------------------------------------------------------------
# detectar_delimitador_csv
# ---------------------------------------------------------------------------

class TestDelimitador:
    def test_ponto_virgula(self):
        assert c.detectar_delimitador_csv("a;b;c\n1;2;3") == ";"

    def test_virgula(self):
        assert c.detectar_delimitador_csv("a,b,c\n1,2,3") == ","


# ---------------------------------------------------------------------------
# linha_tem_dados
# ---------------------------------------------------------------------------

class TestLinhaTemDados:
    def test_com_dados(self):
        assert c.linha_tem_dados(["", "", "x"]) is True

    def test_vazia(self):
        assert c.linha_tem_dados(["", "  ", ""]) is False
