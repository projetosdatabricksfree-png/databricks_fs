"""Testes unitários (pytest) das funções puras — Exame S5 (DevOps Essentials).

Rodam sem Spark: `pytest tests/ -v` local/CI, ou via job_testes no Databricks.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from datetime import date
from lib.transforms import (normaliza_valor_brl, para_data_sgs, valida_serie,
                            ultima_observacao, deduplicar_por_chave)


def test_normaliza_valor_formato_api_json():
    assert normaliza_valor_brl("4.90") == 4.90

def test_normaliza_valor_formato_brasileiro():
    assert normaliza_valor_brl("1.234,56") == 1234.56
    assert normaliza_valor_brl("12,5") == 12.5

def test_normaliza_valor_entradas_invalidas():
    assert normaliza_valor_brl(None) is None
    assert normaliza_valor_brl("") is None
    assert normaliza_valor_brl("abc") is None

def test_para_data_sgs():
    assert para_data_sgs("01/07/2026") == date(2026, 7, 1)
    assert para_data_sgs("31/02/2026") is None
    assert para_data_sgs(None) is None

def test_valida_serie():
    assert valida_serie(11) and valida_serie("433")
    assert not valida_serie(9999) and not valida_serie("x")

def test_ultima_observacao():
    obs = [{"data": "01/07/2026", "valor": "1"},
           {"data": "03/07/2026", "valor": "3"},
           {"data": "xx", "valor": "9"}]
    assert ultima_observacao(obs)["valor"] == "3"
    assert ultima_observacao([]) is None

def test_deduplicar_por_chave_vence_maior_sequencia():
    linhas = [{"k": 1, "v": "velho", "seq": 1},
              {"k": 1, "v": "novo", "seq": 2},
              {"k": 2, "v": "unico", "seq": 1}]
    out = {r["k"]: r["v"] for r in deduplicar_por_chave(linhas, ["k"], "seq")}
    assert out == {1: "novo", 2: "unico"}

def test_deduplicar_por_chave_sequencia_ausente_nao_vence():
    # Sequência None não pode vencer uma válida nem quebrar a comparação (ADR-002).
    linhas = [{"k": 1, "v": "valido", "seq": 5},
              {"k": 1, "v": "sem_seq", "seq": None}]
    out = {r["k"]: r["v"] for r in deduplicar_por_chave(linhas, ["k"], "seq")}
    assert out == {1: "valido"}
    # Ambas None: mantém a primeira vista, sem lançar exceção.
    ambas_none = [{"k": 9, "v": "a", "seq": None}, {"k": 9, "v": "b", "seq": None}]
    out2 = {r["k"]: r["v"] for r in deduplicar_por_chave(ambas_none, ["k"], "seq")}
    assert out2 == {9: "a"}
