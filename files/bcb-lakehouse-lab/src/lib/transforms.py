"""Funções puras de transformação — testáveis sem Spark (Exame S5: modularidade).

Manter lógica de negócio em funções puras permite testes unitários rápidos
(pytest local/CI) e reuso em notebooks, UDFs e pipelines.
"""
from datetime import date, datetime

# Espelha a expectation WARN `serie_catalogada` (02_silver.sql). Fonte da verdade
# das séries é var.series_sgs (databricks.yml); literal mantido por ser check WARN.
SERIES_CATALOGADAS = {1, 11, 433}


def normaliza_valor_brl(valor):
    """Converte string numérica ('4.90', '1.234,56', '12,5') em float.

    Retorna None para entradas vazias/inválidas (nunca lança exceção —
    a decisão de descartar/alertar é da expectation, não da função).
    """
    if valor is None:
        return None
    if isinstance(valor, (int, float)):
        return float(valor)
    s = str(valor).strip()
    if not s:
        return None
    if "," in s:                      # formato brasileiro: '.' milhar, ',' decimal
        s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def para_data_sgs(texto):
    """Converte data SGS 'dd/MM/yyyy' em datetime.date; None se inválida."""
    try:
        return datetime.strptime(str(texto).strip(), "%d/%m/%Y").date()
    except (ValueError, TypeError):
        return None


def valida_serie(codigo, catalogo=SERIES_CATALOGADAS):
    """True se o código da série está catalogado (espelha a expectation WARN)."""
    try:
        return int(codigo) in catalogo
    except (TypeError, ValueError):
        return False


def ultima_observacao(observacoes):
    """Dado o array SGS [{'data': 'dd/MM/yyyy', 'valor': '...'}], retorna a
    observação mais recente com data válida; None se não houver."""
    validas = [o for o in (observacoes or []) if para_data_sgs(o.get("data"))]
    if not validas:
        return None
    return max(validas, key=lambda o: para_data_sgs(o["data"]))


def deduplicar_por_chave(linhas, chave, sequencia):
    """Dedup determinístico: mantém, por chave, a linha de maior `sequencia`.

    Espelha em Python puro a semântica do AUTO CDC SCD TYPE 1 / row_number()==1.
    """
    vencedores = {}
    for linha in linhas or []:
        k = tuple(linha.get(c) for c in chave)
        atual = vencedores.get(k)
        if atual is None:
            vencedores[k] = linha
            continue
        seq_nova, seq_atual = linha.get(sequencia), atual.get(sequencia)
        # Sequência ausente (None) nunca vence uma válida — espelha o requisito de
        # SEQUENCE BY monótona por chave do AUTO CDC (ADR-002). Empate/ambas None:
        # mantém a primeira vista (determinístico).
        if seq_nova is not None and (seq_atual is None or seq_nova > seq_atual):
            vencedores[k] = linha
    return list(vencedores.values())
