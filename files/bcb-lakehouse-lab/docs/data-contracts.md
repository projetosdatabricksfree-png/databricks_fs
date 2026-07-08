# Contratos de dados (resumo por camada)

## Bronze (imutável, crua, auditável)
| Tabela | Grão | Colunas técnicas | Evolução de schema |
|---|---|---|---|
| `sgs_raw` | 1 linha por arquivo coletado | `_arquivo_origem`, `_arquivo_modificado_em`, `_ingerido_em`, `_rescued_data` | addNewColumns |
| `clientes_cdc_raw` | 1 linha por evento CDC | idem | rescue (schema congelado) |
| `expectativas_raw` | 1 linha por coleta OData | idem | addNewColumns |

## Silver (tipada, validada, deduplicada)
| Tabela | Grão/chave | Expectations (modo) |
|---|---|---|
| `sgs_observacoes_stg` | append por observação | serie_presente (FAIL) · valor_valido (DROP) · serie_catalogada (WARN) |
| `sgs_observacoes` | (codigo_serie, data_ref) — SCD1 | herda da stg (a montante) |
| `clientes_cdc_stg` | append por evento | chave_presente (FAIL) · email_valido (DROP) · uf_conhecida (WARN) |
| `clientes_scd2` | (cliente_id, `__START_AT`) | vigente ⇔ `__END_AT IS NULL` |
| `expectativas_ipca` | boletim × mês de referência | mediana_valida (DROP) · referencia_valida (WARN) |

## Gold (consumo/BI — SELECT para `account users`)
| Objeto | Tipo | Conteúdo |
|---|---|---|
| `indicadores_mensais` | Materialized View | métricas mensais por série (avg/min/max/fechamento) |
| `ipca_expectativa_vs_realizado` | Materialized View | mediana Focus × IPCA realizado + surpresa |
| `clientes_atual` | Tabela c/ row filter + mask | fotografia vigente da dimensão |
| `vw_clientes_seguro` | View dinâmica | PII condicionada a grupo |

**SLO de qualidade:** taxa de violações (dropped/total) ≤ 5% por update — auditada
pelo `06_auditoria_qualidade.py`; acima disso a Gold não é certificada (ramo alerta).
