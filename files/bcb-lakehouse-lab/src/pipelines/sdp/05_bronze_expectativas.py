# ==============================================================================
# API PYTHON do SDP (Exame S3): mesmo pipeline, sintaxe por decorators.
# `import dlt` segue válido; o alias moderno é `from pyspark import pipelines as dp`.
# Parâmetros de `configuration` são lidos via spark.conf.get().
# ==============================================================================
import dlt
from pyspark.sql import functions as F

catalog = spark.conf.get("catalog")
bronze = spark.conf.get("schema_bronze")
silver = spark.conf.get("schema_silver")
landing = spark.conf.get("volume_landing")


@dlt.table(
    name=f"{catalog}.{bronze}.expectativas_raw",
    comment="Bronze: payload OData Olinda cru (Auto Loader via cloudFiles)",
    table_properties={"quality": "bronze"},
)
def expectativas_raw():
    return (
        spark.readStream.format("cloudFiles")            # Auto Loader clássico
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"{landing}/expectativas/")
        .select(
            "*",
            F.col("_metadata.file_path").alias("_arquivo_origem"),
            F.current_timestamp().alias("_ingerido_em"),
        )
    )


@dlt.table(
    name=f"{catalog}.{silver}.expectativas_ipca",
    comment="Silver: expectativas Focus (IPCA) achatadas — 1 linha por boletim×mês",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("mediana_valida", "mediana IS NOT NULL")
@dlt.expect("referencia_valida", "mes_referencia IS NOT NULL")   # warn
def expectativas_ipca():
    src = dlt.read_stream(f"{catalog}.{bronze}.expectativas_raw")
    return (
        src.select(F.explode("payload.value").alias("e"), "_ingerido_em")
        .select(
            F.col("e.Indicador").alias("indicador"),
            F.to_date("e.Data").alias("data_boletim"),
            # DataReferencia vem como 'MM/yyyy' → primeiro dia do mês:
            F.to_date(F.concat(F.lit("01/"), F.col("e.DataReferencia")),
                      "dd/MM/yyyy").alias("mes_referencia"),
            F.col("e.Media").cast("double").alias("media"),
            F.col("e.Mediana").cast("double").alias("mediana"),
            F.col("e.numeroRespondentes").cast("int").alias("respondentes"),
            "_ingerido_em",
        )
    )
