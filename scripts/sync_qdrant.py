from __future__ import annotations

import argparse
import logging
import os
import uuid
from datetime import datetime, timedelta
from typing import Any

import redis
from dotenv import load_dotenv
from openai import OpenAI
from psycopg import connect
from psycopg.rows import dict_row
from qdrant_client import QdrantClient, models as modelos_qdrant


REGISTRO = logging.getLogger("sincronizacao_qdrant")
COLECOES = (
    "problema",
    "ocorrencia",
    "ordem_servico",
    "tarefa",
    "observacao_conclusao",
    "evento",
    "equipamento",
    "modelo_equipamento",
    "categoria_equipamento",
    "marca_equipamento",
    "local_endereco",
)
CONSULTAS: dict[str, tuple[str, str]] = {
    "problema": (
        """SELECT p.id, le.unidade_id,
                  COALESCE(NULLIF(BTRIM(p.descricao_problema), ''), p.titulo) AS texto,
                  p.esta_ativo
           FROM problema p
           JOIN local_endereco le ON le.id = p.local_endereco_id""",
        "p.data_atualizacao >= %s OR le.data_atualizacao >= %s",
    ),
    "ocorrencia": (
        """SELECT o.id, le.unidade_id,
                  COALESCE(NULLIF(BTRIM(o.descricao_ocorrencia), ''), o.titulo) AS texto,
                  o.esta_ativo
           FROM ocorrencia o
           JOIN local_endereco le ON le.id = o.local_endereco_id""",
        "o.data_atualizacao >= %s OR le.data_atualizacao >= %s",
    ),
    "ordem_servico": (
        """SELECT os.id, le.unidade_id,
                  COALESCE(NULLIF(BTRIM(p.descricao_problema), ''), p.titulo) AS texto,
                  os.esta_ativo
           FROM ordem_servico os
           JOIN problema p ON p.id = os.problema_id
           JOIN local_endereco le ON le.id = p.local_endereco_id""",
        "os.data_atualizacao >= %s OR p.data_atualizacao >= %s OR le.data_atualizacao >= %s",
    ),
    "tarefa": (
        """SELECT t.id, le.unidade_id,
                  COALESCE(NULLIF(BTRIM(t.descricao), ''), t.titulo) AS texto,
                  t.esta_ativo
           FROM tarefa t
           JOIN ordem_servico os ON os.id = t.ordem_servico_id
           JOIN problema p ON p.id = os.problema_id
           JOIN local_endereco le ON le.id = p.local_endereco_id""",
        "t.data_atualizacao >= %s OR os.data_atualizacao >= %s OR "
        "p.data_atualizacao >= %s OR le.data_atualizacao >= %s",
    ),
    "observacao_conclusao": (
        """SELECT oc.id, le.unidade_id, oc.observacao AS texto, oc.esta_ativo
           FROM observacao_conclusao oc
           JOIN ordem_servico os ON os.id = oc.ordem_servico_id
           JOIN problema p ON p.id = os.problema_id
           JOIN local_endereco le ON le.id = p.local_endereco_id""",
        "oc.data_atualizacao >= %s OR os.data_atualizacao >= %s OR "
        "p.data_atualizacao >= %s OR le.data_atualizacao >= %s",
    ),
    "evento": (
        """SELECT e.id, le.unidade_id,
                  COALESCE(NULLIF(BTRIM(e.descricao), ''), e.titulo) AS texto,
                  e.esta_ativo
           FROM evento e
           JOIN local_endereco le ON le.id = e.local_endereco_id""",
        "e.data_atualizacao >= %s OR le.data_atualizacao >= %s",
    ),
    "equipamento": (
        """SELECT eq.id, le.unidade_id,
                  COALESCE(NULLIF(BTRIM(CONCAT_WS(' ', me.descricao, le.descricao)), ''), eq.codigo) AS texto,
                  eq.esta_ativo
           FROM equipamento eq
           JOIN modelo_equipamento me ON me.id = eq.modelo_equipamento_id
           JOIN local_endereco le ON le.id = eq.local_endereco_id""",
        "eq.data_atualizacao >= %s OR me.data_atualizacao >= %s OR le.data_atualizacao >= %s",
    ),
    "modelo_equipamento": (
        """SELECT me.id, u.unidade_id,
                  COALESCE(NULLIF(BTRIM(me.descricao), ''), me.nome) AS texto,
                  me.esta_ativo
           FROM modelo_equipamento me
           JOIN categoria_equipamento ce ON ce.id = me.categoria_equipamento_id
           JOIN usuario u ON u.id = ce.usuario_id""",
        "me.data_atualizacao >= %s OR ce.data_atualizacao >= %s OR u.data_atualizacao >= %s",
    ),
    "categoria_equipamento": (
        """SELECT ce.id, u.unidade_id,
                  COALESCE(NULLIF(BTRIM(ce.descricao), ''), ce.nome) AS texto,
                  ce.esta_ativo
           FROM categoria_equipamento ce
           JOIN usuario u ON u.id = ce.usuario_id""",
        "ce.data_atualizacao >= %s OR u.data_atualizacao >= %s",
    ),
    "marca_equipamento": (
        """SELECT m.id, COALESCE(NULLIF(BTRIM(m.descricao), ''), m.nome) AS texto,
                  m.esta_ativo
           FROM marca_equipamento m""",
        "m.data_atualizacao >= %s",
    ),
    "local_endereco": (
        """SELECT le.id, le.unidade_id, le.descricao AS texto, le.esta_ativo
           FROM local_endereco le""",
        "le.data_atualizacao >= %s",
    ),
}


def obter_variavel_obrigatoria(nome: str) -> str:
    valor = os.getenv(nome)
    if not valor:
        raise RuntimeError(f"A variável de ambiente obrigatória não foi definida: {nome}")
    return valor


def buscar_registros_alterados(
    conexao: Any, colecao: str, alterado_desde: datetime | None
) -> list[dict[str, Any]]:
    consulta_base, condicao_incremental = CONSULTAS[colecao]
    if alterado_desde is None:
        consulta = consulta_base
        parametros: tuple[Any, ...] = ()
    else:
        consulta = f"{consulta_base} WHERE ({condicao_incremental})"
        parametros = (alterado_desde,) * condicao_incremental.count("%s")
    with conexao.cursor() as cursor:
        cursor.execute(consulta, parametros)
        return list(cursor.fetchall())


def preparar_colecoes(cliente: QdrantClient, dimensao_vetor: int) -> None:
    nomes_existentes = {item.name for item in cliente.get_collections().collections}
    for nome_colecao in COLECOES:
        if nome_colecao not in nomes_existentes:
            if nome_colecao == "local_endereco":
                raise RuntimeError(
                    "A collection local_endereco precisa existir antes da sincronização; "
                    "o worker não a cria"
                )
            cliente.create_collection(
                collection_name=nome_colecao,
                vectors_config=modelos_qdrant.VectorParams(
                    size=dimensao_vetor,
                    distance=modelos_qdrant.Distance.COSINE,
                ),
            )
            REGISTRO.info("Collection criada: %s", nome_colecao)
            continue

        informacoes = cliente.get_collection(collection_name=nome_colecao)
        configuracao_vetores = informacoes.config.params.vectors
        if not isinstance(configuracao_vetores, modelos_qdrant.VectorParams):
            raise RuntimeError(
                f"A collection {nome_colecao} usa vetores nomeados; o worker espera um vetor único"
            )
        if configuracao_vetores.size != dimensao_vetor:
            raise RuntimeError(
                f"A collection {nome_colecao} tem dimensão {configuracao_vetores.size}; "
                f"a configuração pede {dimensao_vetor}"
            )
        if configuracao_vetores.distance != modelos_qdrant.Distance.COSINE:
            raise RuntimeError(
                f"A collection {nome_colecao} precisa usar similaridade Cosine"
            )


def gerar_vetores(
    cliente_openai: OpenAI, textos: list[str], dimensao_vetor: int
) -> list[list[float]]:
    resposta = cliente_openai.embeddings.create(
        model=os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small"),
        input=textos,
        dimensions=dimensao_vetor,
    )
    vetores = [
        item.embedding
        for item in sorted(resposta.data, key=lambda item: item.index)
    ]
    if len(vetores) != len(textos):
        raise RuntimeError(
            f"A OpenAI retornou {len(vetores)} vetores para {len(textos)} textos"
        )
    return vetores


def sincronizar_colecao(
    conexao_pg: Any,
    cliente_qdrant: QdrantClient,
    cliente_openai: OpenAI,
    nome_colecao: str,
    alterado_desde: datetime | None,
    dimensao_vetor: int,
    tamanho_lote: int,
) -> int:
    registros = buscar_registros_alterados(conexao_pg, nome_colecao, alterado_desde)
    total_enviado = 0
    for inicio in range(0, len(registros), tamanho_lote):
        lote = registros[inicio : inicio + tamanho_lote]
        textos = [str(registro["texto"] or registro["id"]) for registro in lote]
        vetores = gerar_vetores(cliente_openai, textos, dimensao_vetor)
        pontos: list[modelos_qdrant.PointStruct] = []
        for registro, vetor in zip(lote, vetores, strict=True):
            carga: dict[str, Any] = {
                "entidade_id": registro["id"],
                "texto": registro["texto"],
                "esta_ativo": registro["esta_ativo"],
            }
            if nome_colecao != "marca_equipamento":
                carga["unidade_id"] = registro["unidade_id"]
            pontos.append(
                modelos_qdrant.PointStruct(
                    id=registro["id"], vector=vetor, payload=carga
                )
            )
        cliente_qdrant.upsert(
            collection_name=nome_colecao,
            points=pontos,
            wait=True,
        )
        total_enviado += len(pontos)
    REGISTRO.info("%s: %d ponto(s) enviado(s)", nome_colecao, total_enviado)
    return total_enviado


def liberar_trava(cliente_redis: redis.Redis, chave_trava: str, identificador: str) -> None:
    cliente_redis.eval(
        "if redis.call('get', KEYS[1]) == ARGV[1] then "
        "return redis.call('del', KEYS[1]) else return 0 end",
        1,
        chave_trava,
        identificador,
    )


def executar() -> int:
    load_dotenv()
    analisador = argparse.ArgumentParser(
        description="Sincroniza registros do PostgreSQL com o Qdrant"
    )
    analisador.add_argument(
        "--somente-criar-collections",
        action="store_true",
        help="Cria e valida collections sem sincronizar registros",
    )
    argumentos = analisador.parse_args()

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    url_qdrant = obter_variavel_obrigatoria("QDRANT_URL")
    dimensao_vetor = int(os.getenv("EMBEDDING_DIMENSION", "768"))
    chave_qdrant = os.getenv("QDRANT_API_KEY") or None
    tempo_limite_qdrant = int(os.getenv("QDRANT_TIMEOUT_SECONDS", "60"))

    if argumentos.somente_criar_collections:
        cliente_qdrant = QdrantClient(
            url=url_qdrant,
            api_key=chave_qdrant,
            timeout=tempo_limite_qdrant,
        )
        try:
            preparar_colecoes(cliente_qdrant, dimensao_vetor)
            REGISTRO.info(
                "Collections validadas com dimensão %d e similaridade Cosine",
                dimensao_vetor,
            )
            return 0
        finally:
            cliente_qdrant.close()

    url_banco = obter_variavel_obrigatoria("DATABASE_URL")
    url_redis = obter_variavel_obrigatoria("REDIS_URL")
    chave_openai = obter_variavel_obrigatoria("OPENAI_API_KEY")
    sobreposicao_minutos = int(os.getenv("SYNC_OVERLAP_MINUTES", "20"))
    tamanho_lote = int(os.getenv("SYNC_BATCH_SIZE", "32"))
    validade_trava = int(os.getenv("SYNC_LOCK_TTL_SECONDS", "3600"))
    modelo_embedding = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")
    chave_cursor = os.getenv(
        "REDIS_CURSOR_KEY", f"qdrant_sync:cursor:openai:{modelo_embedding}"
    )
    chave_trava = os.getenv("REDIS_LOCK_KEY", "qdrant_sync:lock")

    cliente_redis = redis.Redis.from_url(
        url_redis,
        decode_responses=True,
        socket_timeout=15,
    )
    identificador_trava = str(uuid.uuid4())
    if not cliente_redis.set(
        chave_trava,
        identificador_trava,
        nx=True,
        ex=validade_trava,
    ):
        REGISTRO.info("Outra sincronização está em andamento; encerrando")
        return 0

    try:
        conexao_pg = connect(url_banco, row_factory=dict_row)
        with conexao_pg.cursor() as cursor:
            cursor.execute("SET TIME ZONE 'America/Sao_Paulo'")
            cursor.execute("SHOW TIME ZONE")
            fuso_banco = cursor.fetchone()["TimeZone"]
            REGISTRO.info("Fuso da sessão PostgreSQL: %s", fuso_banco)
            cursor.execute("SELECT clock_timestamp() AS agora")
            inicio_execucao = cursor.fetchone()["agora"]
        conexao_pg.commit()
        conexao_pg.autocommit = True

        valor_cursor = cliente_redis.get(chave_cursor)
        if valor_cursor:
            cursor = datetime.fromisoformat(valor_cursor.replace("Z", "+00:00"))
            alterado_desde = cursor - timedelta(minutes=sobreposicao_minutos)
            REGISTRO.info(
                "Buscando alterações desde %s, com sobreposição",
                alterado_desde.isoformat(),
            )
        else:
            alterado_desde = None
            REGISTRO.info("Cursor ausente; iniciando carga completa")

        cliente_qdrant = QdrantClient(
            url=url_qdrant,
            api_key=chave_qdrant,
            timeout=tempo_limite_qdrant,
        )
        cliente_openai = OpenAI(api_key=chave_openai)
        try:
            preparar_colecoes(cliente_qdrant, dimensao_vetor)
            total_enviado = 0
            for nome_colecao in COLECOES:
                total_enviado += sincronizar_colecao(
                    conexao_pg,
                    cliente_qdrant,
                    cliente_openai,
                    nome_colecao,
                    alterado_desde,
                    dimensao_vetor,
                    tamanho_lote,
                )
            # O cursor usa o início da execução para que alterações durante o processamento
            # sejam recuperadas na próxima rodada.
            cliente_redis.set(chave_cursor, inicio_execucao.isoformat())
            REGISTRO.info(
                "Sincronização concluída: %d ponto(s); cursor=%s",
                total_enviado,
                inicio_execucao.isoformat(),
            )
        finally:
            conexao_pg.close()
            cliente_qdrant.close()
            cliente_openai.close()
        return 0
    finally:
        liberar_trava(cliente_redis, chave_trava, identificador_trava)
        cliente_redis.close()


if __name__ == "__main__":
    try:
        raise SystemExit(executar())
    except Exception:
        logging.exception("Falha na sincronização PostgreSQL → Qdrant")
        raise SystemExit(1)
