"""Apaga os pontos de todas as collections, preservando collections e índices."""

import argparse
import os

from dotenv import load_dotenv
from qdrant_client import QdrantClient, models


def apagar_pontos(cliente: QdrantClient, confirmar: bool = False) -> int:
    nomes = sorted(item.name for item in cliente.get_collections().collections)
    if not nomes:
        print("Nenhuma collection encontrada no Qdrant.")
        return 0

    quantidades = {nome: cliente.count(nome, exact=True).count for nome in nomes}
    print("Pontos que serão apagados:")
    for nome, quantidade in quantidades.items():
        print(f"  {nome}: {quantidade}")

    if not confirmar:
        try:
            resposta = input("Digite APAGAR para confirmar: ")
        except EOFError:
            resposta = ""
        if resposta != "APAGAR":
            print("Operação cancelada.")
            return 0

    total = 0
    for nome, quantidade in quantidades.items():
        cliente.delete(
            collection_name=nome,
            points_selector=models.FilterSelector(filter=models.Filter()),
            wait=True,
        )
        restantes = cliente.count(nome, exact=True).count
        if restantes:
            raise RuntimeError(
                f"A collection {nome} ainda tem {restantes} ponto(s) após a exclusão"
            )
        total += quantidade
        print(f"{nome}: {quantidade} ponto(s) apagado(s)")

    print(f"Concluído: {total} ponto(s) apagado(s); collections preservadas.")
    return total


def main() -> None:
    load_dotenv()
    analisador = argparse.ArgumentParser(description=__doc__)
    analisador.add_argument(
        "--sim", action="store_true", help="Executa sem pedir confirmação interativa"
    )
    argumentos = analisador.parse_args()

    url = os.getenv("QDRANT_URL")
    if not url:
        raise RuntimeError("Defina QDRANT_URL no ambiente ou no arquivo .env")

    cliente = QdrantClient(
        url=url,
        api_key=os.getenv("QDRANT_API_KEY") or None,
        timeout=int(os.getenv("QDRANT_TIMEOUT_SECONDS", "60")),
    )
    try:
        apagar_pontos(cliente, confirmar=argumentos.sim)
    finally:
        cliente.close()


if __name__ == "__main__":
    main()
