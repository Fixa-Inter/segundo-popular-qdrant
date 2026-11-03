<div align="center">

# FIXA · Sincronização Qdrant

Indexação vetorial dos dados operacionais do PostgreSQL.

![badge](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white) ![badge](https://img.shields.io/badge/Qdrant-Vector_DB-DC244C) ![badge](https://img.shields.io/badge/OpenAI-Embeddings-412991?logo=openai&logoColor=white) ![badge](https://img.shields.io/badge/License-MIT-green)

</div>

---

## ✨ Visão geral

O worker transforma textos operacionais do FIXA em vetores e os armazena com metadados usados pelas buscas semânticas dos demais serviços.

## 🚀 Funcionalidades

- Carga inicial e sincronização incremental
- Geração de embeddings em lotes
- Criação e validação de coleções vetoriais
- Verificação da dimensão e distância dos vetores
- Indexação dos principais domínios operacionais
- Cursor persistente para continuidade
- Janela de sobreposição contra perda de alterações
- Trava distribuída contra concorrência
- Execução automatizada a cada 10 minutos

## 🧰 Tecnologias

| Área | Tecnologias |
| --- | --- |
| **Worker** | Python |
| **Origem** | PostgreSQL |
| **Coordenação** | Redis |
| **Vetores** | Qdrant |
| **Embeddings** | OpenAI |
| **Acesso a dados** | Psycopg |
| **Automação** | GitHub Actions |

## 📄 Licença

Este projeto é distribuído sob a licença MIT. Consulte o arquivo [LICENSE](LICENSE).

---

<div align="center">
  Desenvolvido pela <strong>Fixa-Inter</strong>.
</div>
