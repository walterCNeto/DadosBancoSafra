"""Calendario de publicacao por fonte, usado por `safra-dados atualizar` para decidir quais fontes
estao com publicacao vencida (ha uma data-base que ja deveria estar disponivel e ainda nao foi
coletada, ou cuja coleta ja envelheceu alem do `refresh_dias`).

`data_base_esperada(fonte, hoje)` devolve a data-base mais recente que ja deveria ter sido publicada em
`hoje`, no formato do checkpoint da fonte (AAAAMM, ou AAAA para as fontes anuais da CVM). Os prazos de
IF.data e Cosif vem dos proprios conectores; os demais sao prazos tipicos observados (conservadores:
se a fonte ainda nao publicou, `ingest` simplesmente nao encontra nada e tenta de novo no dia seguinte).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Callable

from .manifest import Checkpoint
from .periodos import mes_anterior


def _aaaamm(d: date) -> str:
    return f"{d.year}{d.month:02d}"


def _fim_mes(ano: int, mes: int) -> date:
    return (date(ano + (mes == 12), mes % 12 + 1, 1)) - timedelta(days=1)


def _mes_menos(hoje: date, n: int) -> str:
    ano, mes = hoje.year, hoje.month - n
    while mes <= 0:
        ano, mes = ano - 1, mes + 12
    return f"{ano}{mes:02d}"


def _mes_fechado_com_prazo(hoje: date, prazo_dias: int) -> str:
    """Ultimo mes cujo fim + prazo ja passou."""
    ano, mes = hoje.year, hoje.month
    for _ in range(24):
        ano, mes = (ano, mes - 1) if mes > 1 else (ano - 1, 12)
        if _fim_mes(ano, mes) + timedelta(days=prazo_dias) <= hoje:
            return f"{ano}{mes:02d}"
    return f"{ano}{mes:02d}"


def _trimestre_fechado_com_prazo(hoje: date, prazo_dias: int) -> str:
    ano, mes = hoje.year, 12
    for _ in range(12):
        for m in (12, 9, 6, 3):
            if (ano, m) <= (hoje.year, hoje.month) and _fim_mes(ano, m) + timedelta(days=prazo_dias) <= hoje:
                return f"{ano}{m:02d}"
        ano -= 1
    return f"{ano}12"


def _ifdata(hoje: date) -> str:
    from .connectors import bcb_ifdata
    return bcb_ifdata.latest_available_anomes(hoje)


def _cosif(hoje: date) -> str:
    from .connectors import bcb_cosif_balancetes
    return bcb_cosif_balancetes.latest_available_anomes(hoje)


def _ano_dfp(hoje: date) -> str:
    # DFP: prazo legal de 3 meses apos o exercicio (publicada ate 31/03); margem ate 30/04
    return str(hoje.year - 1 if hoje >= date(hoje.year, 4, 30) else hoje.year - 2)


@dataclass(frozen=True)
class Calendario:
    esperada: Callable[[date], str | None]
    refresh_dias: int | None = None  # recoleta a data-base esperada se a ultima coleta for mais velha que isso
    descricao: str = ""


CALENDARIOS: dict[str, Calendario] = {
    "bcb_ifdata": Calendario(_ifdata, None, "trimestral, divulgado 60 dias (90 no 4T) apos a data-base"),
    "bcb_cosif": Calendario(_cosif, None, "trimestral, calendario oficial do Cosif"),
    "bcb_taxas_juros": Calendario(lambda h: _mes_menos(h, 1 if h.day >= 5 else 2), None, "mensal, ate o dia 5 do mes seguinte"),
    "bcb_reclamacoes": Calendario(lambda h: _trimestre_fechado_com_prazo(h, 50), None, "trimestral, ~50 dias apos o trimestre"),
    "bcb_tarifas": Calendario(lambda h: _aaaamm(h), None, "instantaneo: um retrato por mes de coleta"),
    "bcb_estban": Calendario(lambda h: _mes_fechado_com_prazo(h, 50), None, "mensal, ~50 dias apos o mes"),
    "bcb_unicad": Calendario(lambda h: mes_anterior(h), None, "mensal (retrato do ultimo dia do mes)"),
    "bcb_sgs": Calendario(lambda h: _aaaamm(h), 1, "series diarias: recoleta diaria do mes corrente"),
    "cvm_cadastros": Calendario(lambda h: _aaaamm(h), None, "instantaneo: um retrato por mes de coleta"),
    "cvm_fundos_cad": Calendario(lambda h: _aaaamm(h), None, "instantaneo: um retrato por mes de coleta"),
    "cvm_fundos_informe": Calendario(lambda h: _aaaamm(h), 1, "arquivo do mes corrente atualizado diariamente"),
    "cvm_fundos_cda": Calendario(lambda h: mes_anterior(h), None, "mensal, ate ~20 dias apos o mes"),
    "cvm_ofertas": Calendario(lambda h: _aaaamm(h), None, "instantaneo: um retrato por mes de coleta"),
    "cvm_dfp": Calendario(_ano_dfp, None, "anual, ate 30/04 do ano seguinte"),
    "cvm_itr": Calendario(lambda h: str(h.year), 7, "ano corrente, recoleta semanal"),
    "cvm_fre": Calendario(lambda h: str(h.year), 30, "ano corrente, recoleta mensal"),
    "cvm_ipe": Calendario(lambda h: str(h.year), 1, "eventos: recoleta diaria do ano corrente"),
    "susep_ses": Calendario(lambda h: _mes_menos(h, 2 if h.day >= 17 else 3), None, "mensal, carga da base ~dia 16, defasagem de 2 meses"),
    "safra_ri": Calendario(lambda h: _aaaamm(h), 7, "catalogo de RI: nova leitura da pagina a cada 7 dias"),
}


def _idade_dias(atualizado_em: str | None, agora: datetime) -> float:
    if not atualizado_em:
        return float("inf")
    try:
        quando = datetime.fromisoformat(atualizado_em)
    except ValueError:
        return float("inf")
    if quando.tzinfo is None:
        quando = quando.replace(tzinfo=timezone.utc)
    return (agora - quando).total_seconds() / 86400


def vencida(fonte: str, checkpoint: Checkpoint, hoje: date, agora: datetime | None = None) -> tuple[bool, str]:
    """(vencida?, motivo). Fonte sem calendario nunca vence (so roda por `ingest`)."""
    cal = CALENDARIOS.get(fonte)
    if cal is None:
        return False, "sem calendario"
    esperada = cal.esperada(hoje)
    if esperada is None:
        return False, "sem data-base esperada"
    info = checkpoint.data_bases.get(esperada)
    if info is None:
        return True, f"data-base {esperada} esperada e nao coletada"
    if info.get("arquivos_falha", 0) > 0:
        return True, f"data-base {esperada} coletada com {info['arquivos_falha']} falha(s)"
    if cal.refresh_dias is not None:
        agora = agora or datetime.now(timezone.utc)
        idade = _idade_dias(info.get("atualizado_em"), agora)
        if idade >= cal.refresh_dias:
            return True, f"data-base {esperada} coletada ha {idade:.1f} dia(s) (refresh a cada {cal.refresh_dias})"
    return False, f"data-base {esperada} em dia"


# fontes em que `--desde` filtra o CONTEUDO (periodo dos dados / data-base de referencia dos documentos),
# nao so quais arquivos baixar: usam sempre o `desde` configurado para nao truncar a silver
DESDE_CONFIGURADO = {"susep_ses", "safra_ri"}


def desde_para_atualizar(fonte: str, checkpoint: Checkpoint, padrao: str) -> str:
    """Data-base inicial (AAAA-MM) para a atualizacao: recomeca da ultima coleta ok (idempotente, pega
    periodos novos e republicacoes); sem coleta previa, usa o `desde` configurado."""
    ultima = checkpoint.ultima_data_base_ok
    if not ultima or fonte in DESDE_CONFIGURADO:
        return padrao
    if len(ultima) == 4:  # fontes anuais
        return f"{ultima}-01"
    return f"{ultima[:4]}-{ultima[4:6]}"
