from __future__ import annotations

import json

from safra_dados.connectors.bcb_ifdata import url_cadastro, url_valores
from safra_dados.olinda import com_paginacao, juntar_paginas, olinda_url

BASE = "https://olinda.bcb.gov.br/olinda/servico/IFDATA/versao/v1/odata"


def test_url_cadastro_igual_a_testada_manualmente():
    assert url_cadastro(BASE, "202606", top=5) == (
        f"{BASE}/IfDataCadastro(AnoMes=@AnoMes)?@AnoMes=202606&$top=5&$format=json"
    )


def test_url_valores_igual_a_testada_manualmente():
    assert url_valores(BASE, 202606, 1, "1") == (
        f"{BASE}/IfDataValores(AnoMes=@AnoMes,TipoInstituicao=@TipoInstituicao,Relatorio=@Relatorio)"
        "?@AnoMes=202606&@TipoInstituicao=1&@Relatorio='1'&$format=json"
    )


def test_chaves_nunca_codificadas():
    url = olinda_url(BASE, "X", {"A": 1}, filtro="Nome eq 'Itaú'", top=2, skip=4)
    assert "%40" not in url and "%24" not in url
    assert "$filter=Nome%20eq%20'Ita%C3%BA'" in url
    assert url.endswith("&$format=json")


def test_sem_args_usa_recurso_como_veio():
    assert olinda_url(BASE, "ListaDeRelatorio()") == f"{BASE}/ListaDeRelatorio()?$format=json"
    assert olinda_url(BASE, "R", formato=None) == f"{BASE}/R"


def test_string_com_aspas_e_duplicada():
    assert "@Nome='O''Brien'" in olinda_url(BASE, "F", {"Nome": "O'Brien"})


def test_com_paginacao():
    assert com_paginacao(f"{BASE}/R?$format=json", 10, 20).endswith("$format=json&$top=10&$skip=20")


class _Resp:
    def __init__(self, corpo, status=200):
        self._c, self.status_code = corpo, status
        self.content = json.dumps(corpo).encode()
        self.text = self.content.decode()

    def json(self):
        return self._c


class _Client:
    def __init__(self, respostas):
        self.respostas, self.urls = respostas, []

    def get(self, url):
        self.urls.append(url)
        return self.respostas[len(self.urls) - 1]


def test_juntar_paginas_sem_truncamento_devolve_corpo_original():
    client = _Client([_Resp({"value": [1, 2]})])
    assert json.loads(juntar_paginas(client, "u?$format=json")) == {"value": [1, 2]}
    assert len(client.urls) == 1


def test_juntar_paginas_com_nextlink_pagina_por_top_skip():
    client = _Client([
        _Resp({"value": [1], "@odata.nextLink": "x"}),
        _Resp({"value": [1, 2]}),
        _Resp({"value": [3, 4]}),
        _Resp({"value": [5]}),
    ])
    out = json.loads(juntar_paginas(client, "u?$format=json", pagina=2))
    assert out == {"value": [1, 2, 3, 4, 5]}
    assert client.urls[1].endswith("$top=2&$skip=0") and client.urls[3].endswith("$top=2&$skip=4")
