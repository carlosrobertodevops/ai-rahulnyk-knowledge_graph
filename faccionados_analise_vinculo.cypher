// RELACIONAMENTOS DOS FACCIONADOS
// ================================
// faccionados x faccoes (*)
// faccionados x uncoes (*)
// faccionados x bairros (*)
// fafaccionadosc x cidades (*)
// faccionados x uf (*)

LOAD CSV WITH HEADERS FROM "file:///faccionado.csv" AS row
MERGE(faccionado:Faccionado
    {
        nameCompleto:row.nome_completo,
        vulgoAlcunha:row.vulgo_alcunha,
        faccaoID:row.faccao_id,
        funcaoID:row.funcao_id,
        bairroID:row.bairro_id,
        cidadeID:row.cidade_id,
        ufID:row.uf_id,
        name:row.nome_completo
    }
)

WITH faccionado
LOAD CSV WITH HEADERS FROM "file:///faccao.csv" AS row
MERGE (faccao:Faccao
    {
        faccaoID:row.faccao_id,
        faccaoName:row.faccao,
        name:row.faccao
    }
)

WITH faccionado, faccao
LOAD CSV WITH HEADERS FROM "file:///funcao.csv" AS row
MERGE (funcao:Funcao
    {
        funcaoID:row.funcao_id,
        funcaoName:row.faccao_funcao,
        name:row.faccao_funcao
    }
)

WITH faccionado, faccao, funcao
LOAD CSV WITH HEADERS FROM "file:///bairro.csv" AS row
MERGE (bairro:Bairro
    {
        bairroID:row.bairro_id,
        bairroName:row.bairro_atual,
        name:row.bairro_atual
    }
)

WITH faccionado, faccao, funcao, bairro
LOAD CSV WITH HEADERS FROM "file:///cidade.csv" AS row
MERGE (cidade:Cidade
    {
        cidadeID:row.cidade_id,
        cidadeName:row.cidade_atual,
        name:row.cidade_atual
    }
)

WITH faccionado, faccao, funcao, bairro, cidade
LOAD CSV WITH HEADERS FROM "file:///uf.csv" AS row
MERGE (uf:Uf
    {
        ufID:row.uf_id,
        ufName:row.uf_atual,
        name:row.uf_atual
    }
)

WITH faccionado, faccao, funcao, bairro, cidade, uf
MATCH(faccionado{faccaoID:faccionado.faccaoID})
MATCH(faccionado{funcaoID:faccionado.funcaoID})
MATCH(faccionado{bairroID:faccionado.bairroID})
MATCH(faccionado{cidadeID:faccionado.cidadeID})
MATCH(faccionado{ufID:faccionado.ufID})
MATCH(faccao{faccaoID:faccao.faccaoID})
MATCH(funcao{funcaoID:funcao.funcaoID})
MATCH(bairro{bairroID:bairro.bairroID})
MATCH(cidade{cidadeID:cidade.cidadeID})
MATCH(uf{ufID:uf.ufID})
WHERE faccao.faccaoID=faccionado.faccaoID AND
    funcao.funcaoID=faccionado.funcaoID AND
    bairro.bairroID=faccionado.bairroID AND
    cidade.cidadeID=faccionado.cidadeID AND
    uf.ufID=faccionado.ufID

MERGE (bairro)-[:`DO BAIRRO`]->(faccionado)<-[:`DA FACÇÃO`]->(faccao)
MERGE (funcao)-[:`COM A FUNCAO`]->(faccionado)
MERGE (cidade)-[:`DA CIDADE`]->(faccionado)
MERGE (uf)-[:`DA UF`]->(faccionado)
RETURN *