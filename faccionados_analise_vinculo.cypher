// RELACIONAMENTOS DOS FACCIONADOS
// ================================
// faccionados x faccoes (*)
// faccionados x bairros (*)
// fafaccionadosc x cidades (*)
// faccionados x idade
// faccionadosac x homicidas
// faccionadosac x estupradores
// faccionados x assaltantes

// ['nome_completo', 'vulgo_alcunha', 'faccao', 'faccao_funcao', 'bairro_atual', 'cidade_atual',
// 'uf_atual', 'area_atuacao', 'faccao_id', 'funcao_id', 'bairro_id', 'cidade_id', 'uf_id']

LOAD CSV WITH HEADERS FROM "file:///faccionado.csv" AS row
MERGE(faccionado:Faccionado
    {
        nameCompleto:COALESCE(row.nome_completo,"SEM NOME"),
        vulgoAlcunha:COALESCE(row.vulgo_alcunha,"SEM ALCUNHA"),
        faccaoID:COALESCE(row.faccao_id,"SEM FACCAO"),
        funcaoID:COALESCE(row.funcao_id,"SEM FUNCAO"),
        bairroID:COALESCE(row.bairro_id,"SEM BAIRRO"),
        cidadeID:COALESCE(row.cidade_id,"SEM CIDADE"),
        ufID:COALESCE(row.uf_id,"SEM UF"),
        name:COALESCE(row.nome_completo,"SEM NOME")
    }
)

WITH faccionado
LOAD CSV WITH HEADERS FROM "file:///faccao.csv" AS row
MERGE (faccao:Faccao
    {
        faccaoID:COALESCE(row.faccao_id,"SEM FACCAO"),
        faccaoName:COALESCE(row.faccao,"SEM FACCAO"),
        name:COALESCE(row.faccao,"SEM FACCAO")
    }
)

WITH faccionado, faccao
LOAD CSV WITH HEADERS FROM "file:///funcao.csv" AS row
MERGE (funcao:Funcao
    {
        funcaoID:COALESCE(row.funcao_id,"SEM FUNCAO"),
        funcaoName:COALESCE(row.faccao_funcao,"SEM FUNCAO"),
        name:COALESCE(row.faccao_funcao,"SEM FUNCAO")
    }
)

WITH faccionado, faccao, funcao
LOAD CSV WITH HEADERS FROM "file:///bairro.csv" AS row
MERGE (bairro:Bairro
    {
        bairroID:COALESCE(row.bairro_id,"SEM BAIRRO"),
        bairroName:COALESCE(row.bairro_atual,"SEM BAIRRO"),
        name:COALESCE(row.bairro_atual,"SEM BAIRRO")
    }
)

WITH faccionado, faccao, funcao, bairro
LOAD CSV WITH HEADERS FROM "file:///cidade.csv" AS row
MERGE (cidade:Cidade
    {
        cidadeID:COALESCE(row.cidade_id,"SEM CIDADE"),
        cidadeName:COALESCE(row.cidade_atual,"SEM CIDADE"),
        name:COALESCE(row.cidade_atual,"SEM CIDADE")
    }
)

WITH faccionado, faccao, funcao, bairro, cidade
LOAD CSV WITH HEADERS FROM "file:///uf.csv" AS row
MERGE (uf:Uf
    {
        ufID:COALESCE(row.uf_id,"SEM UF"),
        ufName:COALESCE(row.uf_atual,"SEM UF"),
        name:COALESCE(row.uf_atual,"SEM UF")
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
MERGE (faccionado)<-[:`COM A FUNCAO`]-(funcao)
MERGE (faccionado)<-[:`DA CIDADE`]-(cidade)
MERGE (faccionado)<-[:`DA UF`]-(uf)
RETURN *