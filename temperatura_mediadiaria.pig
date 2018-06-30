/*
* Big Data
* BLOCO B
* Trabalho de Bloco
*
* carga de arquivo de temperaturas
* e agrupamentos por ano, mês, dia da semana e ano+mês,
* ----------------------------------------------------------------
*/

REGISTER 'diadasemana.py' using jython AS udfpy;

t1 = LOAD '/user/horton/infnet/temperatura/stations.txt'
      USING PigStorage(',')
      AS(stid:int, estacao:chararray);

t2 = FILTER t1 BY stid is not null;

estacoes = FOREACH t2 GENERATE FLATTEN((stid,TRIM(estacao))) AS (stid:int, estacao:chararray);


s1 = LOAD '/user/horton/infnet/temperatura/sources.txt'
      USING PigStorage(',')
      AS(stid:int, scid:int);

sources = FILTER s1 BY scid is not null and stid is not null;


join_es = JOIN sources BY stid, estacoes BY stid;

est = FOREACH join_es GENERATE $1,$2,$3;



m1 = LOAD '/user/horton/infnet/temperatura/TG_STAID*.txt'
      USING PigStorage(',')
      AS(scid:int, data:chararray, temperatura:int, qualidade:int);

m2 = FILTER m1 BY scid is not null and qualidade == 0 and data > '20061200' and data < '20101200';

m3 = JOIN est BY scid, m2 BY scid;

m4 = GROUP m3 BY data;

m5 = FOREACH m4 GENERATE FLATTEN(group),AVG(m3.temperatura);

m6 = FOREACH m5 GENERATE SUBSTRING($0,0,4), SUBSTRING($0,4,6), SUBSTRING($0,6,8), $1;  

m7 = FOREACH m6 GENERATE $0,$1,$2, udfpy.data2diadasemana((int)$0, (int)$1, (int)$2), $3;  

medicoes_ordenadas = ORDER m7 BY $0,$1,$2;

--dump medicoes_ordenadas
STORE medicoes_ordenadas INTO 'infnet/results/temperatura_mediadiaria' USING PigStorage(';');

