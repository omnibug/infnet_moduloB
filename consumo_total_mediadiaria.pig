/*
* ----------------------------------------------------------------
* Big Data
* BLOCO B
* Trabalho de Bloco
*
* carga de arquivo de leituras de consumo de energia em residência
* e agrupamentos por ano, mês, dia da semana e ano+mês, 
* Medição a cada minuto em Watt/hora
* ----------------------------------------------------------------
*/

REGISTER 'diadasemana.py' using jython AS udfpy;

hpc = LOAD '/user/horton/infnet/hpc_dataonly.txt' 
      USING PigStorage(';') 
      AS(
	Date: chararray,
	Time: chararray,
	Global_active_power: float,
	Global_reactive_power: float,
	Voltage: float,
	Global_intensity: float,
	Sub_metering_1: float,
	Sub_metering_2: float,
	Sub_metering_3: float);

hpc0 = FILTER hpc BY Global_active_power is not null and Date != 'Date';

hpc1 = FOREACH hpc0 GENERATE
	STRSPLIT(Date,'/',3),
	STRSPLIT(Time,':',3),
	Global_active_power,
	Global_reactive_power,
	Voltage,
	Global_intensity,
	Sub_metering_1,
	Sub_metering_2,
	Sub_metering_3;

hpc2 = FOREACH hpc1 GENERATE
	FLATTEN($0),
	FLATTEN($1),
	Global_active_power,
	Global_reactive_power,
	Voltage,
	Global_intensity,
	Sub_metering_1,
	Sub_metering_2,
	Sub_metering_3;

hpc3 = FOREACH hpc2 GENERATE
	((int)$2, (int)$1, (int)$0, (int)$3, (int)$4, 
	(int)udfpy.data2diadasemana((int)$2, (int)$1, (int)$0), 
	(float)$6, (float)$7, (float)$8, 
	(float)$9, (float)$10, (float)$11, (float)$12,
	(float)$6*1000/60 - (float)$10 - (float)$11 - (float)$12,
	(float)$6*1000/60 )
	AS ((ano:int , mes:int, dia:int, hora:int,  minuto:int, diasemana:int,
	activpwr:float, reactpwr:float, voltagem:float, intensidade:float, 
	cozinha:float, lavanderia:float, aquecimento:float, outros:float, total:float));


hpc4 = FOREACH hpc3 GENERATE FLATTEN ($0);


/*
* Agrupamento por Ano,Mês,Dia
*/
hpcanomesdia = GROUP hpc4 BY (ano,mes,dia,diasemana);

hpcanomesdiamedia = FOREACH hpcanomesdia GENERATE 
	FLATTEN(group), AVG(hpc4.total), SUM(hpc4.total);

hpcanomesdiamediaordenado = ORDER hpcanomesdiamedia BY $0,$1,$2;

--dump hpcanomesdiamediaordenado;
STORE hpcanomesdiamediaordenado INTO '/trabalho/results/consumo_mediadiaria' USING PigStorage(';');





