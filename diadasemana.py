import datetime

@outputSchema('diadasemana:int')
def data2diadasemana(ano, mes, dia):
    return (datetime.datetime(ano, mes, dia)).weekday()

	
