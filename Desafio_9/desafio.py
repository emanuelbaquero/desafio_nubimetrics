from datetime import datetime, timedelta

def generateLastDaysPaths(date, days):
    
    
    año = int(date[:4])
    mes = int(date[4:6])
    dia = int(date[6:])
    v_fecha = datetime(año, mes, dia)
    List=[]
    for i in range(0,days):
        List.append(f"https://importantdata@location/{v_fecha.year}/{v_fecha.month}/{v_fecha.day}/")
        v_fecha=v_fecha-timedelta(1)
    List=reversed(List)
    return list(List)

generateLastDaysPaths('20220210',13)
