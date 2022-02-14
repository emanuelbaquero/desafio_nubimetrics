from datetime import datetime

def generateMonthlyPathList(año, mes, dia):
    año = int(año)
    mes = int(mes)
    dia = int(dia)
    v_año = datetime(año, mes, dia).replace(day=1).year
    v_month = datetime(año, mes, dia).replace(day=1).month
    v_dia = datetime(año, mes, dia).replace(day=1).day
    List=[]
    while v_dia<=dia:
        List.append("https://importantdata@location/"+str(v_año)+"/"+str(v_month).zfill(2)+"/"+str(v_dia).zfill(2))
        v_dia=v_dia+1
    return List
        
generateMonthlyPathList("2021", "6", "30")
