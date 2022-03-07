import creeds
import requests
import pandas as pd
import utils
from datetime import datetime, timedelta, date


class get_data:
    token = creeds.token

    def __init__(self, *args, **kwargs):
        self.start = kwargs["start"]
        self.end = kwargs["end"]
        self.path_csv_S3 = kwargs["path"]
        self.host = kwargs["endPoint"]

    def capture(self):
        data = []

        try:
            #### ---- Definimos el header y body ---- ####
            headers = {"Authorization": self.token}
            page = 1
            pagePagination = 1
            # print('Download Info :' + self.host)
            while True:
                if page > pagePagination:
                    break

                body = {"start": self.start, "end": self.end, "page": page}
                solicitud = requests.get(self.host, headers=headers, params=body)
                response = solicitud.json()

                if response["status"] == requests.codes.ok:
                    pagePagination = int(response['totalpages'])
                    if len(response['data']) > 0:
                        for r in response['data']:
                            data.append(r)
                else:
                    # print('response invalid')
                    break

                page += 1

            if len(data) > 0:
                self.save(data)

        except Exception as e:
            print(f'Ocurrio un error al realizar la peticion: start = {self.start} , end = {self.end}   - error: {e}')

    def save(self, data):
        df_history = None
        data_new = []
        df_new = None
        exist = False

        try:
            df_history = pd.read_csv(self.path_csv_S3)
        except:
            print('error al leer csv historico')

        for d in data:
            if str(self.path_csv_S3).find('tickets.csv') > 0:
                if df_history is None:
                    exist = False
                else:
                    q = df_history.query(f"userid=='{d['userid']}' and number=={d['number']} and clubid=={d['clubid']} and paid_date=='{d['paid_date']}'")
                    exist = len(q) > 0

                if not exist:
                    charges = d['charges']
                    if isinstance(charges, list):
                        for c in charges:
                            data_new.append({
                                "userid": d["userid"],
                                "number": d["number"],
                                "clubid": d["clubid"],
                                "club_name": d["club_name"],
                                "paid_date": d["paid_date"],
                                "payments": d["payments"],
                                "vendorid": d["vendorid"],
                                "cashierid": d["cashierid"],
                                "productid": c["productid"],
                                'name': c["name"],
                                'value': c["value"],
                                'desct': c["desct"],
                                'total': c["total"]
                            })
                    else:
                        data_new.append({
                                "userid": d["userid"],
                                "number": d["number"],
                                "clubid": d["clubid"],
                                "club_name": d["club_name"],
                                "paid_date": d["paid_date"],
                                "payments": d["payments"],
                                "vendorid": d["vendorid"],
                                "cashierid": d["cashierid"],
                                "productid": "",
                                'name': "",
                                'value': "",
                                'desct': "",
                                'total': "",
                            })

            elif str(self.path_csv_S3).find('congelaciones.csv') > 0:
                if df_history is None:
                    exist = False
                else:
                    q = df_history.query(f"userid=='{d['userid']}' and clubid=={d['clubid']} and contractid=={d['contractid']} and comment=='{d['comment']}' and start=='{d['start']}' and end=='{d['end']}'")
                    exist = len(q) > 0

                if not exist:
                    data_new.append(d)
                else:
                    pass

        if df_history is None:
            df_new = pd.DataFrame.from_dict(data_new)
            utils.save_csv_gz(df_new, self.path_csv_S3)
        else:
            if len(data_new) > 0:
                df_consolidado = pd.concat([df_history, pd.DataFrame.from_dict(data_new)], ignore_index=True)
                utils.save_csv_gz(df_consolidado, self.path_csv_S3)

    def try_parse_int(self, value):
        try:
            return int(value)
        except:
            return value

    def try_strptime(self, value, fmts=None):
        d = value
        if fmts is None:
            fmts = '%d-%m-%Y'
        try:
            d = datetime.strptime(value, fmts)
        except:
            pass
        return d


if __name__ == '__main__':

    dateStart = (datetime.today() - timedelta(days=30)).date()
    dateStart = date(2010, 1, 1)
    while True:
        if dateStart > datetime.today().date():
            break
    
        start = dateStart.strftime("%Y-%m-%d 00:00:00")
        end = (dateStart + timedelta(days=30)).strftime("%Y-%m-%d 23:59:59")
        '''
        start = '2022-02-25 00:00:00'
        end = '2022-03-25 23:59:59'
        print('procesando informacion')
    
        path = 's3://karrott-sporlife/raw/congelaciones.csv.gz'
        endPoint = "https://sportlifesa.grupodtg.com/api/karrot/getFrozen"
        GF = get_data(start=start, end=end, path=path, endPoint=endPoint)
        GF.capture()
    
        path = 's3://karrott-sporlife/raw/asistencias.csv.gz'
        endPoint = "https://sportlifesa.grupodtg.com/api/karrot/getAssistance"
        GF = get_data(start=start, end=end, path=path, endPoint=endPoint)
        GF.capture()
        
        path = 's3://karrott-sporlife/raw/sesionesPT.csv.gz'
        endPoint = "https://sportlifesa.grupodtg.com/api/karrot/getSessionsPT"
        GF = get_data(start=start, end=end, path=path, endPoint=endPoint)
        GF.capture()
        
        path = 's3://karrott-sporlife/raw/clases_reservas.csv.gz'
        endPoint = "https://sportlifesa.grupodtg.com/api/karrot/getReservesClass"
    
        GF = get_data(start=start, end=end, path=path, endPoint=endPoint)
        GF.capture()
    
        path = 's3://karrott-sporlife/raw/tickets.csv.gz'
        endPoint = "https://sportlifesa.grupodtg.com/api/karrot/getTickets"
        GF = get_data(start=start, end=end, path=path, endPoint=endPoint)
        GF.capture()
        '''

        print(f'fecha start: {start}, fecha end: {end}')
        path = 's3://karrott-sporlife/raw/congelaciones.csv.gz'
        endPoint = "https://sportlifesa.grupodtg.com/api/karrot/getFrozen"
        GF = get_data(start=start, end=end, path=path, endPoint=endPoint)
        GF.capture()

        dateStart = dateStart + timedelta(days=30)

