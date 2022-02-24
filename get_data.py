import creeds
import requests
import pandas as pd
import utils
from datetime import datetime, timedelta


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

            while True:
                if page > pagePagination:
                    break

                body = {"start": self.start, "end": self.end, "page": page}
                solicitud = requests.get(self.host, headers=headers, params=body)
                response = solicitud.json()

                if response["status"] == requests.codes.ok:
                    pagePagination = int(response['totalpages'])

                    for d in response['data']:
                        data.append(d)
                else:
                    print('response invalid')
                    break

                page += 1

            self.parseSpecialData(data)

        except Exception as e:
            print('Ocurrio un error al realizar la peticion')

    def parseSpecialData(self, data):

        data2 = []
        p = str(self.path_csv_S3)
        if p.find('tickets.csv') > 0:
            for d in data:
                charges = d['charges']
                if isinstance(charges, list):
                    for c in charges:
                        temp = d
                        temp["productid"] = c["productid"]
                        temp['name'] = c["name"]
                        temp['value'] = c["value"]
                        temp['desct'] = c["desct"]
                        temp['total'] = c["total"]

                        data2.append(temp)
                else:
                    data2.append(d)
        else:
            data2 = data

        self.save(data2)

    def save(self, data):
        """
        Transformamos la data de json object a dataframe, para poder guardarla
        """
        df_history = None
        # df = pd.DataFrame(kwargs["data"])
        df_now = pd.DataFrame.from_dict(data, orient='columns')

        #### ---- Concatenamos ---- ####
        try:
            df_history = pd.read_csv(self.path_csv_S3)
        except:
            print('error al leer csv')

        if df_history is None:
            df_now = pd.DataFrame(data)
            utils.save_csv_gz(df_now, self.path_csv_S3)
        else:
            df_consolidado = df_history.append(df_now, ignore_index=True)
            df_consolidado = df_consolidado.drop_duplicates(keep=False)
            utils.save_csv_gz(df_consolidado, self.path_csv_S3)
        #### ---- Guardamos ----- ####


if __name__ == '__main__':
    start = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")
    end = (datetime.today()).strftime("%Y-%m-%d")

    path = 's3://karrott-sporlife/raw/congelaciones.csv.gz'
    endPoint = "https://sportlifesa.grupodtg.com/api/karrot/getFrozen"
    GF = get_data(start=start, end=end, path=path, endPoint=endPoint)
    GF.capture()

    path = 's3://karrott-sporlife/raw/asistencias.csv.gz'
    endPoint = "https://sportlifesa.grupodtg.com/api/karrot/getAssistance"
    GF = get_data(start=start, end=end, path=path, endPoint=endPoint)
    GF.capture()


    path = 's3://karrott-sporlife/raw/tickets.csv.gz'
    endPoint = "https://sportlifesa.grupodtg.com/api/karrot/getTickets"
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
