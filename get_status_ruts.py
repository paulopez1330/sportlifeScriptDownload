import creeds
import requests

if __name__ == '__main__':

    def status_ruts(**kwargs):
        data = []
        headers = {"Authorization": kwargs["token"]}

        for user in kwargs["ruts"]:
            try:
                body = {"lastrow": 0, "clubid": kwargs["clubid"], "userid": str(user).replace(".", "").replace("-", "").replace("_", "")}
                solicitud = requests.get(kwargs["endPoint"], headers=headers, params=body)
                response = solicitud.json()

                if response["status"] == requests.codes.ok:
                    if len(response['data']) > 0:
                        for r in response['data']:
                            data.append(r)
                    else:
                        print('response invalid')
                        break
            except Exception as e:
                print(
                    f'Ocurrio un error al realizar la peticion: clubid = {kwargs["clubid"]} , user = {user}   - error: {e}')

        return data

    endPoint = "https://sportlifesa.grupodtg.com/api/karrot/getStatus/"
    token = creeds.token
    ruts = ['231204377', '55-3']
    clubid = 3
    out_data = status_ruts(endPoint=endPoint, token=token, clubid=clubid, ruts=ruts)
    print(out_data)
