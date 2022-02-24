from datetime import datetime, timedelta
import os


class enex_file_data:

    def __init__(self, *args, **kwargs):
        self.path = kwargs["path"]
        self.file = kwargs["file"]

    def existAndValid(self, **kwargs):
        valid = False
        try:
            exists = os.path.exists(self.path + self.file)
            if exists:
                file_size = os.path.getsize(self.path + self.file)
                if file_size > 385:
                    valid = True
        except BaseException as err:
            print('error al leer csv' + err)

        return valid


if __name__ == '__main__':
    path = 's3://enexsftp/'
    file = f'MC_DailyFile_{(datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")}.xls'
    en = enex_file_data(path=path, file=file)
    en.existAndValid()

