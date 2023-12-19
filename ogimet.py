import shutil
import time
from calendar import monthrange
import os
from sys import argv
import logging
from pathlib import Path
from datetime import datetime

import requests
from lxml import html
from dateutil.rrule import rrule, MONTHLY


logger = logging.getLogger(__name__)


class RequestDataFailed(Exception):
    pass


class Downloader():
    """docstring for Downloader."""

    def __init__(self):
        if os.getcwd().__contains__("\\"):
            self.sep = "\\"
        else:
            self.sep = "/"

        self.temptcolnames = ['Max', 'Min', 'Avg']
        self.windcolnames = ['Dir.', 'Int.']
        self.sumcolnames = ['03', '06', '09', '12', '15', '18', '21', '24']
        self.comb = {}

    def month_iter(self, start_month, start_year, end_month, end_year):
        start = datetime(start_year, start_month, 1)
        end = datetime(end_year, end_month, 1)
        r = ((d.month, d.year) for d in rrule(MONTHLY,dtstart=start, until=end))
        return r

    def tryGetTable(self, year, month):
        logger.debug("Calling tryGetTable")

        link = self.linkConstructor(year, month)

        logger.debug(f"Contructed link: {link}")

        tree = self.requestData(link)

        try:
            table = tree.xpath('//table[@border="0"]')[0]
        except IndexError as e:
            logger.error(f"Error- no table found: {e}")
            raise e
        except AttributeError as e:
            logger.error(f"requestData failed: {e}")
            raise e

        logger.debug(f"Returning table={table}")
        return table


    def requestData(self,link, attempt=10):
        logger.debug(f"Calling requestData on {link}")
        logger.debug(f"attempt={attempt}")

        if attempt == 0:
            raise RequestDataFailed

        try:
            page = requests.get(link, timeout=5)
        except (requests.exceptions.ReadTimeout, \
                requests.exceptions.ConnectionError, \
                requests.exceptions.ConnectTimeout):
            time.sleep(5)
            return self.requestData(link, attempt=attempt - 1)

        logger.debug(f"Returned content = {page.content[:100]}")

        if page.status_code != 200:
            logger.debug(f"Retrying requestData. status_code = {page.status_code}.")
            return self.requestData(link, attempt=attempt-1)

        tree = html.fromstring(page.content)
        return tree


    def running_all(self, end_year, end_month, start_year=2000, start_month=1, stationid="", location=os.getcwd()):
        logger.info(f"Downloading data for station={stationid}, {start_year}-{start_month} to {end_year}-{end_month}\n" + "=" * 32)
        self.stationid = stationid

        # Make dir
        outdir = Path(location) / stationid / (str(start_year) + "-" + str(start_month) + "-"+ str(end_year) + "-" + str(end_month))

        if outdir.exists():
            logger.error(f"Directory {outdir} already exists. Skipping.")
            raise FileExistsError

        else:
            outdir.mkdir(parents=True)
        
        self.location = str(outdir)

        success = True
        for m in self.month_iter(start_month, start_year, end_month, end_year):
            try:
                success = success & self.writeData(m[1], m[0])
            except RequestDataFailed as e:
                logger.critical(f"RequestData failed. Is the internet down? Cleaning up {outdir} and stopping.")
                shutil.rmtree(outdir)
                raise e


        if not success:
            logger.critical(f"Processing failed. Cleaning up {outdir}")
            shutil.rmtree(outdir)




    def linkConstructor(self, year, month):
        link = "https://www.ogimet.com/cgi-bin/gsynres?lang=en&ind="+ \
        self.stationid +"&ndays=" + monthrange(year, month)[1].__str__() + \
        "&ano=" + year.__str__() + "&mes=" + "%02d" % month + "&day=" + \
        "%02d" % monthrange(year, month)[1] + "&hora=00&ord=REV&Send=Send"
        return link


    def getcolum(self, table):

        if table is None:
            logger.error("Table is none!")
            assert 0
            
        expected_cols = ['Date', 'Temperature(C)Max', 'Temperature(C)Min', 'Temperature(C)Avg', 'TdAvg(C)', 'Hr.Avg(%)', 'Wind(km/h)Dir.', 'Wind(km/h)Int.', 'Pres.s.lev(Hp)', 'Prec.(mm)', 'VisKm']

        colnames = []
        for a in table.getchildren()[1][0][:]:
            if a.text_content().__contains__("Temperature"):
                for b in table.getchildren()[1][1]:
                    if self.temptcolnames.__contains__(b.text_content()):
                        col = a.text_content().strip() + b.text_content()
                        colnames.append(col)
            elif a.text_content().__contains__("Wind"):
                for b in table.getchildren()[1][1]:
                    if self.windcolnames.__contains__(b.text_content()):
                        col = a.text_content() + b.text_content()
                        colnames.append(col)
            else:
                col = a.text_content()

                if col in expected_cols:
                    colnames.append(col)

        return colnames

    def writeData(self, year, month) -> bool:
        logger.debug("Starting writeData")

        try:
            table = self.tryGetTable(year, month)
        except IndexError:
            # No data for this year/month - return true since we don't need to revisit this
            return True
        except AttributeError:
            # Something else has gone wrong - try again later
            return False

        colnames = self.getcolum(table)

        logger.debug(f"Found column names: {colnames}")

        if len(colnames) <= 3:
            return True

        assert len(colnames) <= 11, colnames

        tr = table.getchildren()[2:monthrange(year, month)[1] + 2]

        for a in tr[::-1]:
            # Each a is a list of table cells, a[i] is the i-th column
            data = {}
            for id, colname in enumerate(colnames):
                try:
                    data[colname] = a.getchildren()[id].text_content()
                except Exception as e:
                    logger.error(e)
                    logger.error(colname)
                    logger.error(id)
                    logger.error(a)
                    raise e

            # Check wind direction is as expected
            if "Wind(km/h)Dir." in data:
                assert set(data["Wind(km/h)Dir."]) <= set("NSEW-CAL"), data["Wind(km/h)Dir."]
            
            # Check percentage is a percentage
            if "Hr.Avg(%)" in data:
                if data["Hr.Avg(%)"] != "-----":
                    assert 0.0 <= float(data["Hr.Avg(%)"]) <= 100.0, data["Hr.Avg(%)"]

            name = self.sep + 'data' + year.__str__() + '-' +\
            "%02d" % month + '-' + data['Date'].split("/")[1] + '.csv'
            self.comb[name]=data

            for key, value in data.items():
                timestamp = year.__str__() + "-%02d-" % month + \
                data['Date'].split("/")[1]
                self.writecsv(key, timestamp , value)

        return True

    def writecsv(self, key, timestamp, val):
        if not key.endswith("."):
            filename = self.location + self.sep + key + ".csv"
        elif key.__contains__("/"):
            newkey = key.split("/")[0] + key.split("/")[1]
            filename = self.location + self.sep + newkey + "csv"
        else:
            filename = self.location + self.sep + key + "csv"

        with open(filename, 'a') as csv_file:
            if any([val == '----', val == 'No data']):
                val = 'NA'
            if val == 'Tr':
                val = 0
            csv_file.write("%s, %s\n" % (timestamp, val))


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    cont = True
    try:
        script, yend, mend, ystart, mstart, stationid = argv
    except:
        format = '(end-year) (end-month) (start-year) (start-month) (stationid)'
        print("usage >>>> python ogimet.py " + format)
        print("example >>>>> python ogimet.py 2019 5 2019 1 97240")
        print(" WARNING!!!!: DO NOT OPEN THE FILE WHILE DOWNLOADED!!!!")
        cont = False
    if cont:
        D = Downloader()
        #D.running_all(2019, 5, start_year=2019, start_month=1,\
        #stationid="97240"
        D.running_all(int(yend), int(mend), int(ystart), int(mstart), stationid)
        print("Enjoy you data :) ")
