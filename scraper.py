import lxml.html
import requests
import pandas as pd
from dateutil import parser
import openpyxl
import csv
from io import BytesIO
from tempfile import NamedTemporaryFile

overview_url = "https://www.stjornarradid.is/gogn/malaskrar-raduneyta/"


class IcelandicDateParserInfo(parser.parserinfo):
    def __init__(self):
        self.WEEKDAYS = [
            (u"Mán", u"Mánudagur"),
            (u"Þri", u"Þriðjudagur"),
            (u"Mið", u"Miðvikudagur"),
            (u"Fim", u"Fimmtudagur"),
            (u"Fös", u"Föstudagur"),
            (u"Lau", u"Laugardagur"),
            (u"Sun", u"Sunnudagur"),
        ]
        self.MONTHS = [
            (u"Jan", u"janúar"),
            (u"Feb", u"febrúar"),
            (u"Mar", u"mars"),
            (u"Apr", u"apríl"),
            (u"May", u"maí"),
            (u"jún", u"júní"),
            (u"júl", u"júlí"),
            (u"ágú", u"ágúst"),
            (u"sept", u"september"),
            (u"okt", u"október"),
            # (u"nov", u"nóvember"),
            (u"nóv", u"nóvember"),
            (u"des", u"desember"),
        ]
        parser.parserinfo.__init__(self)

    def __call__(self):
        """dateutil calls the parserinfo to instantiate it"""
        return self


def find_xlsx_files(url):
    r = requests.get(url)
    root = lxml.html.fromstring(r.text)
    lis = root.xpath('//div[@class="l-user-content"]/ul/li')
    lis = [x.text_content().strip() for x in lis]
    hrefs = root.xpath('//div[@class="l-user-content"]/ul/li//a')
    hrefs = ["https://www.stjornarradid.is" + x.attrib["href"] for x in hrefs]
    urls = {k: v for (k, v) in zip(lis, hrefs)}
    if urls:
        print("Found {} urls".format(len(urls)))
    else:
        print("Found no urls")
    return urls


def replace_newlines(value):
    value = openpyxl.utils.escape.unescape(value).replace("\n", " ").replace("\r", " ")
    value = " ".join(value.split())
    return value


def parse_xlsx(ministry, url):
    r = requests.get(url)
    all_dfs = []
    with NamedTemporaryFile() as tmp:
        wb = openpyxl.load_workbook(BytesIO(r.content))
        for sheet in wb.worksheets:
            print("Parsing sheet: {}".format(sheet))
            for col in sheet["A"]:
                if col.value == "Málsnúmer":
                    header_row = int(col.row) - 1
                if col.value:
                    col.value = replace_newlines(col.value)
            for col in sheet["B"]:
                if col.value:
                    col.value = replace_newlines(col.value)
            sheetname = openpyxl.utils.escape.unescape(sheet.title).strip()
            try:
                sheetdate = parser.parse(
                    sheetname, parserinfo=IcelandicDateParserInfo()
                )
            except parser.ParserError as e:
                print("ERRRROR")
                print(e)
            wb.save(tmp.name)
            tmp.seek(0)
            stream = tmp.read()
            df = pd.read_excel(
                BytesIO(stream),
                header=header_row,
                sheet_name=sheet.title,
                index_col=[0],
                usecols="A:B",
            )
            year = sheetdate.strftime("%Y")
            month = sheetdate.strftime("%m")
            df = df.assign(ár=year)
            df = df.assign(mánuður=month)
            df = df.assign(ráðuneyti=ministry)
            # df["Efni"] = df["Efni"].apply(openpyxl.utils.escape.unescape)
            all_dfs.append(df)
    dfs = pd.concat(all_dfs)
    return dfs


xlsx_urls = find_xlsx_files(overview_url)


all_dfs = []
for ministry, url in xlsx_urls.items():
    print("Parsing {}".format(ministry))
    dfs = parse_xlsx(ministry, url)
    all_dfs.append(dfs)
df = pd.concat(all_dfs)
df.to_csv("data.csv", sep=";", quoting=csv.QUOTE_ALL)
