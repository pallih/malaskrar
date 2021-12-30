import lxml.html
import requests
import pandas as pd
from dateutil import parser
import openpyxl
import csv
from io import BytesIO
from tempfile import NamedTemporaryFile
import pathlib
import dataset

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
            (u"nóv", u"nóvember"),
            (u"des", u"desember"),
        ]
        parser.parserinfo.__init__(self)

    def __call__(self):
        """dateutil calls the parserinfo to instantiate it"""
        return self


replacements = {"Nov ": "nóv "}


def replace_bogus_values(value):
    for k, v in replacements.items():
        value = value.replace(k, v)
    return value


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
            sheetname = replace_bogus_values(sheetname)
            try:
                sheetdate = parser.parse(
                    sheetname, parserinfo=IcelandicDateParserInfo()
                )
                print(
                    "   - Parsed month: {}, year: {}".format(
                        sheetdate.strftime("%m"), sheetdate.strftime("%Y")
                    )
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
                usecols="A:B",
            )
            year = sheetdate.strftime("%Y")
            month = sheetdate.strftime("%m")
            df = df.assign(Ár=year)
            df = df.assign(Mánuður=month)
            df = df.assign(Ráðuneyti=ministry)
            all_dfs.append(df)
    dfs = pd.concat(all_dfs)
    return dfs


if __name__ == "__main__":
    xlsx_urls = find_xlsx_files(overview_url)
    all_dfs = []
    for ministry, url in xlsx_urls.items():
        print("Parsing {}".format(ministry))
        dfs = parse_xlsx(ministry, url)
        all_dfs.append(dfs)
    df = pd.concat(all_dfs)
    export_dir = pathlib.Path.cwd() / "data"
    export_dir.mkdir(parents=True, exist_ok=True)
    csv_filename = export_dir / "malaskrar.csv"
    db_filename = export_dir / "malaskrar.db"
    df.to_csv(csv_filename, sep=";", quoting=csv.QUOTE_ALL, index=False)
    dicts = df.to_dict(orient="records")
    db = dataset.connect("sqlite:///" + db_filename.as_posix(), sqlite_wal_mode=False)
    table = db.create_table(
        "malaskrar",
    )
    table.upsert_many(dicts, ["Málsnúmer", "Mánuður", "Ár"])
