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
import re
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE

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


replacements = {"Nov ": "nóv ", "IRN ": "", "SRN - ": ""}


def replace_bogus_values(value):
    for k, v in replacements.items():
        value = value.replace(k, v)
    return value


def find_xlsx_files(url):
    r = requests.get(url)
    urls = []
    root = lxml.html.fromstring(r.text)
    hrefs = root.xpath("//div[contains(@class,'column')]//a")
    for href in hrefs:
        if "xlsx" in href.attrib["href"]:
            year = href.xpath("./preceding::h2")
            selected_year = year[-1:][0].text
            url_item = {}
            ministry = href.text_content().strip()
            url = "https://www.stjornarradid.is" + href.attrib["href"]
            if ministry:
                url_item["ministry"] = ministry
                url_item["url"] = url
                url_item["year"] = selected_year.strip()
                urls.append(url_item)
    if urls:
        print("Found {} urls".format(len(urls)))
        for url in urls:
            print(url)
    else:
        print("Found no urls")
    return urls


def replace_newlines(value):
    value = openpyxl.utils.escape.unescape(value).replace("\n", " ").replace("\r", " ")
    value = " ".join(value.split())
    value = ILLEGAL_CHARACTERS_RE.sub(r"", value)
    return value


def parse_xlsx(ministry, url, year):
    try:
        r = requests.get(url)
        r.raise_for_status()
    except requests.HTTPError as exc:
        print("Error: {}".format(exc.response.status_code))
        return None
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

            wb.save(tmp.name)
            tmp.seek(0)
            stream = tmp.read()
            df = pd.read_excel(
                BytesIO(stream),
                header=header_row,
                sheet_name=sheet.title,
                usecols="A:B",
                names=["Málsnúmer", "Efni"],
            )
            try:
                df["Ár"] = "20" + df.Málsnúmer.str.extract("(\d\d)", expand=True)
            except AttributeError:
                continue
            try:
                df["Mánuður"] = df.Málsnúmer.str.extract("\d\d(\d\d)", expand=True)
            except AttributeError:
                continue
            df = df.assign(Ráðuneyti=ministry)
            all_dfs.append(df)
    dfs = pd.concat(all_dfs)

    return dfs


if __name__ == "__main__":
    xlsx_urls = find_xlsx_files(overview_url)

    all_dfs = []
    for item in xlsx_urls:
        print(
            "Parsing {} from url {} for year {}".format(
                item["ministry"], item["url"], item["year"]
            )
        )
        dfs = parse_xlsx(item["ministry"], item["url"], item["year"])
        all_dfs.append(dfs)
    df = pd.concat(all_dfs)
    df.drop_duplicates(subset=["Málsnúmer"], keep="first", inplace=True)
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
    table.upsert_many(dicts, ["Málsnúmer"])
