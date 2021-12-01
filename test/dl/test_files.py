import unittest
import pandas as pd
import io

from src.dl.files import load_file_as_dataframe


class TestDLFiles(unittest.TestCase):
    def test_load_csv_file_as_dataframe(self):
        """
        Test loading a CSV file as a DataFrame
        """
        csv_path = "data/samples/test.csv"
        csv = pd.read_csv(csv_path)
        
        with open(csv_path, "rb") as file:
            data = file.read()
            csv_detect = load_file_as_dataframe(io.BytesIO(data))
            csv_extension = load_file_as_dataframe(io.BytesIO(data), "csv")
        
        self.assertTrue(csv.equals(csv_detect))
        self.assertTrue(csv.equals(csv_extension))


    def test_load_tsv_file_as_dataframe(self):
        """
        Test loading a TSV file as a DataFrame
        """
        tsv_path = "data/samples/test.tsv"
        tsv = pd.read_csv(tsv_path, sep='\t')
        
        with open(tsv_path, "rb") as file:
            data = file.read()
            tsv_detect = load_file_as_dataframe(io.BytesIO(data))
            tsv_extension = load_file_as_dataframe(io.BytesIO(data), "tsv")
        
        self.assertTrue(tsv.equals(tsv_detect))
        self.assertTrue(tsv.equals(tsv_extension))


    def test_load_json_file_as_dataframe(self):
        """
        Test loading a JSON file as a DataFrame
        """
        json_path = "data/samples/test.json"
        json = pd.read_json(json_path)
        
        with open(json_path, "rb") as file:
            data = file.read()
            json_detect = load_file_as_dataframe(io.BytesIO(data))
            json_extension = load_file_as_dataframe(io.BytesIO(data), "json")
        
        self.assertTrue(json.equals(json_detect))
        self.assertTrue(json.equals(json_extension))


    def test_load_xml_file_as_dataframe(self):
        """
        Test loading an XML file as a DataFrame
        """
        xml_path = "data/samples/test.xml"
        xml = pd.read_xml(xml_path)
        
        with open(xml_path, "rb") as file:
            data = file.read()
            xml_detect = load_file_as_dataframe(io.BytesIO(data))
            xml_extension = load_file_as_dataframe(io.BytesIO(data), "xml")
        
        self.assertTrue(xml.equals(xml_detect))
        self.assertTrue(xml.equals(xml_extension))
       

    def test_load_excel_file_as_dataframe(self):
        """
        Test loading an Excel file as a DataFrame
        """
        xls_path = "data/samples/test.xls"
        xls = pd.read_excel(xls_path)

        with open(xls_path, "rb") as file:
            data = file.read()
            xls_detect = load_file_as_dataframe(io.BytesIO(data))
            xls_extension = load_file_as_dataframe(io.BytesIO(data), "xls")
        
        xlsx_path = "data/samples/test.xlsx"
        xlsx = pd.read_excel(xlsx_path)

        with open(xls_path, "rb") as file:
            data = file.read()
            xlsx_detect = load_file_as_dataframe(io.BytesIO(data))
            xlsx_extension = load_file_as_dataframe(io.BytesIO(data), "xls")

        self.assertTrue(xls.equals(xls_detect))
        self.assertTrue(xls.equals(xls_extension))
        self.assertTrue(xls.equals(xlsx))
        self.assertTrue(xls.equals(xlsx_detect))
        self.assertTrue(xls.equals(xlsx_extension))
        

if __name__ == "__main__":
    unittest.main()
