import re, psycopg2
from lxml import etree

class XML2Database:
    connectstring = "host='localhost' dbname='postgis_geonode' user='user' password=''"
    tablename = "un-data-it-internet"
    
    cursor = None
    connection = None
    
    def __init__(self, inputTree):
        self.connection = psycopg2.connect(self.connectstring)
        
        self.fillDatabase(inputTree)
        self.connection.commit()
        
    def fillDatabase(self, inputTree):
        for record in inputTree.iter("record"):
            
            self.cursor = self.connection.cursor()
            gu_a3 = ""
            un_name_en = ""
            un_data_year = ""
            un_data_value = ""
            
            for field in record.iter():
                if(field.get("name") == "GU_A3"):
                    gu_a3 = str(field.text)
                if(field.get("name") == "Country or Area"):
                    un_name_en = str(re.sub("[ \n\t]+", " ",field.text))
                if(field.get("name") == "Year"):
                    un_data_year = str(field.text)
                if(field.get("name") == "Value"):
                    un_data_value = str(field.text)
            
            self.insert(gu_a3, un_name_en, un_data_year, un_data_value)
            
    def insert(self, gu_a3, un_name_en, un_data_year, un_data_value):
        qs = """INSERT INTO \"{0}\" VALUES (%s, %s, %s, %s, %s, %s);""".format(self.tablename)
        self.cursor.execute(qs, (gu_a3, "it-internet", un_name_en, un_data_year, un_data_value, "%", ))
        self.cursor.close()

class Xml2PostgisData:
    def __init__(self, filename):
        with open(filename) as f:
            dbInput = self.parseXML(f)
            XML2Database(dbInput)
        
    def parseXML(self, f):
        return etree.parse(f)
        
[filename] = sys.argv[1:]

Xml2PostgisData(filename)

