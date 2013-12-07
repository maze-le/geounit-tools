import re, json, sys
from lxml import etree
from microfiber import Database

verbose = True

# Format floats
def prettyfloat(fl):
    return "%0.2f" % float(fl)

# Lookup-Table to Resolve GUA3-Code (used in Natural Earth Data Dataset)
# to a Area Name (used in UN: Data Dataset) and vice versa.
# 
#   GUA3 represents the ISO-3166-1 3-character alphabetic code
# 
class AreaLookup:
    areaToGUA3 = {}
    GUA3ToArea = {}
    
    lookupFile = "./GUA3Lookup.xml"
    xmlInput = ""
    
    def __init__(self):
        with open(self.lookupFile) as f:
            self.xmlInput = self.parseXML(f)
        self.buildLookup()
    
    def buildLookup(self):
        for record in self.xmlInput.iter("record"):
            for field in record.iter():
                gua3 = field.get("gua3")
                ctr = field.get("ctr")
                
                self.areaToGUA3[ctr] = gua3
                self.GUA3ToArea[gua3] = ctr
    
    def parseXML(self, f):
        return etree.parse(f)


# Class to put UN-Data XML to couchDB
#  
#  - Reads the whole file to memory. 
#  - Builds a dictionary while parsing
#  - Validates while parsing, if errors occur, exit
#  - Looks up Footnotes
#  - When everything is loaded and validated, iterate through the dictionary and put entries to couchDB
#
class XML2CouchDB:
    
    dbName = ""
    dbEnv  = {
        'url': "http://localhost:5984", 
        'basic': {
            'password': 'secret',
            'username': 'couchAdmin'
        }
    }
    
    lookup = AreaLookup()
    xmlDataInput = ""
    jsonData = {}
    db = None
    dataName = ""
    footnotes = {}
    
    def __init__(self, filename, dbName, dataName):
        self.dbName = dbName
        
        self.dataName = dataName
        
        with open(filename) as f:
            self.xmlDataInput = self.parseXML(f)
        
        self.db = Database(dbName, self.dbEnv)
        self.makeFootnoteIndex()
        
        self.sample()
    
    def parseXML(self, f):
        return etree.parse(f)
    
    def makeFootnoteIndex(self):
        for fn in self.xmlDataInput.iter('footnote'):
            seq = fn.attrib['footnoteSeqID']
            seq = int(seq)
            
            self.footnotes[seq] = fn.text
    
    def sample(self):
        sample = {}
        
        for record in self.xmlDataInput.iter('record'):
            jsonRecord = {}
            
            gua3   = ""
            date   = ""
            val    = ""
            name   = ""
            fnSeq  = -1
            
            for field in record.iter('field'):
                if(field.attrib['name'] == "Country or Area"):
                    name = field.text
                    
                    if name in self.lookup.areaToGUA3:
                        gua3 = self.lookup.areaToGUA3[name]
                    else:
                        print("No GUA3-country code found for: " + field.text)
                        sys.exit(-1)
                        
                
                elif(field.attrib['name'] == "Year"):
                    date = field.text
                    
                    if date == None || date == "":
                        print("No date-code found for: " + name)
                        sys.exit(-1)
                        
                
                elif(field.attrib['name'] == "Value"):
                    val = prettyfloat(field.text)
                    
                elif(field.attrib['name'] == "Value Footnotes"):
                    if(field.text is not None):
                        fnSeq = int(field.text)
            
            if(gua3 != ''):
                common_ID =  "rc" + "-" + gua3 + "-" + date
                if(common_ID not in sample):
                    sample[common_ID] = {
                        'gua3' : gua3,
                        'date' : date
                    }
                    
                sample[common_ID][self.dataName] = {
                    "val":val, 
                    "fn" : self.lookupFootnote(fnSeq)
                }
        
        self.putSampleToCouch(sample)

    def lookupFootnote(self, fnSeq):
        if(fnSeq < 0):
            return "--"
        else:
            if(fnSeq not in self.footnotes):
                return "--"
            else:
                return self.footnotes[fnSeq]
    
    
    def putSampleToCouch(self, sample):
        for record in sample:
            if(verbose):
                print(sample[record])
            self.db.put(sample[record], record)


[filename]     = sys.argv[1:]
[databaseName] = sys.argv[2:]
[datasetName]  = sys.argv[3:]


XML2CouchDB(filename, databaseName, datasetName)

