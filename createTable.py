from asyncore import read
import sqlite3 as sql
from datetime import datetime
import json
import sys
import os
def returnDatetime() -> str:
    current_time = datetime.utcnow()
    str_current_time = str(current_time.date()).replace('-','') + str(current_time.hour) + str(current_time.minute) + str(current_time.second)
    return str_current_time


class createTable:
    # default database configs
    #__database_name__ = "dataregex.db"
    # _table_prefix = "data"

    def __init__(self) -> None:
        self.jsonFile,self.database_name,self.table_name = self.argumetnController()
        self.setConn()
        self.createUniqueTable()
        self.readJSON()
        # self.writeData()
        self.killConn()

    def setConn(self):
        # This method sets the connection through sqlite database
        try:
            self.con = sql.connect(self.database_name)
            cur = self.con.cursor()
            print("Database is connected")

            version_query = "select sqlite_version()"
            cur.execute(version_query)
            vers = cur.fetchall()
            print("sqllite database version is", vers)
            cur.close()

        except sql.Error as error:
            print("Error has been occurd", error)
    
    def createUniqueTable(self):
        # This method create table with name "data" + "YYYYGGAAHHMMSS"
        create_tabel_sql =  f"""
                                CREATE TABLE IF NOT EXISTS {self.table_name}
                                (
                                    email VARCHAR(50),
                                    username VARCHAR(50),
                                    isimsoyisim VARCHAR(50),
                                    emailuserlk  BOOLEAN,
                                    usernamelk BOOLEAN,
                                    dogumyil INT,
                                    dogumay INT,
                                    dogumgun INT,
                                    ulke VARCHAR(25),
                                    ap BOOLEAN DEFAULT 1
                                ) 
                            """
        cur = self.con.cursor()
        cur.execute(create_tabel_sql)
        cur.fetchall()
        cur.close()

    def readJSON(self):
        # This method reads the JSON file and writes into table
        """table_columns = [        'email',
                                    'username',
                                    'isimsoyisim',
                                    'emailuserlk' ,
                                    'usernamelk',
                                    'dogumyil',
                                    'dogumay' ,
                                    'dogumgun' ,
                                    'ulke',
                                    'ap' 
                    ]"""

        # Reading the file
        try:
            with open(self.jsonFile,'r') as f:
                data = json.loads(f.read())
        except FileExistsError as e:
            print("File not exists", e)


        # appeding the json datas into list as tuples 
        json_data_list = []
        for x in data:    
            temp_data =    (
                x['email'],
                x['username'],
                x['profile']['name'],
                self.checkLK(x['email'],x['profile']['name'].split(' ')[0]),
                self.checkLK(x['username'],x['profile']['name'].split(' ')[0]),
                int(x['profile']['dob'].split('-')[0]),
                int(x['profile']['dob'].split('-')[1]),
                int(x['profile']['dob'].split('-')[2]),
                x['profile']['address'].split(',')[-1]
                )
            json_data_list.append(temp_data)
        
        # inserting to the table
        self.writeData(json_data_list)

    def writeData(self,data):
        # print(data)

        # set the cursor for writing
        cur = self.con.cursor()
        #Sql statement for writing
        sql_statement = f"""INSERT INTO {self.table_name} (
                                    email,
                                    username,
                                    isimsoyisim,
                                    emailuserlk ,
                                    usernamelk,
                                    dogumyil,
                                    dogumay ,
                                    dogumgun ,
                                    ulke
                                    ) 
                                    values(?,?,?,?,?,?,?,?,?)"""
        
        #This method helps to write whole data once at a time                                    
        cur.executemany(sql_statement, data)
        # need to commit the changes because this operation is writing
        self.con.commit()
        cur.close()

    def checkLK(self, email, name):
        # Knuth-Morris algorithm helps to find substrings of name in email or username
        res=KMPSearch(name.lower(), email)
        return res

    def argumetnController(self):
        """
        This method parse the arguments and returns the paths and table name
        --db: path of the database
        --file: path of the json file
        """
        args = sys.argv[1:]
        # databasepath = './dataregex.db'
        table_name = "data"+returnDatetime()
        # json_file_path = './dataregex.json'
        if args:
            print(len(args), args)
            for arg in args:                    
                if arg == '--db' and not(args[args.index('--db')+1].startswith('-')):
                    databasepath = args[args.index('--db')+1]
                elif arg == '--file' and os.path.exists(args[args.index('--file')+1]) and not(args[args.index('--db')+1].startswith('-')):
                        json_file_path = args[args.index('--file')+1].split('/')[-1]
            return json_file_path, databasepath, table_name
        else:
            sys.exit("Please enter the parameters --db and --file and try again later")
        
   
    def killConn(self):
        self.con.close()



# Python program for KMP Algorithm
def KMPSearch(pat, txt):
    M = len(pat)
    N = len(txt)
 
    # create lps[] that will hold the longest prefix suffix
    # values for pattern
    lps = [0]*M
    j = 0 # index for pat[]
 
    # Preprocess the pattern (calculate lps[] array)
    computeLPSArray(pat, M, lps)
 
    i = 0 # index for txt[]
    flag = 0
    while i < N:
        if pat[j] == txt[i]:
            i += 1
            j += 1
 
        if j == M:
            # print ("Found pattern at index", str(i-j))
            flag=1
            j = lps[j-1]
 
        # mismatch after j matches
        elif i < N and pat[j] != txt[i]:
            # Do not match lps[0..lps[j-1]] characters,
            # they will match anyway
            if j != 0:
                j = lps[j-1]
            else:
                i += 1
    return flag
 
def computeLPSArray(pat, M, lps):
    len = 0 # length of the previous longest prefix suffix
 
    lps[0] # lps[0] is always 0
    i = 1
 
    # the loop calculates lps[i] for i = 1 to M-1
    while i < M:
        if pat[i]== pat[len]:
            len += 1
            lps[i] = len
            i += 1
        else:
            # This is tricky. Consider the example.
            # AAACAAAA and i = 7. The idea is similar
            # to search step.
            if len != 0:
                len = lps[len-1]
 
                # Also, note that we do not increment i here
            else:
                lps[i] = 0
                i += 1
      


createTable()        





