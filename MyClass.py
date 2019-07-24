import pandas as pd
import tempfile
import json
import datetime
import sys
import os

class MyClass:
    fileDir='/tmp'
    def __init__(self,file=""):
        if not file:
            self.file=tempfile.NamedTemporaryFile(suffix='.csv',dir=self.fileDir,delete=False).name
            print(self.file)
            print('No Input File Specified Creating Own, Please use '+self.file+' file for read')
        else:
            self.file=file

    def _create(self,key,value,_tot=0):
        print(key,value,_tot)
        key=key.upper()[0:32]
        try:
            size=os.stat(self.file).st_size
            if size >(1024*1024*1024):
                print('Data File Exceeds the Limit... Please choose New ..'+ self.file)
                return None
            else:
                print('Data File size is ok ..'+str(size))
        except (pd.errors.EmptyDataError, FileNotFoundError):
            pass
        except PermissionError:
            print('Permission Denied')
            return
            
        try :
            df=pd.read_csv(self.file,sep=',' ,names=['key','value','_cts','_tot'],index_col=0,header=None)
        except (pd.errors.EmptyDataError, FileNotFoundError):
            df=pd.DataFrame(columns=['key','value','_cts','_tot'])
        except PermissionError:
            print('Permission Denied')
            return
        if key not in list(df.index):
            df=pd.DataFrame([(key,json.dumps(value),datetime.datetime.now().strftime("%s"),_tot)],columns=['key','value','_cts','_tot'])
            try: 
                df.to_csv(self.file, mode='a',index=None,header=None)
                print('Created key Value Pair for .....'+key)
            except PermissionError:
                print('Permission Denied')
        else:
            print('Key Already Exists  ....'+key)


    def _read(self,key):
        key=key.upper()[0:32]
        try:
            df=pd.read_csv(self.file,sep=',' ,names=['key','value','_cts','_tot'],index_col=0,header=None)
        except FileNotFoundError:
            print('File Not Found')
            return None
        if key in list(df.index):
            if (df.loc[key]['_tot']+df.loc[key]['_cts']>=int(datetime.datetime.now().strftime("%s")) or df.loc[key]['_tot']==0):
                print('Value stored '+df.loc[key]['value'])
                return json.loads(df.loc[key]['value'])
            else:
                print('Value stored is Expired',df.loc[key]['_tot']+df.loc[key]['_cts'],int(datetime.datetime.now().strftime("%s")))
        else:
            print('Key Not Found for Reading ....'+key)

    def _delete(self,key):
        key=key.upper()[0:32]
        try:
            df=pd.read_csv(self.file,sep=',' ,names=['key','value','_cts','_tot'],index_col=0,header=None)
        except FileNotFoundError:
            print('File Not Found')
            return None
        if key in list(df.index):
            if (df.loc[key]['_tot']+df.loc[key]['_cts']>=int(datetime.datetime.now().strftime("%s"))or df.loc[key]['_tot']==0):
                df=df.loc[set(df.index) - set([key])]
                df.to_csv(self.file, mode='w',header=None)
                print('Deleted Key ...'+key)
            else:
                print('Value stored is Expired',df.loc[key]['_tot']+df.loc[key]['_cts'],int(datetime.datetime.now().strftime("%s")))
        else:
            print('Key Not Found for Deletion .....'+key)

