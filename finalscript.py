import pandas as pd
import datetime
import warnings
warnings.simplefilter("ignore")
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import sys
import argparse
import csv

class Weekly:
    def __init__(self,data,datecol,pricecol,week1,week2):
        self.rawdata = data
        self.datecol = datecol
        self.pricecol = pricecol
        self.week1 = week1
        self.week2 = week2
        if self.headercheck() == True:
            self.data = pd.read_csv(f"{data}")
            self.data.columns = [self.datecol,self.pricecol]     
        else:
            self.data = pd.read_csv(f"{data}",header=None)
            self.data.columns = [self.datecol,self.pricecol] 

    def headercheck(self):
        sniffer = csv.Sniffer()
        sample_bytes = 1024
        header = sniffer.has_header(open(f"{self.rawdata}").read(sample_bytes))
        return header
    
    def data_ingestion(self,data):
        data[self.datecol] = pd.to_datetime(data[self.datecol])
        dx = pd.date_range(start=min(data[self.datecol]),end=max(data[self.datecol]))
        data = data.groupby(by=self.datecol)[self.pricecol].mean().reset_index()
        data.set_index(self.datecol,inplace=True)
        data.index = pd.DatetimeIndex(data.index)
        data = data.reindex(dx, fill_value=0).reset_index()
        data = data.rename(columns={"index":self.datecol})
        data["Day_of_week"] = pd.DatetimeIndex(data[self.datecol]).dayofweek
        data["Week"] = pd.DatetimeIndex(data[self.datecol]).week
        data["Day_of_week"].replace({0: "Mon", 1: "Tue", 2: "Wed", 3: "Thrus", 4: "Fri", 5: "Sat", 6: "Sun"}, inplace=True)
        return data

    def daycor(self,i):
        df = self.data_ingestion(self.data)
        df2 = df[df['Week']==self.week1]
        df3 = df[df['Week']==self.week2]
        x = df3[["Day_of_week", self.pricecol]]
        l = x[x["Day_of_week"] == i]
        l2 = []
        list1 = ["Mon", "Tue", "Wed", "Thrus", "Fri", "Sat", "Sun"]

        for i in list1:
            a = list(df2[df2["Day_of_week"] == i][self.pricecol])
            l2.append(a)

        dm = pd.DataFrame(l2).T
        dm.columns = list1
        dm.dropna(inplace=True)
        dm["Corr_value"] = l.reset_index()[self.pricecol]
        corr_table = pd.Series(dm.corr(method="spearman")["Corr_value"]).reset_index()
        return corr_table["Corr_value"]
    
    def final_out(self):
        list1 = ["Mon", "Tue", "Wed", "Thrus", "Fri", "Sat", "Sun"]
        a = pd.DataFrame()
        for i in list1:
            a[i] = None
            a[i] = pd.DataFrame(self.daycor(i)).rename_axis(str(i), axis=1)["Corr_value"]
        a = a.iloc[:7, :]
        a["Past_Day"] = list1
        return a.set_index("Past_Day")
    
class Monthly:
    def __init__(self,data,datecol,pricecol,month1,month2):
        self.rawdata = data
        self.datecol = datecol
        self.pricecol = pricecol
        self.month1 = month1
        self.month2 = month2
        if self.headercheck() == True:
            self.data = pd.read_csv(f"{data}")
            self.data.columns = [self.datecol,self.pricecol] 
        else:
            self.data = pd.read_csv(f"{data}",header=None) 
            self.data.columns = [self.datecol,self.pricecol] 
        
    def headercheck(self):
        sniffer = csv.Sniffer()
        sample_bytes = 1024
        header = sniffer.has_header(open(f"{self.rawdata}").read(sample_bytes))
        return header
       
    def data_ingestion(self,data):
        data[self.datecol] = pd.to_datetime(data[self.datecol])
        dx = pd.date_range(start=min(data[self.datecol]),end=max(data[self.datecol]))
        data = pd.DataFrame(data.groupby(by=self.datecol)[self.pricecol].mean().reset_index())
        data.set_index(self.datecol,inplace=True)
        data.index = pd.DatetimeIndex(data.index)
        data = data.reindex(dx, fill_value=0).reset_index()
        data = data.rename(columns={"index":self.datecol})
        data["Day"] = pd.DatetimeIndex(data[self.datecol]).day
        data["Month"] = pd.DatetimeIndex(data[self.datecol]).month
        return data
    
    def daycor(self,i):
        df = self.data_ingestion(self.data)
        df2 = df[df['Month']==self.month1]
        df3 = df[df['Month']==self.month2]
        x = df3[["Day",self.pricecol]]
        l = x[x["Day"] == i]
        l2 = []
        list1 = [i for i in range(1,31)]

        for i in list1:
            a = list(df2[df2["Day"] == i][self.pricecol])
            l2.append(a)

        dm = pd.DataFrame(l2).T
        dm.columns = list1
        dm.dropna(inplace=True)
        dm["Corr_value"] = l.reset_index()[self.pricecol]
        corr_table = pd.Series(dm.corr(method="spearman")["Corr_value"]).reset_index()
        return corr_table["Corr_value"]
    
    def final_out(self):
        list1 = [i for i in range(1,31)]
        a = pd.DataFrame()
        for i in list1:
            a[i] = None
            a[i] = pd.DataFrame(self.daycor(i)).rename_axis(str(i), axis=1)["Corr_value"]
        a = a.iloc[:30, :]
        a["Past_Day"] = list1
        return a.set_index("Past_Day")

class Yearly:
    def __init__(self,data,datecol,pricecol,year1,year2):
        self.datecol = datecol
        self.pricecol = pricecol
        self.year1 = year1
        self.year2 = year2 
        self.rawdata = data
        if self.headercheck() == True:
            self.data = pd.read_csv(f"{data}")
            self.data.columns = [self.datecol,self.pricecol] 
        else:
            self.data = pd.read_csv(f"{data}",header=None) 
            self.data.columns = [self.datecol,self.pricecol] 

    def headercheck(self):
        sniffer = csv.Sniffer()
        sample_bytes = 1024
        header = sniffer.has_header(open(f"{self.rawdata}").read(sample_bytes))
        return header
       
    def data_ingestion(self,data):
        data[self.datecol] = pd.to_datetime(data[self.datecol])
        dx = pd.date_range(start=min(data[self.datecol]),end=max(data[self.datecol]))
        data = pd.DataFrame(data.groupby(by=self.datecol)[self.pricecol].mean().reset_index())
        data.set_index(self.datecol,inplace=True)
        data.index = pd.DatetimeIndex(data.index)
        data = data.reindex(dx, fill_value=0).reset_index()
        data = data.rename(columns={"index":self.datecol})
        data["day_of_year"] = pd.DatetimeIndex(data[self.datecol]).dayofyear
        data["year"] = pd.DatetimeIndex(data[self.datecol]).year
        return data
    
    def daycor(self,i):
        df = self.data_ingestion(self.data)
        df2 = df[df['year']==self.year1]
        df3 = df[df['year']==self.year2]
        x = df3[["day_of_year",self.pricecol]]
        l = x[x["day_of_year"] == i]
        l2 = []
        list1 = [i for i in range(1,366)]

        for i in list1:
            a = list(df2[df2["day_of_year"] == i][self.pricecol])
            l2.append(a)

        dm = pd.DataFrame(l2).T
        dm.columns = list1
        dm.dropna(inplace=True)
        dm["Corr_value"] = l.reset_index()[self.pricecol]
        corr_table = pd.Series(dm.corr(method="spearman")["Corr_value"]).reset_index()
        return corr_table["Corr_value"]
    
    def final_out(self):
        list1 = [i for i in range(1,366)]
        a = pd.DataFrame()
        for i in list1:
            a[i] = None
            a[i] = pd.DataFrame(self.daycor(i,year1,year2)).rename_axis(str(i), axis=1)["Corr_value"]
        a = a.iloc[:365, :]
        a["Past_Day"] = list1
        return a.set_index("Past_Day")



class Runner:
    def __init__(self,data,typeofcorr,firstval,secondval,datecol,pricecol):
        if typeofcorr == "weekly":
            corr = Weekly(data,datecol,pricecol,firstval,secondval)
            a = corr.final_out()
            finaldf = a.replace(np.NaN,0)
            print(finaldf)
            finaldf.to_excel(f"{typeofcorr}_corr_result.xlsx")
            fig = plt.figure(figsize=(14,8))
            fig = sns.heatmap(finaldf,cmap="RdPu",annot=True,linewidth=0.2,annot=True,fmt='.2g')
            plt.savefig(f"{typeofcorr}_correlation_heatmap.jpg")
            
        elif typeofcorr == "monthly":
            corr = Monthly(data,datecol,pricecol,firstval,secondval)
            a = corr.final_out()
            finaldf = a.replace(np.NaN,0)
            print(finaldf)
            finaldf.to_excel(f"{typeofcorr}_corr_result.xlsx")
            fig = plt.figure(figsize=(20,14))
            fig = sns.heatmap(finaldf,cmap="RdPu",linewidth=0.2,annot=True,fmt='.2g')
            plt.savefig(f"{typeofcorr}_correlation_heatmap.jpg")

        elif typeofcorr =="yearly":
            corr = Yearly(data,datecol,pricecol,firstval,secondval)
            a = corr.final_out()
            finaldf = a.replace(np.NaN,0)
            print(finaldf)
            finaldf.to_excel(f"{typeofcorr}_corr_result.xlsx")
            fig = plt.figure(figsize=(20,14))
            fig = sns.heatmap(finaldf,cmap="RdPu",linewidth=0.2,annot=True,fmt='.2g')
            plt.savefig(f"{typeofcorr}_correlation_heatmap.jpg")


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("typeofcorr",help="choose yearly/monthly/yearly",type=str)
    parser.add_argument("firstval",help="first week/month number",type=int)
    parser.add_argument("secondval",help="second week/month number",type=int)
    parser.add_argument("--datecol",help="enter name of data column",type=str,default='date')
    parser.add_argument("--pricecol",help="enter name of price column",type=str,default='price')
    parser.add_argument("data",help="enter csv file name",type=str)
    args = parser.parse_args()

    typeofcorr = args.typeofcorr
    firstval = args.firstval
    secondval = args.secondval
    datecol = args.datecol
    pricecol = args.pricecol
    data = args.data

    runner = Runner(data,typeofcorr,firstval,secondval,datecol,pricecol)
