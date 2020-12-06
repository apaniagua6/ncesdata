# CCD Data Backup

Created a backup for the csv data sets that are hosted at nces.ed.gov 

Information on the Common Core of Data (CCD)
The primary purpose of the CCD is to provide basic information on public elementary and secondary schools, local education agencies (LEAs), and state education agencies (SEAs) for each state, the District of Columbia, and the outlying territories with a U.S. relationship.



# Methods

Root url: 

https://nces.ed.gov/ccd/files.asp#Fiscal:2,LevelId:2,SchoolYearId:33,Page:1

Gets the CSV files from:
NonFiscal / {State, District, School} / {School-Year}

# Pipeline
1. download data set
