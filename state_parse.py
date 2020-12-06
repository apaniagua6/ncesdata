import zipfile
import pandas as pd
from io import BytesIO
import glob
from collections import defaultdict


#


def read_state_2010_2014():
    # needs to be between 2010 and 2011

    file_2010_2011 = "st101a_txt.zip"
    file_2011_2012 = "st111a_txt.zip"
    file_2012_2013 = "st121a_imp_txt.zip"
    file_2013_2014 = "st131a_imp_txt.zip"

    data_files_1 = [file_2010_2011, file_2011_2012, file_2012_2013, file_2013_2014]

    # Read all files that have the following format "*imp_txt.zip"
    mydataframes = []

    for filename in data_files_1:
        data_filename = 'ncesdata/state/{0}'.format(filename)
        archive = zipfile.ZipFile(data_filename, 'r')

        for filename in archive.filelist:
            # read bytes from archive
            mytext_file = archive.read(filename.filename)
            mypd = pd.read_csv(BytesIO(mytext_file), sep='\t')
            mydataframes.append(mypd)

    combined_df = pd.concat(mydataframes, sort=False)
    combined_df.to_csv('./state_combined_df_2013_2014.csv', index=False, header=True)


def read_state_2014_2016():
    # Read the state between 2014_2016

    # https://nces.ed.gov/ccd/data/zip/ccd_sea_029_1718_w_1a_083118.zip

    #  "ccd_sea_029_1415_w_0216161a_txt.zip"
    directory_list = ["ccd_sea_029_1415_w_0216161a_txt.zip",
                      "ccd_sea_029_1516_w_1a_011717_csv.zip",
                      ]

    membership_list = ["ccd_sea_052_1415_w_0216161a_txt.zip",
                       "ccd_sea_052_1516_w_1a_011717_csv.zip",
                       ]

    staff_list = ["ccd_sea_059_1415_w_0216161a_txt.zip",
                  "ccd_sea_059_1516_w_1a_011717_csv.zip",
                  ]

    tab_seperated_files = ["ccd_sea_059_1415_w_0216161a_txt.zip", "ccd_sea_052_1415_w_0216161a_txt.zip",
                           "ccd_sea_029_1415_w_0216161a_txt.zip"]

    data_files = {"directory": directory_list, "membership": membership_list, "staff": staff_list}

    mydataframes = defaultdict(list)

    for data_type, data_file_name_list in data_files.items():

        for filename in data_file_name_list:
            # print('reading Zip: ', filename)
            data_filename = 'ncesdata/state/{0}'.format(filename)
            archive = zipfile.ZipFile(data_filename, 'r')

            for myfilename in archive.filelist:
                # print('reading file: ', myfilename)
                if myfilename.filename.endswith("txt") or myfilename.filename.endswith("csv"):
                    # read bytes from archive
                    mytext_file = archive.read(myfilename.filename)

                    if filename in tab_seperated_files:
                        # print(myfilename)
                        mypd = pd.read_csv(BytesIO(mytext_file), sep='\t')
                    else:
                        mypd = pd.read_csv(BytesIO(mytext_file), sep=',')

                    mydataframes[data_type].append(mypd)

    directory_df = pd.concat(mydataframes["directory"], sort=False)
    membership_df = pd.concat(mydataframes["membership"], sort=False)
    staff_df = pd.concat(mydataframes["staff"], sort=False)

    directory_df.to_csv('./state_directory_df_2014_2016.csv', index=False, header=True)
    membership_df.to_csv('./state_membership_df_2014_2016.csv', index=False, header=True)
    staff_df.to_csv('./state_staff_df_2014_2016.csv', index=False, header=True)


def read_state_2017_2019():
    # Read the state between 2014 - 2019

    # https://nces.ed.gov/ccd/data/zip/ccd_sea_029_1718_w_1a_083118.zip

    #  "ccd_sea_029_1415_w_0216161a_txt.zip"
    directory_list = [
        "ccd_sea_029_1718_w_1a_083118.zip",
        "ccd_sea_029_1819_l_1a_091019.zip"]

    membership_list = [
        "ccd_sea_052_1718_l_1a_083118.zip",
        "ccd_sea_052_1819_l_1a_091019.zip"]

    staff_list = [
        "ccd_sea_059_1718_l_1a_083118.zip",
        "ccd_sea_059_1819_l_1a_091019.zip"]

    tab_seperated_files = ["ccd_sea_059_1415_w_0216161a_txt.zip", "ccd_sea_052_1415_w_0216161a_txt.zip",
                           "ccd_sea_029_1415_w_0216161a_txt.zip"]

    data_files = {"directory": directory_list, "membership": membership_list, "staff": staff_list}

    mydataframes = defaultdict(list)

    for data_type, data_file_name_list in data_files.items():

        for filename in data_file_name_list:
            # print('reading Zip: ', filename)
            data_filename = 'ncesdata/state/{0}'.format(filename)
            archive = zipfile.ZipFile(data_filename, 'r')

            for myfilename in archive.filelist:
                # print('reading file: ', myfilename)
                if myfilename.filename.endswith("txt") or myfilename.filename.endswith("csv"):
                    # read bytes from archive
                    mytext_file = archive.read(myfilename.filename)

                    if filename in tab_seperated_files:
                        # print(myfilename)
                        mypd = pd.read_csv(BytesIO(mytext_file), sep='\t')
                    else:
                        mypd = pd.read_csv(BytesIO(mytext_file), sep=',')

                    mydataframes[data_type].append(mypd)

    #directory_df = pd.concat(mydataframes["directory"], sort=False)
    #membership_df = pd.concat(mydataframes["membership"], sort=False)
    #staff_df = pd.concat(mydataframes["staff"], sort=False)
    for dir_name in mydataframes.keys():
        for index, data_frame in enumerate(mydataframes[dir_name]):
            unique_name = './state_directory_df_{0}.csv'.format(index)
            data_frame.to_csv(unique_name, index=False, header=True)


#read_state_2010_2014()

# Years 2014-2015, 2015-2016, 2016-2017, 2017-2018, 2018-2019
read_state_2014_2016()
read_state_2017_2019()


