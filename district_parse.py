import zipfile
import pandas as pd
from io import BytesIO
import glob
from collections import defaultdict




def read_district_2016_2019():


    # https://nces.ed.gov/ccd/data/zip/ccd_sea_029_1718_w_1a_083118.zip


    directory_list = [
        "ccd_lea_029_1617_w_1a_11212017_csv.zip",
        "ccd_lea_029_1718_w_1a_083118.zip",
        "ccd_lea_029_1819_l_1a_091019.zip"]

    membership_list = [
        "ccd_lea_052_1617_l_2a_11212017.zip",
        "ccd_lea_052_1718_l_1a_083118.zip",
        "ccd_lea_052_1819_l_1a_091019.zip"]

    staff_list = [
        "ccd_lea_059_1617_l_2a_11212017.zip",
        "ccd_lea_059_1718_l_1a_083118.zip",
        "ccd_lea_059_1819_l_1a_091019.zip"]

    children_disabilities_list = [
        "ccd_lea_2_89_1617_l_2a_11212017.zip",
        "ccd_lea_2_89_1718_l_1a_083118.zip",
        "ccd_lea_2_89_1819_l_1a_091019.zip"]

    english_learners_list = [
        "ccd_lea_141_1617_l_2a_11212017.zip",
        "ccd_lea_141_1718_l_1a_083118.zip",
        "ccd_lea_141_1819_l_1a_091019.zip"]

    tab_seperated_files = ["ccd_lea_029_1415_w_0216161ar_txt.zip", "ccd_lea_052_1415_w_0216161a_txt.zip",
                           "ccd_lea_059_1415_w_0216161a_txt.zip", "ccd_lea_002089_1415_w_0216161a_txt.zip",
                           "ccd_lea_046141_1415_w_0216161a_txt.zip"]

    data_files = {"directory": directory_list, "membership": membership_list,
                  "staff": staff_list, "children_disabilities":children_disabilities_list,
                  "english_learners": english_learners_list}

    mydataframes = defaultdict(list)

    for data_type, data_file_name_list in data_files.items():

        for filename in data_file_name_list:
            # print('reading Zip: ', filename)
            data_filename = 'ncesdata/district/{0}'.format(filename)
            archive = zipfile.ZipFile(data_filename, 'r')

            for myfilename in archive.filelist:
                print('reading file: ', filename)
                print('tab_seperated_files: ', tab_seperated_files)
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
    children_disabilities_df = pd.concat(mydataframes["children_disabilities"], sort=False)
    english_learners_df = pd.concat(mydataframes["english_learners"], sort=False)

    directory_df.to_csv('./district_directory_df_2016_2019.csv', index=False, header=True)
    membership_df.to_csv('./district_membership_df_2016_2019.csv', index=False, header=True)
    staff_df.to_csv('./district_staff_df_2016_2019.csv', index=False, header=True)
    children_disabilities_df.to_csv('./district_children_disabilities_2016_2019.csv', index=False, header=True)
    english_learners_df.to_csv('./district_english_learners_2016_2019.csv', index=False, header=True)


def read_district_2014_2016():
    # https://nces.ed.gov/ccd/data/zip/ccd_sea_029_1718_w_1a_083118.zip

    directory_list = [
        "ccd_lea_029_1415_w_0216161ar_txt.zip",
        "ccd_lea_029_1516_w_1a_011717_csv.zip", ]

    membership_list = [
        "ccd_lea_052_1415_w_0216161a_txt.zip",
        "ccd_lea_052_1516_w_1a_011717_csv.zip", ]

    staff_list = [
        "ccd_lea_059_1415_w_0216161a_txt.zip",
        "CCD_LEA_059_1516_W_1a_011717_csv.zip", ]

    children_disabilities_list = [
        "ccd_lea_002089_1415_w_0216161a_txt.zip",
        "ccd_lea_002089_1516_w_1a_011717_csv.zip", ]

    english_learners_list = [
        "ccd_lea_046141_1415_w_0216161a_txt.zip",
        "ccd_lea_141_1516_w_1a_011717_csv.zip", ]

    tab_seperated_files = ["ccd_lea_029_1415_w_0216161ar_txt.zip", "ccd_lea_052_1415_w_0216161a_txt.zip",
                           "ccd_lea_059_1415_w_0216161a_txt.zip", "ccd_lea_002089_1415_w_0216161a_txt.zip",
                           "ccd_lea_046141_1415_w_0216161a_txt.zip"]

    data_files = {"directory": directory_list, "membership": membership_list,
                  "staff": staff_list, "children_disabilities": children_disabilities_list,
                  "english_learners": english_learners_list}

    mydataframes = defaultdict(list)

    for data_type, data_file_name_list in data_files.items():

        for filename in data_file_name_list:
            # print('reading Zip: ', filename)
            data_filename = 'ncesdata/district/{0}'.format(filename)
            archive = zipfile.ZipFile(data_filename, 'r')

            for myfilename in archive.filelist:
                print('reading file: ', filename)
                print('tab_seperated_files: ', tab_seperated_files)
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
    children_disabilities_df = pd.concat(mydataframes["children_disabilities"], sort=False)
    english_learners_df = pd.concat(mydataframes["english_learners"], sort=False)

    directory_df.to_csv('./district_directory_df_2014_2016.csv', index=False, header=True)
    membership_df.to_csv('./district_membership_df_2014_2016.csv', index=False, header=True)
    staff_df.to_csv('./district_staff_df_2014_2016.csv', index=False, header=True)
    children_disabilities_df.to_csv('./district_children_disabilities_2014_2016.csv', index=False, header=True)
    english_learners_df.to_csv('./district_english_learners_2014_2016.csv', index=False, header=True)

read_district_2014_2016()
read_district_2016_2019()