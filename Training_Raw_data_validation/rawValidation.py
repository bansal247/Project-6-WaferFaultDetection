from datetime import datetime
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
from application_logging.logger import App_Logger

class Raw_Data_validation:
    """

        Description: This class is used for handling validation of raq traning data.

        Written By: Shashwat Bansal
        Version: 1.0
        Revisions: None
    """

    def __init__(self,path):
        self.Batch_Directory = path
        self.schema_path = 'schema_training.json'
        self.logger = App_Logger()

    def values_from_schema(self):
        """
        Description: This method extracts all the relevant information from the pre-defined "Schema" file.
        On Failure: Raise ValueError,KeyError,Exception

        Written By: Shashwat Bansal
        Version: 1.0
        Revisions: None
        :return:LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Number of Columns
        """
        try:
            with open(self.schema_path,'r') as f:
                dic = json.load(f)
                f.close()
                LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
                LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
                column_names = dic['ColName']
                NumberofColumns = dic['NumberofColumns']

                file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
                message ="LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile +"\t " + "NumberofColumns:: %s" % NumberofColumns + "\n"
                self.logger.log(file,message)

                file.close()
        except ValueError:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "ValueError:Value not found inside schema_training.json")
            file.close()
            raise ValueError
        except KeyError:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "KeyError:Key value error incorrect key passed")
            file.close()
            raise KeyError
        except Exception as e:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, str(e))
            file.close()
            raise e

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns

    def manualRegexCreation(self):
        """
        Description: This method contains a manually defined regex based on the "FileName" given in "Schema" file.
                    This Regex is used to validate the filename of the training data.
        On Failure: None

        Written By: Shashwat Bansal
        Version: 1.0
        Revisions: None
        :return:Regex pattern
        """
        try:
            regex = "^wafer+\_+[\d]+\_+[\d]+.csv$"
        except Exception as e:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, str(e))
            file.close()
            raise e
        return regex

    def create_directory_for_good_bad_raw_data(self):
        """
        Description: This method creates directories to store the Good Data and Bad Data
                     after validating the training data.
        On Failure: OSError

        Written By: Shashwat Bansal
        Version: 1.0
        Revisions: None
        :return:None
        """

        try:
            #Here I am creating directories
            path = os.path.join("Training_Raw_files_validated/", "Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("Training_Raw_files_validated/", "Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
        except OSError as ex:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while creating Directory %s:" % ex)
            file.close()
            raise OSError

    def delete_existing_good_data_training_folder(self):
        """
        Description: This method deletes the directory made  to store the Good Data
                      after loading the data in the table. Once the good files are
                      loaded in the DB,deleting the directory ensures space optimization.
        Written By: Shashwat Bansal
        Version: 1.0
        Revisions: None
        :return:None
        """
        try:
            path = 'Training_Raw_files_validated/'
            if os.path.isdir(path+'Good_Raw/'):
                shutil.rmtree(path+'Good_Raw/')
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"GoodRaw directory deleted successfully!!!")
                file.close()
        except OSError as ex:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while Deleting Directory : %s" % ex)
            file.close()
            raise OSError

    def delete_existing_bad_data_training_folder(self):
        """
        Description: This method deletes the directory made  to store the Bad Data
                      after loading the data in the table. Once the Bad files are
                      loaded in the DB,deleting the directory ensures space optimization.
        Written By: Shashwat Bansal
        Version: 1.0
        Revisions: None
        :return:None
        """
        try:
            path = 'Training_Raw_files_validated/'
            if os.path.isdir(path+'Bad_Raw/'):
                shutil.rmtree(path+'Bad_Raw/')
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"BadRaw directory deleted before starting validation!!!")
                file.close()
        except OSError as ex:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while Deleting Directory : %s" % ex)
            file.close()
            raise OSError

    def move_bad_files_to_archive_bad(self):
        """
        Description: This method deletes the directory made  to store the Bad Data
                      after moving the data in an archive folder. We archive the bad
                      files to send them back to the client for invalid data issue.

        Written By: Shashwat Bansal
        Version: 1.0
        Revisions: None
        :return:None
        """

        try:
            source = 'Training_Raw_files_validated/Bad_Raw/'
            now = datetime.now()
            date = now.date()
            time = now.strftime("%H%M%S")
            if os.path.isdir(source):
                path = "TrainingArchiveBadData"
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = 'TrainingArchiveBadData/BadData_' + str(date) + "_" + str(time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                files = os.listdir(source)
                for f in files:
                    if f not in os.listdir(dest):
                        shutil.move(source+f,dest)
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file, "Bad files moved to archive")
                path = 'Training_Raw_files_validated/'
                if not os.path.isdir(path):
                    os.makedirs(path)
                if os.path.isdir(path + 'Bad_Raw/'):
                    shutil.rmtree(path + 'Bad_Raw/')
                self.logger.log(file, "Bad Raw Data Folder Deleted successfully!!")
                file.close()
        except Exception as e:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while moving bad files to archive:: %s" % e)
            file.close()
            raise e

    def validation_file_name_raw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
        """
        Description: This function validates the name of the training csv files as per given name in the schema!
                     Regex pattern is used to do the validation.If name format do not match the file is moved
                     to Bad Raw Data folder else in Good raw data.
        On Failure: Exception

        Written By: Shashwat Bansal
        Version: 1.0
        Revisions: None
        :return:None
        """
        self.delete_existing_good_data_training_folder()
        self.delete_existing_bad_data_training_folder()
        self.create_directory_for_good_bad_raw_data()
        onlyfiles = [f for f in listdir(self.Batch_Directory)]
        try:
            f = open("Training_Logs/nameValidationLog.txt", 'a+')
            for filename in onlyfiles:
                if re.match(regex,filename):
                    splitAtDot = re.split('.csv', filename) #['wafer_08012020_120000', '']
                    splitAtDot = (re.split('_', splitAtDot[0])) #['wafer', '08012020', '120000']
                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Good_Raw")
                            self.logger.log(f, "Valid File name!! File moved to GoodRaw Folder :: %s" % filename)

                        else:
                            shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                            self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                    else:
                        shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                        self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                else:
                    shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_files_validated/Bad_Raw")
                    self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
            f.close()

        except Exception as e:
            f = open("Training_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(f, "Error occured while validating FileName %s" % e)
            f.close()
            raise e

    def validate_column_length(self,NumberofColumns):
        """
        Description:This function validates the number of columns in the csv files.
                    It is should be same as given in the schema file.
                    If not same file is not suitable for processing and thus is moved to Bad Raw Data folder.
                    If the column number matches, file is kept in Good Raw Data for processing.
                    The csv file is missing the first column name, this function changes the missing name to "Wafer".

        On Failure: Exception

        Written By: Shashwat Bansal
        Version: 1.0
        Revisions: None
        :NumberofColumns:int
        :return:None
        """
        try:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Column Length Validation Started!!")
            for file in listdir('Training_Raw_files_validated/Good_Raw/'):
                csv = pd.read_csv("Training_Raw_files_validated/Good_Raw/" + file)
                if not csv.shape[1] == NumberofColumns:
                    shutil.move("Training_Raw_files_validated/Good_Raw/" + file, "Training_Raw_files_validated/Bad_Raw")
                    self.logger.log(f, "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
            self.logger.log(f, "Column Length Validation Completed!!")
        except OSError:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e
        f.close()

    def validate_missing_values_in_whole_columns(self):
        """
        Description: This function validates if any column in the csv file has all values missing.
                   If all the values are missing, the file is not suitable for processing.
                   SUch files are moved to bad raw data.
        On Failure: Exception

        Written By: Shashwat Bansal
        Version: 1.0
        Revisions: None
        :return:None
        """
        try:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Missing Values Validation Started!!")

            for file in listdir('Training_Raw_files_validated/Good_Raw/'):
                csv = pd.read_csv("Training_Raw_files_validated/Good_Raw/" + file)
                count = 0
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count+=1
                        shutil.move("Training_Raw_files_validated/Good_Raw/" + file,
                                    "Training_Raw_files_validated/Bad_Raw")
                        self.logger.log(f,
                                        "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
                        break
                    if count==0:
                        csv.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                        csv.to_csv("Training_Raw_files_validated/Good_Raw/" + file, index=None, header=True)
            f.close()
        except OSError:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e



