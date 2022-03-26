from Training_Raw_data_validation.rawValidation import Raw_Data_validation
from DataTransform_Training.Data_Transformation import dataTransform
from DataTypeValidation_Insertion_Training.DataTypeValidation import dBOperation
from application_logging import logger

class train_validation:
    def __init__(self,path):
        self.raw_data = Raw_Data_validation(path)
        self.dataTransform = dataTransform()
        self.dBOperation = dBOperation()
        self.file_object = open("Training_Logs/Training_Main_Log.txt", 'a+')
        self.log_writer = logger.App_Logger()

    def train_validation(self):
        """
        Description: This method is used for whole validation of training datasets.
        On Failure: Raise Exception

        Written By: Shashwat Bansal
        Version: 1.0
        Revisions: None
        :return:None
        """
        try:
            self.log_writer.log(self.file_object, 'Start of Validation on files!!')
            # extracting values from prediction schema
            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, noofcolumns = self.raw_data.values_from_schema()
            # getting the regex defined to validate filename
            regex = self.raw_data.manualRegexCreation()
            # validating filename of prediction files
            self.raw_data.validation_file_name_raw(regex,LengthOfDateStampInFile,LengthOfTimeStampInFile)
            # validating column length in the file
            self.raw_data.validate_column_length(noofcolumns)
            # validating if any column has all values missing
            self.raw_data.validate_missing_values_in_whole_columns()
            self.log_writer.log(self.file_object, "Raw Data Validation Complete!!")

            self.log_writer.log(self.file_object, "Starting Data Transforamtion!!")
            # replacing blanks in the csv file with "Null" values to insert in table
            self.dataTransform.replace_missing_with_null()
            self.log_writer.log(self.file_object, "DataTransformation Completed!!!")

            self.log_writer.log(self.file_object,"Creating Training_Database and tables on the basis of given schema!!!")
            # create database with given name, if present open the connection! Create table with columns given in schema
            self.dBOperation.create_table_db('Training', column_names)
            self.log_writer.log(self.file_object, "Table creation Completed!!")
            self.log_writer.log(self.file_object, "Insertion of Data into Table started!!!!")
            # insert csv files in the table
            self.dBOperation.insert_into_table_good_data('Training')
            self.log_writer.log(self.file_object, "Insertion in Table completed!!!")
            self.log_writer.log(self.file_object, "Deleting Good Data Folder!!!")
            # Delete the good data folder after loading files in table
            self.raw_data.delete_existing_good_data_training_folder()
            self.log_writer.log(self.file_object, "Good_Data folder deleted!!!")
            self.log_writer.log(self.file_object, "Moving bad files to Archive and deleting Bad_Data folder!!!")
            # Move the bad files to archive folder
            self.raw_data.move_bad_files_to_archive_bad()
            self.log_writer.log(self.file_object, "Bad files moved to archive!! Bad folder Deleted!!")
            self.log_writer.log(self.file_object, "Validation Operation completed!!")
            self.log_writer.log(self.file_object, "Extracting csv file from table")
            # export data in table to csvfile
            self.dBOperation.selecting_data_from_table_into_csv('Training')
            self.file_object.close()
        except Exception as e:
            self.log_writer.log(self.file_object, "Some Thing happened:  %s " % e)
            self.file_object.close()