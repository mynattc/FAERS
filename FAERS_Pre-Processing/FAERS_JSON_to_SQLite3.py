import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from sqlite3 import Error


def file_array(path):
    data_dir = Path(path)
    text_files = [f for f in os.listdir(data_dir) if f.endswith('.json')]
    print(text_files)
    return text_files


def json_load_send2db_function(input_path, file_name_arr, sql_db):
    """ Function to load into memory all file contents from an array of file names """
    length = len(file_name_arr)
    counter = 1
    try:
        for file_name in file_name_arr:
            with open(input_path + file_name, encoding="utf8") as file_input:
                print("Starting file " + str(counter) + " of " + str(length) + " (with filename = " + file_name + ")\r")
                data_into_db(json.load(file_input), sql_db)
                file_input.close()
                counter += 1
    except Error as e:
        print(e)
    finally:
        sql_db.commit()
        sql_db.close()
    print("All Files entered into sqlite database.")


def data_into_db(json_data, sql_db):
    """ Function to parse and write content to the sqlite db """
    patients = json_data["results"]
    # use safetyreportid as PK for each report.
    for record in patients:
        duplicate = record.get("duplicate", "")
        if duplicate != 1:
            safetyreportid = record.get("safetyreportid", "")
            serious = record.get("serious", "")
            companynumb = record.get("companynumb", "")
            receiver = record.get("receiver").get("receiverorganization", "")
            sender = record.get("sender").get("senderorganization", "")
            occurcountry = record.get("occurcountry", "")
            receiptdate = record.get("receiptdate", "")
            receiptdateformat = record.get("receiptdateformat", "")
            transmissiondate = record.get("transmissiondate", "")
            receivedate = record.get("receivedate", "")
            receivedateformat = record.get("receivedateformat", "")
            fulfillexpeditecriteria = record.get("fulfillexpeditecriteria", "")
            patientonsetage = record.get("patient", "").get("patientonsetage", "")
            patientonsetageunit = record.get("patient", "").get("patientonsetageunit", "")
            patientsex = record.get("patient", "").get("patientsex", "")
            patientagegroup = record.get("patient", "").get("patientagegroup", "")
            patientweight = record.get("patient", "").get("patientweight", "")
            reactionoutcome = record.get("patient", "").get("reaction", "")[0].get("reactionoutcome", "")
            reactionmeddrapt = record.get("patient", "").get("reaction", "")[0].get("reactionmeddrapt", "")
            seriousnessdeath = record.get("seriousnessdeath", "")
            seriousnesslifethreatening = record.get("seriousnesslifethreatening", "")
            seriousnesshospitalization = record.get("seriousnesshospitalization", "")
            seriousnessdisabling = record.get("seriousnessdisabling", "")
            seriousnesscongenitalanomali = record.get("seriousnesscongenitalanomali", "")
            seriousnessother = record.get("seriousnessother", "")
            record = (
                safetyreportid, serious, companynumb, receiver, sender, occurcountry, receiptdate, receiptdateformat,
                transmissiondate,
                receivedate, receivedateformat, fulfillexpeditecriteria, patientonsetage, patientonsetageunit,
                patientsex, patientagegroup, patientweight, reactionoutcome, reactionmeddrapt, seriousnessdeath,
                seriousnesslifethreatening, seriousnesshospitalization, seriousnessdisabling,
                seriousnesscongenitalanomali, seriousnessother)
            create_record(sql_db, record)
            print("\n")


def create_record(conn, record):
    """
    Create a new project into the projects table
    :param conn:
    :param record:
    :return: record id
    """

    sql = ''' INSERT INTO patients(safetyreportid, serious, companynumb, receiver, sender, occurcountry, receiptdate, receiptdateformat, transmissiondate, receivedate, receivedateformat, fulfillexpeditecriteria, patientonsetage, patientonsetageunit, patientsex, patientagegroup, patientweight, reactionoutcome, reactionmeddrapt, seriousnessdeath, seriousnesslifethreatening, seriousnesshospitalization, seriousnessdisabling, seriousnesscongenitalanomali, seriousnessother)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, record)
    # print(cur.lastrowid)
    return cur.lastrowid


def create_table(conn):
    c = conn.cursor()
    # Create table
    c.execute('''CREATE TABLE IF NOT EXISTS patients( safetyreportid integer PRIMARY KEY, serious text, 
    companynumb text, receiver text, sender text, occurcountry text, receiptdate text, receiptdateformat text, 
    transmissiondate text, receivedate text, 
    receivedateformat text, fulfillexpeditecriteria integer, patientonsetage integer, patientonsetageunit text, 
    patientsex integer, patientagegroup text, patientweight integer, reactionoutcome text, reactionmeddrapt text, 
    seriousnessdeath text, seriousnesslifethreatening text, seriousnesshospitalization text, seriousnessdisabling 
    text, seriousnesscongenitalanomali text, seriousnessother text)''')
    print("Created SQL Table")
    # commit the changes to db
    conn.commit()


def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    return conn


def main():
    # Create timer to see execution time length
    start = datetime.now()

    # Values for the start tag and the alert directory.
    input_path = '../FAERS_data/'
    output_path = r"C:\Users\mynat\PycharmProjects\FAERS\faers_2018.db"

    conn = create_connection(output_path)
    create_table(conn)

    json_load_send2db_function(input_path, file_array(input_path), conn)

    # End timer and calculate execution time.
    print('Time elapsed (hh:mm:ss.ms) {}'.format(datetime.now() - start))


if __name__ == '__main__':
    main()
