import csv
import mysql.connector
import pymysql


class Parser:

    __connection = None
    @staticmethod
    def parse_23andme(file, table_name):
        cursor = Parser.__connection.cursor()
        insert_snp_query_merged= ("INSERT INTO "+table_name+
                "(rs_id, chromosome, position, alleles) "
                "VALUES (%s, %s, %s, %s)")
        snpfilereader = csv.reader(filter(lambda row: row[0]!='#', file), delimiter='\t', quotechar='"')
        snpvalues = []
        for row in snpfilereader:
            rs_id = row[0]
            chromosome = row[1]
            position = row[2]
            alleles = row[3]
            if chromosome == 'MT' or chromosome == 'X' or chromosome == 'Y':continue
            data_snp = (rs_id,chromosome,position,alleles)
            snpvalues.append(data_snp)
        cursor.executemany(insert_snp_query_merged, snpvalues)
        Parser.__connection.commit()
        cursor.close()

    @staticmethod
    def parse_ftdna(file, table_name):
        cursor = Parser.__connection.cursor()
        insert_snp_query_merged= ("INSERT INTO "+table_name+
                "(rs_id, chromosome, position, alleles) "
                "VALUES (%s, %s, %s, %s)")
        snpfilereader = csv.reader(filter(lambda row: row[0]!='#', file), delimiter=',', quotechar='"')
        snpvalues = []
        for row in snpfilereader:
            rs_id = row[0]
            chromosome = row[1]
            position = row[2]
            alleles = row[3]
            if rs_id == 'RSID':continue
            if chromosome == 'MT' or chromosome == 'X' or chromosome == 'Y':continue
            data_snp = (rs_id,chromosome,position,alleles)
            snpvalues.append(data_snp)
        cursor.executemany(insert_snp_query_merged, snpvalues)
        Parser.__connection.commit()
        cursor.close()

    @staticmethod
    def parse_ancestry(file, table_name):
        cursor = Parser.__connection.cursor()
        insert_snp_query_merged= ("INSERT INTO "+table_name+
                "(rs_id, chromosome, position, alleles) "
                "VALUES (%s, %s, %s, %s)")
        snpfilereader = csv.reader(filter(lambda row: row[0]!='#', file), delimiter='\t', quotechar='"')
        snpvalues = []
        for row in snpfilereader:
            rs_id = row[0]
            chromosome = row[1]
            position = row[2]
            alleles = row[3]+row[4]
            if rs_id == 'rsid' or int(chromosome) > 22:continue
            if chromosome == 'MT' or chromosome == 'X' or chromosome == 'Y':continue
            data_snp = (rs_id,chromosome,position,alleles)
            snpvalues.append(data_snp)
        cursor.executemany(insert_snp_query_merged, snpvalues)
        Parser.__connection.commit()
        cursor.close()

    @staticmethod
    def set_connection(username, password, host, database):
        Parser.__connection = pymysql.connect(user=username, password=password, host=host,database=database)