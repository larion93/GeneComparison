import time
import csv
import pymysql
from bisect import bisect_left


class ComparingClass:

    __connection = None
    _gen_map_file_list = [{} for i in range(23)]
    _list_of_keys = [[] for i in range(23)]

    def __init__(self, first_table, second_table, snp_threshold, cm_threshold, error_radius):
        self.first_table = first_table
        self.second_table = second_table
        self.snp_threshold = snp_threshold
        self.cm_threshold = cm_threshold
        self.error_radius = error_radius

    @staticmethod
    def load_genetic_map_files(path):
        for index in range(1,23):
            map_file = open(path+str(index)+".txt")
            snp_file_reader = csv.reader(map_file, delimiter='\t', quotechar='"')
            for row in snp_file_reader:
                position = int(row[1])
                map_value = float(row[3])
                ComparingClass._gen_map_file_list[index].update({position: map_value})
            ComparingClass._list_of_keys[index] = sorted(list(ComparingClass._gen_map_file_list[index].keys()))
            #print (gen_map_file_list[index])

    @staticmethod
    def take_closest(mylist, mynumber):
        pos = bisect_left(mylist, mynumber)
        if pos == 0:
            return mylist[0]
        if pos == len(mylist):
            return mylist[-1]
        before = mylist[pos - 1]
        after = mylist[pos]
        if after - mynumber < mynumber - before:
           return after
        else:
           return before

    def get_values_in_both(self, table_name1, table_name2):
        cursor = self.__connection.cursor()
        query = ("SELECT first.chromosome, first.position, first.alleles, second.alleles  FROM "+table_name1+" first"
                 " INNER JOIN "+table_name2+" second using(rs_id)"
                 " ORDER BY chromosome, position ASC ")
        print(query)
        cursor.execute(query)
        return cursor.fetchall()

    #@profile
    #@do_profile(follow=[])
    @staticmethod
    def compare_two_snp_values(input_list):
        result_list = [[] for i in range(23)]
        for input_row in input_list:
            if input_row[2][0] in input_row[3] or input_row[2][1] in input_row[3]:
                temp_storing_results = (input_row[0], input_row[1], 1)
            elif input_row[2] == "--" or input_row[3] == "--":
                temp_storing_results = (input_row[0], input_row[1], 1)
            else:
                temp_storing_results = (input_row[0], input_row[1], 0)
            result_list[input_row[0]].append(temp_storing_results)
        return result_list

    #@do_profile(follow=[])
    @staticmethod
    def get_sequences_count(input_sequence, error_radius):
        length = 0
        output_sequence = []
        start_position = 0
        if input_sequence:
            for index, row in enumerate(input_sequence):
                if row[2] == 1:
                    length +=1
                elif 0 in (x[2] for x in input_sequence[index+1:index+error_radius]):
                    end_position = row[1]
                    temp_list = [row[0], start_position, end_position, length]
                    output_sequence.append(temp_list)
                    length = 0
                    start_position = row[1]
                else:length +=1
            end_position = row[1]
            temp_list = [row[0], start_position, end_position, length]
            output_sequence.append(temp_list)
        return output_sequence

    def megabases_to_centimorgan(self, input_sequence, chromosome):
        start_time1 = time.time()
        key_start = self.take_closest(self._list_of_keys[chromosome], input_sequence[1])
        key_end = self.take_closest(self._list_of_keys[chromosome], input_sequence[2])
        map_value_start = self._gen_map_file_list[chromosome][key_start]
        map_value_end = self._gen_map_file_list[chromosome][key_end]
        print("Time take closest:", time.time()-start_time1)
        return map_value_end-map_value_start

    def get_length_in_centimorgans(self, input_sequence, snp_threshold, cm_threshold, error_radius):
        total_length_in_megabases = 0
        total_length_in_centimorgans = 0
        for chromosome in range(1, 23):
            start_time1 = time.time()
            count_list = self.get_sequences_count(input_sequence[chromosome], error_radius)
            print("Time get_sequences_count:", time.time()-start_time1)
            for line in count_list:
                if line[3] > snp_threshold:
                    total_length_in_megabases += line[2] - line[1]
                    length_in_centimorgan = self.megabases_to_centimorgan(line, chromosome)
                    if length_in_centimorgan > cm_threshold:
                        total_length_in_centimorgans += length_in_centimorgan
                    print("chromosome: "+str(chromosome)+" "+str(line)+" "+str(length_in_centimorgan))
        print("Total length in megabases:", total_length_in_megabases/1000000)
        print("Total length in centimorgans:", total_length_in_centimorgans)
        return total_length_in_centimorgans

    def compare(self):
        start_time = time.time()
        values = self.get_values_in_both(self.first_table, self.second_table)
        print("Time getting values from database:", time.time()-start_time)
        start_time = time.time()
        result = self.compare_two_snp_values(values)
        print("Time comparing:", time.time()-start_time)
        start_time = time.time()
        length_in_centimorgans = self.get_length_in_centimorgans(result, self.snp_threshold, self.cm_threshold, self.error_radius)
        print("Time converting:", time.time()-start_time)
        return length_in_centimorgans

    @staticmethod
    def set_connection(username, password, host, database):
        ComparingClass.__connection = pymysql.connect(user=username, password=password, host=host,database=database)