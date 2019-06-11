
from configs import hive_all_data
from utils import print_states
from .hive_reader import Table

class Converter:
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self.tbs = hive_all_data
    
    def covert(self, **kwargs):
        print_states('>>>>>>>>>> STARTING <<<<<<<<<<\n\n')
        table_list = []
        for tb_name in self.tbs:
            table_list.append(Table(self.reader, tb_name))
        if 'conf' in kwargs:
            if 'truncate' in kwargs['conf']:
                print_states('START CREATING TABLES\n')
                for tb in table_list:
                    self.writer.truncate(tb)
        
        for tb_obj in table_list:
            print_states('START WRITING TABLE DATA')
            print_states(tb_obj.name)
            self.writer.write_contents(tb_obj, self.reader)
            print_states('DONE WRITING TABLE DATA    ')
        print_states('\n\n>>>>>>>>>> FINISHED <<<<<<<<<<')
        self.writer.close()
        self.reader.close()
        # print_states('START WRITING TABLE DATA')
        # tb_obj = Table(self.reader, 'dw_ods.course_module')
        # print_states(tb_obj.name)
        # self.writer.write_contents(tb_obj, self.reader)
        # print_states('DONE WRITING TABLE DATA    ')
        # self.writer.close()
        # self.reader.close()
        