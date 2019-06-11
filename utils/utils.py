import time
import sys
class FileObjFaker:
    def __init__(self, data):
        self.data = data
        self.idx = 1
        self.start_time = time.time()
        self.prev_val_len = 0
        self.prev_idx = 0

    def readline(self, *args, **kwargs):
        try:
            row = list(next(self.data))
        except StopIteration:
            return ''
        else:
            return '{}\n'.format('\t'.join(
                str(i) if (i or i == 0) else '' for i in row))
        ###
        finally:
            if (self.idx % 20000) == 0:
                now = time.time()
                elapsed = now - self.start_time
                val = '%.2f rows/sec [%s]' % (
                    (self.idx - self.prev_idx) / elapsed, self.idx)
                tmp = "\b" * self.prev_val_len
                print_row_progress(f'{tmp}{val}')
                self.prev_val_len = len(val) + 3
                self.start_time = now
                self.prev_idx = self.idx + 0
            self.idx += 1

    def read(self, *args, **kwargs):
        return self.readline(*args, **kwargs)


def print_states(val):
    print(f'\033[0;32;40m\r{val}\t\033[0m')

def print_row_progress(val):
    print(f"\033[0;36;40m\r{val}\033[0m", end='')
    sys.stdout.flush()
