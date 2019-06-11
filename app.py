from hive2pg import Converter, HiveReader, PGDBWrite
from configs import kw

if __name__ == "__main__":
    writer = PGDBWrite()
    reader = HiveReader()
    app = Converter(reader, writer)
    app.covert(conf=kw)
